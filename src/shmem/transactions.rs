use crate::errors::PwbResult;
use crate::types::{EpochMillis, PolicyId, ScopeHash, WalBytes};

use super::{
    BudgetBucketSnapshot, BudgetBucketState, CounterDelta, PwbCounters, QueryProfileSnapshot,
    RecentDecisionRecord, ScopeNameSnapshot, budget_buckets, counters, profiles, recent_decisions,
    scope_names,
};
use crate::types::ScopeKey;

pub(crate) fn record_admission_telemetry(
    delta: CounterDelta,
    recent_decision: RecentDecisionRecord,
    scope: &ScopeKey,
    now_epoch_ms: EpochMillis,
) -> PwbResult<()> {
    super::record_admission_telemetry_locked(delta, recent_decision, scope, now_epoch_ms)
}

pub(crate) fn record_decision_telemetry(
    delta: CounterDelta,
    recent_decision: RecentDecisionRecord,
) -> PwbResult<()> {
    counters::add_counters(delta)?;
    recent_decisions::record_recent_decision(recent_decision)
}

pub(crate) fn add_counter_delta(delta: CounterDelta) -> PwbResult<()> {
    counters::add_counters(delta)
}

pub(crate) fn admit_with_budget_bucket<R>(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    initialize: impl FnOnce() -> BudgetBucketState,
    admit: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<R> {
    budget_buckets::with_budget_bucket(policy_id, scope_hash, initialize, admit)
}

pub(crate) fn with_existing_budget_bucket<R>(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<Option<R>> {
    budget_buckets::with_existing_budget_bucket(policy_id, scope_hash, callback)
}

pub(crate) fn refund_budget_charge(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    bytes: WalBytes,
) -> PwbResult<()> {
    let _ = budget_buckets::with_existing_budget_bucket(policy_id, scope_hash, |bucket| {
        bucket.available_bytes = bucket
            .available_bytes
            .saturating_add(bytes)
            .min(bucket.max_burst_bytes);
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn record_budget_debt(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    bytes: WalBytes,
) -> PwbResult<()> {
    let _ = budget_buckets::with_existing_budget_bucket(policy_id, scope_hash, |bucket| {
        bucket.debt_bytes = bucket.debt_bytes.saturating_add(bytes);
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn ensure_profiles_loaded(
    now_epoch_ms: EpochMillis,
    stale_after_ms: EpochMillis,
    load_profiles: impl FnOnce() -> PwbResult<Vec<QueryProfileSnapshot>>,
) -> PwbResult<()> {
    profiles::ensure_profiles_loaded(now_epoch_ms, stale_after_ms, load_profiles)
}

pub(crate) fn persist_profiles_if_due(
    now_epoch_ms: EpochMillis,
    interval_ms: EpochMillis,
    persist_profiles: impl FnOnce(&[QueryProfileSnapshot]) -> PwbResult<()>,
) -> PwbResult<()> {
    profiles::persist_profiles_if_due(now_epoch_ms, interval_ms, persist_profiles)
}

pub(crate) fn persist_profiles_now(
    now_epoch_ms: EpochMillis,
    persist_profiles: impl FnOnce(&[QueryProfileSnapshot]) -> PwbResult<()>,
) -> PwbResult<()> {
    profiles::persist_profiles_now(now_epoch_ms, persist_profiles)
}

pub(crate) fn counters_snapshot() -> PwbResult<PwbCounters> {
    counters::snapshot_counters()
}

pub(crate) fn budget_bucket_snapshots() -> PwbResult<Vec<BudgetBucketSnapshot>> {
    budget_buckets::snapshot_budget_buckets()
}

pub(crate) fn query_profile_snapshots() -> PwbResult<Vec<QueryProfileSnapshot>> {
    profiles::snapshot_query_profiles()
}

pub(crate) fn recent_decision_snapshots(limit: usize) -> PwbResult<Vec<RecentDecisionRecord>> {
    recent_decisions::snapshot_recent_decisions(limit)
}

pub(crate) fn scope_name_snapshots() -> PwbResult<Vec<ScopeNameSnapshot>> {
    scope_names::snapshot_scope_names()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transaction_snapshots_report_unavailable_shared_memory() {
        let error = match counters_snapshot() {
            Ok(counters) => panic!("expected unavailable shared memory, got {counters:?}"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("shared memory is not initialized"),
            "unexpected error: {error}"
        );
    }
}
