use pgrx::iter::TableIterator;
use pgrx::prelude::*;
use pgrx::{PgLogLevel, PgSqlErrorCode, ereport};

use crate::errors::{PwbError, PwbResult};
use crate::privileges::{self, ADMIN_ROLE};
use crate::profile;
use crate::profile_store;
use crate::shmem::{
    self, BudgetBucketSnapshot, PwbCounters, QueryProfileSnapshot, RecentDecisionRecord,
};
use crate::types::WalBytes;

// pgrx requires `name!` columns inline in `#[pg_extern]` TableIterator return types. These aliases
// keep internal row builders readable; keep them in sync with the public function signatures below.
type CountersRow = (
    name!(accepted_statements, i64),
    name!(rejected_statements, i64),
    name!(shadow_would_reject_count, i64),
    name!(predicted_wal_bytes, i64),
    name!(actual_wal_bytes, i64),
    name!(absolute_prediction_error, i64),
    name!(scope_debt_bytes, i64),
    name!(missing_actual_wal_count, i64),
    name!(internal_fail_open_count, i64),
    name!(aborted_after_charge_count, i64),
);

type ScopeStatsRow = (
    name!(policy_id, i32),
    name!(scope_hash, i64),
    name!(available_bytes, i64),
    name!(max_burst_bytes, i64),
    name!(rate_bytes_per_sec, i64),
    name!(debt_bytes, i64),
    name!(last_refill_epoch_ms, i64),
);

type QueryProfileRow = (
    name!(scope_hash, Option<i64>),
    name!(query_id, i64),
    name!(calls, i64),
    name!(ewma_wal_bytes, i64),
    name!(max_wal_bytes, i64),
    name!(last_seen_epoch_ms, i64),
    name!(is_global, bool),
);

type RecentDecisionRow = (
    name!(timestamp_epoch_ms, i64),
    name!(decision_kind, &'static str),
    name!(policy_id, Option<i32>),
    name!(scope_kind, &'static str),
    name!(scope_hash, i64),
    name!(query_id, Option<i64>),
    name!(statement_class, &'static str),
    name!(predicted_wal_bytes, i64),
    name!(actual_wal_bytes, Option<i64>),
    name!(available_before, i64),
    name!(available_after, i64),
    name!(reason_code, &'static str),
);

#[pg_extern(stable)]
#[allow(clippy::type_complexity)]
fn pwb_counters() -> TableIterator<
    'static,
    (
        name!(accepted_statements, i64),
        name!(rejected_statements, i64),
        name!(shadow_would_reject_count, i64),
        name!(predicted_wal_bytes, i64),
        name!(actual_wal_bytes, i64),
        name!(absolute_prediction_error, i64),
        name!(scope_debt_bytes, i64),
        name!(missing_actual_wal_count, i64),
        name!(internal_fail_open_count, i64),
        name!(aborted_after_charge_count, i64),
    ),
> {
    let counters = shmem::snapshot_counters().unwrap_or_else(raise_stats_error);
    TableIterator::new(vec![counters_row(counters)])
}

#[pg_extern(stable)]
#[allow(clippy::type_complexity)]
fn pwb_scope_stats() -> TableIterator<
    'static,
    (
        name!(policy_id, i32),
        name!(scope_hash, i64),
        name!(available_bytes, i64),
        name!(max_burst_bytes, i64),
        name!(rate_bytes_per_sec, i64),
        name!(debt_bytes, i64),
        name!(last_refill_epoch_ms, i64),
    ),
> {
    let buckets = shmem::snapshot_budget_buckets().unwrap_or_else(raise_stats_error);
    TableIterator::new(buckets.into_iter().map(scope_stats_row))
}

#[pg_extern(stable)]
#[allow(clippy::type_complexity)]
fn pwb_query_profiles() -> TableIterator<
    'static,
    (
        name!(scope_hash, Option<i64>),
        name!(query_id, i64),
        name!(calls, i64),
        name!(ewma_wal_bytes, i64),
        name!(max_wal_bytes, i64),
        name!(last_seen_epoch_ms, i64),
        name!(is_global, bool),
    ),
> {
    let profiles = shmem::snapshot_query_profiles().unwrap_or_else(raise_stats_error);
    TableIterator::new(profiles.into_iter().map(query_profile_row))
}

#[pg_extern(stable)]
#[allow(clippy::type_complexity)]
fn pwb_recent_decisions(
    limit: default!(i32, "100"),
) -> TableIterator<
    'static,
    (
        name!(timestamp_epoch_ms, i64),
        name!(decision_kind, &'static str),
        name!(policy_id, Option<i32>),
        name!(scope_kind, &'static str),
        name!(scope_hash, i64),
        name!(query_id, Option<i64>),
        name!(statement_class, &'static str),
        name!(predicted_wal_bytes, i64),
        name!(actual_wal_bytes, Option<i64>),
        name!(available_before, i64),
        name!(available_after, i64),
        name!(reason_code, &'static str),
    ),
> {
    let limit = usize::try_from(limit.max(0)).unwrap_or(usize::MAX);
    let decisions = shmem::snapshot_recent_decisions(limit).unwrap_or_else(raise_stats_error);
    TableIterator::new(decisions.into_iter().map(recent_decision_row))
}

#[pg_extern]
fn pwb_reset_stats() {
    reset_stats_impl().unwrap_or_else(raise_stats_error);
}

#[pg_extern]
fn pwb_reset_profiles() {
    reset_profiles_impl().unwrap_or_else(raise_stats_error);
}

#[pg_extern]
fn pwb_flush_profiles() {
    flush_profiles_impl().unwrap_or_else(raise_stats_error);
}

fn reset_stats_impl() -> PwbResult<()> {
    require_admin("reset pg_wal_budget stats")?;
    shmem::reset_stats()
}

fn reset_profiles_impl() -> PwbResult<()> {
    require_admin("reset pg_wal_budget query profiles")?;
    profile_store::delete_profiles()?;
    shmem::reset_profiles()
}

fn flush_profiles_impl() -> PwbResult<()> {
    require_admin("flush pg_wal_budget query profiles")?;
    profile::flush_profiles()
}

const fn counters_row(counters: PwbCounters) -> CountersRow {
    (
        u64_to_i64_saturating(counters.accepted_statements),
        u64_to_i64_saturating(counters.rejected_statements),
        u64_to_i64_saturating(counters.shadow_would_reject_count),
        u64_to_i64_saturating(counters.predicted_wal_bytes),
        u64_to_i64_saturating(counters.actual_wal_bytes),
        u64_to_i64_saturating(counters.absolute_prediction_error),
        u64_to_i64_saturating(counters.scope_debt_bytes),
        u64_to_i64_saturating(counters.missing_actual_wal_count),
        u64_to_i64_saturating(counters.internal_fail_open_count),
        u64_to_i64_saturating(counters.aborted_after_charge_count),
    )
}

const fn scope_stats_row(bucket: BudgetBucketSnapshot) -> ScopeStatsRow {
    (
        bucket.policy_id,
        u64_to_i64_saturating(bucket.scope_hash),
        wal_bytes_to_i64_saturating(bucket.available_bytes),
        wal_bytes_to_i64_saturating(bucket.max_burst_bytes),
        wal_bytes_to_i64_saturating(bucket.rate_bytes_per_sec),
        wal_bytes_to_i64_saturating(bucket.debt_bytes),
        u64_to_i64_saturating(bucket.last_refill_epoch_ms),
    )
}

fn query_profile_row(snapshot: QueryProfileSnapshot) -> QueryProfileRow {
    (
        snapshot.scope_hash.map(u64_to_i64_saturating),
        u64_to_i64_saturating(snapshot.query_id),
        u64_to_i64_saturating(snapshot.profile.calls),
        wal_bytes_to_i64_saturating(snapshot.profile.ewma_wal_bytes),
        wal_bytes_to_i64_saturating(snapshot.profile.max_wal_bytes),
        u64_to_i64_saturating(snapshot.profile.last_seen_epoch_ms),
        snapshot.scope_hash.is_none(),
    )
}

fn recent_decision_row(record: RecentDecisionRecord) -> RecentDecisionRow {
    (
        u64_to_i64_saturating(record.timestamp_epoch_ms),
        record.decision_kind.as_sql_str(),
        record.policy_id,
        record.scope_kind.as_sql_str(),
        u64_to_i64_saturating(record.scope_hash),
        record.query_id.map(u64_to_i64_saturating),
        record.statement_class.as_sql_str(),
        wal_bytes_to_i64_saturating(record.predicted_wal_bytes),
        record.actual_wal_bytes.map(wal_bytes_to_i64_saturating),
        wal_bytes_to_i64_saturating(record.available_before),
        wal_bytes_to_i64_saturating(record.available_after),
        record.reason_code.as_sql_str(),
    )
}

fn require_admin(operation: &'static str) -> PwbResult<()> {
    if privileges::current_user_is_superuser_or_member_of(&[ADMIN_ROLE])? {
        Ok(())
    } else {
        Err(PwbError::InsufficientPrivilege { operation })
    }
}

const fn wal_bytes_to_i64_saturating(value: WalBytes) -> i64 {
    u64_to_i64_saturating(value)
}

const fn u64_to_i64_saturating(value: u64) -> i64 {
    if value > i64::MAX as u64 {
        i64::MAX
    } else {
        value.cast_signed()
    }
}

#[allow(clippy::needless_pass_by_value)]
fn raise_stats_error<T>(error: PwbError) -> T {
    let message = error.message();
    let detail = error.to_string();
    let sqlstate = match error {
        PwbError::InsufficientPrivilege { .. } => PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
        PwbError::InvalidBudgetMode { .. }
        | PwbError::InvalidScopeKind { .. }
        | PwbError::InvalidPolicyValue { .. } => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
        PwbError::Internal { .. } => PgSqlErrorCode::ERRCODE_INTERNAL_ERROR,
        PwbError::MissingScope
        | PwbError::PredictionUnavailable { .. }
        | PwbError::BudgetExceeded { .. } => PgSqlErrorCode::ERRCODE_RAISE_EXCEPTION,
    };

    ereport!(PgLogLevel::ERROR, sqlstate, format!("{message}: {detail}"));
    unreachable!("ereport(ERROR) should not return");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_bigint_conversion_saturates() {
        assert_eq!(u64_to_i64_saturating(42), 42);
        assert_eq!(u64_to_i64_saturating(i64::MAX as u64), i64::MAX);
        assert_eq!(u64_to_i64_saturating(u64::MAX), i64::MAX);
    }
}
