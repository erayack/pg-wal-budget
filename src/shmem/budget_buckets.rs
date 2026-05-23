use crate::errors::PwbResult;
use crate::types::{PolicyId, ScopeHash};

use super::{
    BudgetBucketSnapshot, BudgetBucketState, PwbBudgetBucket, PwbSharedState,
    budget_bucket_capacity_exhausted, with_locked_bucket_state,
};

pub(crate) fn snapshot_budget_buckets() -> PwbResult<Vec<BudgetBucketSnapshot>> {
    with_locked_bucket_state(|_state, buckets| snapshot_budget_buckets_from_slice(buckets))
}

pub(crate) fn with_budget_bucket<R>(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    initializer: impl FnOnce() -> BudgetBucketState,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<R> {
    with_locked_bucket_state(|state, buckets| {
        apply_budget_bucket(state, buckets, policy_id, scope_hash, initializer, callback)
    })
}

pub(crate) fn with_existing_budget_bucket<R>(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<Option<R>> {
    with_locked_bucket_state(|_state, buckets| {
        let Some(slot) = find_budget_bucket_slot(buckets, policy_id, scope_hash) else {
            return Ok(None);
        };

        let mut bucket = buckets[slot].state();
        let result = callback(&mut bucket)?;
        buckets[slot] = PwbBudgetBucket::encode(bucket);
        Ok(Some(result))
    })
}

pub(super) fn apply_budget_bucket<R>(
    state: &mut PwbSharedState,
    buckets: &mut [PwbBudgetBucket],
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    initializer: impl FnOnce() -> BudgetBucketState,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<R> {
    if buckets.is_empty() {
        return Err(budget_bucket_capacity_exhausted());
    }

    if let Some(slot) = find_budget_bucket_slot(buckets, policy_id, scope_hash) {
        let mut bucket = buckets[slot].state();
        let result = callback(&mut bucket)?;
        buckets[slot] = PwbBudgetBucket::encode(bucket);
        return Ok(result);
    }

    let slot =
        find_empty_budget_bucket_slot(buckets).ok_or_else(budget_bucket_capacity_exhausted)?;
    let mut bucket = initializer();
    let result = callback(&mut bucket)?;
    buckets[slot] = PwbBudgetBucket::encode(bucket);
    state.budget_buckets_len = state
        .budget_buckets_len
        .saturating_add(1)
        .min(state.budget_bucket_capacity);
    Ok(result)
}

pub(super) fn snapshot_budget_buckets_from_slice(
    buckets: &[PwbBudgetBucket],
) -> PwbResult<Vec<BudgetBucketSnapshot>> {
    let mut snapshots = Vec::new();

    for bucket in buckets.iter().filter(|bucket| bucket.occupied == 1) {
        let decoded = bucket.decode()?;
        snapshots.push(BudgetBucketSnapshot {
            policy_id: decoded.policy_id,
            scope_hash: decoded.scope_hash,
            available_bytes: decoded.available_bytes,
            max_burst_bytes: decoded.max_burst_bytes,
            rate_bytes_per_sec: decoded.rate_bytes_per_sec,
            last_refill_epoch_ms: decoded.last_refill_epoch_ms,
            debt_bytes: decoded.debt_bytes,
        });
    }

    Ok(snapshots)
}

fn find_budget_bucket_slot(
    buckets: &[PwbBudgetBucket],
    policy_id: PolicyId,
    scope_hash: ScopeHash,
) -> Option<usize> {
    buckets.iter().position(|bucket| {
        bucket.occupied == 1 && bucket.policy_id == policy_id && bucket.scope_hash == scope_hash
    })
}

fn find_empty_budget_bucket_slot(buckets: &[PwbBudgetBucket]) -> Option<usize> {
    buckets.iter().position(|bucket| bucket.occupied == 0)
}
