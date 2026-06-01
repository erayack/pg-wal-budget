use crate::errors::PwbResult;
use crate::types::{PolicyId, ScopeHash};

use super::records::{BudgetBucketSnapshot, BudgetBucketState, PwbBudgetBucket, PwbSharedState};
use super::{budget_bucket_capacity_exhausted, with_locked_bucket_state};

pub(crate) fn snapshot_budget_buckets() -> PwbResult<Vec<BudgetBucketSnapshot>> {
    with_locked_bucket_state(|state, buckets| {
        snapshot_budget_buckets_from_slice(buckets, state.budget_buckets_len as usize)
    })
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
    with_locked_bucket_state(|state, buckets| {
        let occupied_len = occupied_budget_bucket_len(state, buckets);
        let Some(slot) = find_budget_bucket_slot(&buckets[..occupied_len], policy_id, scope_hash)
        else {
            return Ok(None);
        };

        let mut bucket = buckets[slot].state();
        let result = callback(&mut bucket)?;
        buckets[slot] = PwbBudgetBucket::encode(bucket);
        Ok(Some(result))
    })
}

fn apply_budget_bucket<R>(
    state: &mut PwbSharedState,
    buckets: &mut [PwbBudgetBucket],
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    initializer: impl FnOnce() -> BudgetBucketState,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<R> {
    let occupied_len = occupied_budget_bucket_len(state, buckets);
    for (slot, bucket) in buckets[..occupied_len].iter().enumerate() {
        if bucket.occupied == 1 && bucket.policy_id == policy_id && bucket.scope_hash == scope_hash
        {
            let mut bucket = bucket.state();
            let result = callback(&mut bucket)?;
            buckets[slot] = PwbBudgetBucket::encode(bucket);
            return Ok(result);
        }
    }

    if occupied_len == buckets.len() {
        return Err(budget_bucket_capacity_exhausted());
    }

    let slot = occupied_len;
    let mut bucket = initializer();
    let result = callback(&mut bucket)?;
    buckets[slot] = PwbBudgetBucket::encode(bucket);
    state.budget_buckets_len += 1;
    Ok(result)
}

fn occupied_budget_bucket_len(state: &PwbSharedState, buckets: &[PwbBudgetBucket]) -> usize {
    debug_assert!(
        state.budget_buckets_len <= state.budget_bucket_capacity,
        "budget bucket occupied length exceeded configured capacity"
    );
    debug_assert_eq!(
        state.budget_bucket_capacity as usize,
        buckets.len(),
        "budget bucket slice length must match configured capacity"
    );
    state.budget_buckets_len as usize
}

fn snapshot_budget_buckets_from_slice(
    buckets: &[PwbBudgetBucket],
    occupied_len: usize,
) -> PwbResult<Vec<BudgetBucketSnapshot>> {
    let mut snapshots = Vec::with_capacity(occupied_len.min(buckets.len()));

    for bucket in buckets[..occupied_len.min(buckets.len())]
        .iter()
        .filter(|bucket| bucket.occupied == 1)
    {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::PwbError;
    use crate::shmem::records::test_state;

    #[test]
    fn snapshots_only_occupied_budget_buckets() {
        let buckets = [
            PwbBudgetBucket::encode(BudgetBucketState {
                policy_id: 7,
                scope_hash: 99,
                available_bytes: 1024,
                max_burst_bytes: 4096,
                rate_bytes_per_sec: 512,
                last_refill_epoch_ms: 123,
                debt_bytes: 64,
            }),
            PwbBudgetBucket::default(),
        ];

        let snapshots = snapshot_budget_buckets_from_slice(&buckets, 1)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].policy_id, 7);
        assert_eq!(snapshots[0].scope_hash, 99);
        assert_eq!(snapshots[0].debt_bytes, 64);
    }

    #[test]
    fn new_budget_bucket_is_not_persisted_when_callback_errors() {
        let mut state = test_state(1);
        let mut buckets = [PwbBudgetBucket::default()];
        let error = match apply_budget_bucket(
            &mut state,
            &mut buckets,
            7,
            99,
            || BudgetBucketState {
                policy_id: 7,
                scope_hash: 99,
                available_bytes: 1024,
                max_burst_bytes: 4096,
                rate_bytes_per_sec: 512,
                last_refill_epoch_ms: 123,
                debt_bytes: 0,
            },
            |_bucket| {
                Err(PwbError::BudgetExceeded {
                    policy_id: 7,
                    predicted_wal_bytes: 2048,
                    available_wal_bytes: 1024,
                })
            },
        ) {
            Ok(()) => panic!("expected callback error"),
            Err(error) => error,
        };

        assert!(matches!(error, PwbError::BudgetExceeded { .. }));
        assert_eq!(state.budget_buckets_len, 0);
        assert_eq!(buckets[0], PwbBudgetBucket::default());
    }

    #[test]
    fn new_budget_bucket_errors_without_capacity() {
        let mut state = test_state(0);
        let mut buckets = [];

        let error = match apply_budget_bucket(
            &mut state,
            &mut buckets,
            7,
            99,
            || BudgetBucketState {
                policy_id: 7,
                scope_hash: 99,
                available_bytes: 1024,
                max_burst_bytes: 4096,
                rate_bytes_per_sec: 512,
                last_refill_epoch_ms: 123,
                debt_bytes: 0,
            },
            |_bucket| Ok(()),
        ) {
            Ok(()) => panic!("expected capacity error"),
            Err(error) => error,
        };

        assert_eq!(error, budget_bucket_capacity_exhausted());
        assert_eq!(state.budget_buckets_len, 0);
    }

    #[test]
    fn new_budget_bucket_errors_when_capacity_is_full() {
        let mut state = test_state(1);
        state.budget_buckets_len = 1;
        let initial = BudgetBucketState {
            policy_id: 7,
            scope_hash: 99,
            available_bytes: 1024,
            max_burst_bytes: 4096,
            rate_bytes_per_sec: 512,
            last_refill_epoch_ms: 123,
            debt_bytes: 0,
        };
        let mut buckets = [PwbBudgetBucket::encode(initial)];

        let error = match apply_budget_bucket(
            &mut state,
            &mut buckets,
            7,
            100,
            || BudgetBucketState {
                policy_id: 7,
                scope_hash: 100,
                available_bytes: 1024,
                max_burst_bytes: 4096,
                rate_bytes_per_sec: 512,
                last_refill_epoch_ms: 123,
                debt_bytes: 0,
            },
            |_bucket| Ok(()),
        ) {
            Ok(()) => panic!("expected capacity error"),
            Err(error) => error,
        };

        assert_eq!(error, budget_bucket_capacity_exhausted());
        assert_eq!(state.budget_buckets_len, 1);
        assert_eq!(buckets[0], PwbBudgetBucket::encode(initial));
    }

    #[test]
    fn existing_budget_bucket_is_persisted_when_callback_succeeds() {
        let mut state = test_state(1);
        state.budget_buckets_len = 1;
        let initial = BudgetBucketState {
            policy_id: 7,
            scope_hash: 99,
            available_bytes: 1024,
            max_burst_bytes: 4096,
            rate_bytes_per_sec: 512,
            last_refill_epoch_ms: 123,
            debt_bytes: 0,
        };
        let mut buckets = [PwbBudgetBucket::encode(initial)];

        apply_budget_bucket(
            &mut state,
            &mut buckets,
            7,
            99,
            || panic!("existing bucket should not be initialized"),
            |bucket| {
                bucket.available_bytes = 256;
                Ok(())
            },
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(state.budget_buckets_len, 1);
        assert_eq!(
            buckets[0]
                .decode()
                .unwrap_or_else(|error| panic!("{error}"))
                .available_bytes,
            256
        );
    }

    #[test]
    fn new_budget_bucket_appends_after_occupied_prefix() {
        let mut state = test_state(2);
        state.budget_buckets_len = 1;
        let initial = BudgetBucketState {
            policy_id: 7,
            scope_hash: 99,
            available_bytes: 1024,
            max_burst_bytes: 4096,
            rate_bytes_per_sec: 512,
            last_refill_epoch_ms: 123,
            debt_bytes: 0,
        };
        let appended = BudgetBucketState {
            policy_id: 8,
            scope_hash: 100,
            available_bytes: 2048,
            max_burst_bytes: 8192,
            rate_bytes_per_sec: 1024,
            last_refill_epoch_ms: 456,
            debt_bytes: 0,
        };
        let mut buckets = [PwbBudgetBucket::encode(initial), PwbBudgetBucket::default()];

        apply_budget_bucket(
            &mut state,
            &mut buckets,
            appended.policy_id,
            appended.scope_hash,
            || appended,
            |_bucket| Ok(()),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(state.budget_buckets_len, 2);
        assert_eq!(buckets[0], PwbBudgetBucket::encode(initial));
        assert_eq!(buckets[1], PwbBudgetBucket::encode(appended));
    }
}
