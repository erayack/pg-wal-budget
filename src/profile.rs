#![allow(dead_code)]

use crate::errors::PwbResult;
use crate::guc;
use crate::profile_store;
use crate::shmem;
use crate::time;
use crate::types::{EpochMillis, QueryId, QueryWalProfile, ScopeHash, WalBytes};

const PROFILE_PERSIST_INTERVAL_MS: EpochMillis = 60_000;
const PROFILE_RESTORE_STALE_MS: EpochMillis = 60_000;

pub(crate) fn lookup_prediction_profile(
    scope_hash: ScopeHash,
    query_id: Option<QueryId>,
) -> PwbResult<Option<QueryWalProfile>> {
    let Some(query_id) = query_id else {
        return Ok(None);
    };

    ensure_profiles_loaded()?;
    let profile = shmem::lookup_scoped_or_global_query_profile(scope_hash, query_id)?;
    let _ = maybe_persist_profiles(time::current_epoch_ms());
    Ok(profile)
}

pub(crate) fn record_observation(
    scope_hash: ScopeHash,
    query_id: Option<QueryId>,
    actual_wal_bytes: WalBytes,
    now_epoch_ms: EpochMillis,
) -> PwbResult<()> {
    let Some(query_id) = query_id else {
        return Ok(());
    };

    record_query_observation(scope_hash, query_id, actual_wal_bytes, now_epoch_ms)
}

pub(crate) fn record_query_observation(
    scope_hash: ScopeHash,
    query_id: QueryId,
    actual_wal_bytes: WalBytes,
    now_epoch_ms: EpochMillis,
) -> PwbResult<()> {
    ensure_profiles_loaded()?;
    let ewma_weights = guc::profile_ewma_alpha_weights()?;
    shmem::upsert_scoped_and_global_query_profiles(
        scope_hash,
        query_id,
        actual_wal_bytes,
        now_epoch_ms,
        ewma_weights,
    )
}

pub(crate) fn flush_profiles() -> PwbResult<()> {
    ensure_profiles_loaded()?;
    let now_epoch_ms = time::current_epoch_ms();
    shmem::persist_profiles_now(now_epoch_ms, profile_store::persist_profiles)
}

fn ensure_profiles_loaded() -> PwbResult<()> {
    let now_epoch_ms = time::current_epoch_ms();
    shmem::ensure_profiles_loaded(now_epoch_ms, PROFILE_RESTORE_STALE_MS, || {
        profile_store::load_profiles(guc::shmem_capacity())
    })
}

fn maybe_persist_profiles(now_epoch_ms: EpochMillis) -> PwbResult<()> {
    shmem::persist_profiles_if_due(
        now_epoch_ms,
        PROFILE_PERSIST_INTERVAL_MS,
        profile_store::persist_profiles,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_without_query_id_does_not_require_shared_memory() {
        assert_eq!(lookup_prediction_profile(99, None), Ok(None));
    }

    #[test]
    fn record_without_query_id_is_a_noop() {
        assert_eq!(record_observation(99, None, 1024, 123), Ok(()));
    }
}
