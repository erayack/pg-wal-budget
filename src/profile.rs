#![allow(dead_code)]
#![allow(clippy::redundant_pub_crate)]

use crate::errors::PwbResult;
use crate::shmem;
use crate::types::{EpochMillis, QueryId, QueryWalProfile, ScopeHash, WalBytes};

pub(crate) const EWMA_ALPHA_NUMERATOR: u64 = 1;
pub(crate) const EWMA_ALPHA_DENOMINATOR: u64 = 2;

pub(crate) fn lookup_prediction_profile(
    scope_hash: ScopeHash,
    query_id: Option<QueryId>,
) -> PwbResult<Option<QueryWalProfile>> {
    let Some(query_id) = query_id else {
        return Ok(None);
    };

    shmem::lookup_scoped_or_global_query_profile(scope_hash, query_id)
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

    shmem::upsert_scoped_and_global_query_profiles(
        scope_hash,
        query_id,
        actual_wal_bytes,
        now_epoch_ms,
        ewma_weights()?,
    )
}

fn ewma_weights() -> PwbResult<shmem::ProfileEwmaWeights> {
    shmem::ProfileEwmaWeights::new(EWMA_ALPHA_NUMERATOR, EWMA_ALPHA_DENOMINATOR)
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
