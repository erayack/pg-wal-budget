use crate::errors::PwbResult;
use crate::guc;
use crate::profile_store;
use crate::shmem;
use crate::time;
use crate::types::{EpochMillis, QueryId, QueryWalProfile, ScopeHash, StatementClass, WalBytes};

const PROFILE_PERSIST_INTERVAL_MS: EpochMillis = 60_000;
const PROFILE_RESTORE_STALE_MS: EpochMillis = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PredictionContext {
    pub(crate) statement_class: StatementClass,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) scope_hash: ScopeHash,
}

pub(crate) fn predict_context(context: &PredictionContext) -> WalBytes {
    let statement_class = context.statement_class;
    if matches!(statement_class, StatementClass::ReadOnly) {
        return 0;
    }

    if matches!(guc::predictor_kind(), guc::PredictorKind::ProfileEwma)
        && let Ok(Some(profile)) = lookup_prediction_profile(context.scope_hash, context.query_id)
    {
        return clamp_prediction(profile.ewma_wal_bytes, guc::max_prediction_bytes());
    }

    let fallback = fallback_wal_bytes_for_class(
        statement_class,
        guc::default_write_wal_bytes(),
        guc::default_utility_wal_bytes(),
    );

    clamp_prediction(fallback, guc::max_prediction_bytes())
}

fn lookup_prediction_profile(
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
        profile_store::load_profiles(guc::profile_cache_capacity())
    })
}

fn maybe_persist_profiles(now_epoch_ms: EpochMillis) -> PwbResult<()> {
    shmem::persist_profiles_if_due(
        now_epoch_ms,
        PROFILE_PERSIST_INTERVAL_MS,
        profile_store::persist_profiles,
    )
}

const fn fallback_wal_bytes_for_class(
    statement_class: StatementClass,
    default_write: WalBytes,
    default_utility: WalBytes,
) -> WalBytes {
    match statement_class {
        StatementClass::ReadOnly => 0,
        StatementClass::Write => default_write,
        StatementClass::Utility | StatementClass::Copy | StatementClass::Unknown => default_utility,
    }
}

const fn clamp_prediction(predicted: WalBytes, max_prediction: WalBytes) -> WalBytes {
    if predicted > max_prediction {
        max_prediction
    } else {
        predicted
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_WRITE: WalBytes = 16 * 1024;
    const DEFAULT_UTILITY: WalBytes = 1024 * 1024;

    #[test]
    fn read_only_predicts_zero_without_shared_memory() {
        assert_eq!(
            predict_context(&PredictionContext {
                statement_class: StatementClass::ReadOnly,
                query_id: Some(42),
                scope_hash: 99,
            }),
            0
        );
    }

    #[test]
    fn read_only_fallback_is_zero() {
        assert_eq!(
            fallback_wal_bytes_for_class(StatementClass::ReadOnly, DEFAULT_WRITE, DEFAULT_UTILITY),
            0
        );
    }

    #[test]
    fn write_uses_write_fallback() {
        assert_eq!(
            fallback_wal_bytes_for_class(StatementClass::Write, DEFAULT_WRITE, DEFAULT_UTILITY),
            DEFAULT_WRITE
        );
    }

    #[test]
    fn utility_and_copy_use_utility_fallback() {
        assert_eq!(
            fallback_wal_bytes_for_class(StatementClass::Utility, DEFAULT_WRITE, DEFAULT_UTILITY),
            DEFAULT_UTILITY
        );
        assert_eq!(
            fallback_wal_bytes_for_class(StatementClass::Copy, DEFAULT_WRITE, DEFAULT_UTILITY),
            DEFAULT_UTILITY
        );
    }

    #[test]
    fn unknown_uses_conservative_utility_fallback() {
        assert_eq!(
            fallback_wal_bytes_for_class(StatementClass::Unknown, DEFAULT_WRITE, DEFAULT_UTILITY),
            DEFAULT_UTILITY
        );
    }

    #[test]
    fn prediction_is_clamped_to_max() {
        assert_eq!(clamp_prediction(1024, 512), 512);
    }

    #[test]
    fn zero_max_clamps_to_zero() {
        assert_eq!(clamp_prediction(1024, 0), 0);
    }

    #[test]
    fn lookup_without_query_id_does_not_require_shared_memory() {
        assert_eq!(lookup_prediction_profile(99, None), Ok(None));
    }
}
