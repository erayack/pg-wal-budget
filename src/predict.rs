#![allow(dead_code)]

use crate::guc;
use crate::profile;
use crate::types::{QueryId, ScopeHash, StatementClass, WalBytes};

pub(crate) fn predict_wal_bytes(
    statement_class: StatementClass,
    query_id: Option<QueryId>,
    scope_hash: ScopeHash,
) -> WalBytes {
    if matches!(statement_class, StatementClass::ReadOnly) {
        return 0;
    }

    if let Ok(Some(profile)) = profile::lookup_prediction_profile(scope_hash, query_id) {
        return clamp_prediction(profile.ewma_wal_bytes, guc::max_prediction_bytes());
    }

    let fallback = fallback_wal_bytes_for_class(
        statement_class,
        guc::default_write_wal_bytes(),
        guc::default_utility_wal_bytes(),
    );

    clamp_prediction(fallback, guc::max_prediction_bytes())
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
    fn read_only_predicts_zero() {
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
}
