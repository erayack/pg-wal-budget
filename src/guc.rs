use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::errors::{PwbError, PwbResult};
use crate::types::{ProfileEwmaWeights, WalBytes};

const DEFAULT_WRITE_WAL_BYTES_VALUE: i32 = 16 * 1024;
const DEFAULT_UTILITY_WAL_BYTES_VALUE: i32 = 1024 * 1024;
const MAX_PREDICTION_BYTES_VALUE: i32 = 1024 * 1024 * 1024;
const RECENT_DECISION_CAPACITY_VALUE: i32 = 1024;
const PROFILE_CACHE_CAPACITY_VALUE: i32 = 4096;
const MAX_CAPACITY_VALUE: i32 = 1_000_000;
const DEFAULT_PROFILE_EWMA_ALPHA_VALUE: f64 = 0.5;
const MIN_PROFILE_EWMA_ALPHA_VALUE: f64 = 0.000_001;
const MAX_PROFILE_EWMA_ALPHA_VALUE: f64 = 1.0;
const PROFILE_EWMA_ALPHA_DENOMINATOR: u64 = 1_000_000;

static ENABLED: GucSetting<bool> = GucSetting::<bool>::new(true);
static FAIL_OPEN: GucSetting<bool> = GucSetting::<bool>::new(true);
static DEFAULT_WRITE_WAL_BYTES: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_WRITE_WAL_BYTES_VALUE);
static DEFAULT_UTILITY_WAL_BYTES: GucSetting<i32> =
    GucSetting::<i32>::new(DEFAULT_UTILITY_WAL_BYTES_VALUE);
static MAX_PREDICTION_BYTES: GucSetting<i32> = GucSetting::<i32>::new(MAX_PREDICTION_BYTES_VALUE);
static RECENT_DECISION_CAPACITY: GucSetting<i32> =
    GucSetting::<i32>::new(RECENT_DECISION_CAPACITY_VALUE);
static PROFILE_CACHE_CAPACITY: GucSetting<i32> =
    GucSetting::<i32>::new(PROFILE_CACHE_CAPACITY_VALUE);
static PROFILE_EWMA_ALPHA: GucSetting<f64> =
    GucSetting::<f64>::new(DEFAULT_PROFILE_EWMA_ALPHA_VALUE);

pub(crate) fn register_gucs() {
    GucRegistry::define_bool_guc(
        c"pwb.enabled",
        c"Enables pg_wal_budget admission checks.",
        c"Enables pg_wal_budget hook behavior. When disabled, future hooks should allow statements without budget accounting.",
        &ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        c"pwb.fail_open",
        c"Allows statements when pg_wal_budget hits an internal error.",
        c"Controls whether pg_wal_budget allows statements after internal classification, prediction, or accounting errors.",
        &FAIL_OPEN,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pwb.default_write_wal_bytes",
        c"Fallback WAL prediction for write statements.",
        c"Fallback predicted WAL bytes used for write statements without a learned query profile.",
        &DEFAULT_WRITE_WAL_BYTES,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_int_guc(
        c"pwb.default_utility_wal_bytes",
        c"Fallback WAL prediction for utility statements.",
        c"Fallback predicted WAL bytes used for utility and COPY statements without a learned query profile.",
        &DEFAULT_UTILITY_WAL_BYTES,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_int_guc(
        c"pwb.max_prediction_bytes",
        c"Maximum WAL prediction.",
        c"Upper bound applied to pg_wal_budget predicted WAL bytes before admission decisions.",
        &MAX_PREDICTION_BYTES,
        0,
        i32::MAX,
        GucContext::Sighup,
        GucFlags::UNIT_BYTE,
    );

    GucRegistry::define_float_guc(
        c"pwb.profile_ewma_alpha",
        c"EWMA smoothing factor for query WAL profiles.",
        c"Controls how strongly learned pg_wal_budget query WAL profiles weight the latest observation. Higher values react faster; lower values smooth over more history.",
        &PROFILE_EWMA_ALPHA,
        MIN_PROFILE_EWMA_ALPHA_VALUE,
        MAX_PROFILE_EWMA_ALPHA_VALUE,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pwb.recent_decision_capacity",
        c"Recent decision ring buffer capacity.",
        c"Maximum number of recent pg_wal_budget admission decisions retained in shared memory. Changes require restart.",
        &RECENT_DECISION_CAPACITY,
        0,
        MAX_CAPACITY_VALUE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pwb.profile_cache_capacity",
        c"Query profile cache capacity.",
        c"Maximum number of query WAL profiles retained in shared memory. Changes require restart.",
        &PROFILE_CACHE_CAPACITY,
        0,
        MAX_CAPACITY_VALUE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

#[allow(dead_code)]
pub(crate) fn enabled() -> bool {
    ENABLED.get()
}

#[allow(dead_code)]
pub(crate) fn fail_open() -> bool {
    FAIL_OPEN.get()
}

#[allow(dead_code)]
pub(crate) fn default_write_wal_bytes() -> WalBytes {
    nonnegative_i32_to_wal_bytes(DEFAULT_WRITE_WAL_BYTES.get())
}

#[allow(dead_code)]
pub(crate) fn default_utility_wal_bytes() -> WalBytes {
    nonnegative_i32_to_wal_bytes(DEFAULT_UTILITY_WAL_BYTES.get())
}

#[allow(dead_code)]
pub(crate) fn max_prediction_bytes() -> WalBytes {
    nonnegative_i32_to_wal_bytes(MAX_PREDICTION_BYTES.get())
}

#[allow(dead_code)]
pub(crate) fn recent_decision_capacity() -> usize {
    nonnegative_i32_to_usize(RECENT_DECISION_CAPACITY.get())
}

#[allow(dead_code)]
pub(crate) fn profile_cache_capacity() -> usize {
    nonnegative_i32_to_usize(PROFILE_CACHE_CAPACITY.get())
}

#[allow(dead_code)]
pub(crate) fn profile_ewma_alpha_weights() -> PwbResult<ProfileEwmaWeights> {
    alpha_to_profile_ewma_weights(PROFILE_EWMA_ALPHA.get())
}

const fn nonnegative_i32_to_wal_bytes(value: i32) -> WalBytes {
    if value <= 0 {
        0
    } else {
        value.cast_unsigned() as WalBytes
    }
}

const fn nonnegative_i32_to_usize(value: i32) -> usize {
    if value <= 0 {
        0
    } else {
        value.cast_unsigned() as usize
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "profile EWMA alpha is range-checked to 0.000_001..=1.0 and scaled by a small fixed denominator"
)]
fn alpha_to_profile_ewma_weights(alpha: f64) -> PwbResult<ProfileEwmaWeights> {
    if !alpha.is_finite()
        || !(MIN_PROFILE_EWMA_ALPHA_VALUE..=MAX_PROFILE_EWMA_ALPHA_VALUE).contains(&alpha)
    {
        return Err(PwbError::Internal {
            message: format!("invalid profile EWMA alpha: {alpha}"),
        });
    }

    let numerator = (alpha * PROFILE_EWMA_ALPHA_DENOMINATOR as f64).round() as u64;
    ProfileEwmaWeights::new(
        numerator.clamp(1, PROFILE_EWMA_ALPHA_DENOMINATOR),
        PROFILE_EWMA_ALPHA_DENOMINATOR,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_defaults_fit_wal_bytes() {
        assert_eq!(
            nonnegative_i32_to_wal_bytes(DEFAULT_WRITE_WAL_BYTES_VALUE),
            16_384
        );
        assert_eq!(
            nonnegative_i32_to_wal_bytes(DEFAULT_UTILITY_WAL_BYTES_VALUE),
            1_048_576
        );
        assert_eq!(
            nonnegative_i32_to_wal_bytes(MAX_PREDICTION_BYTES_VALUE),
            1_073_741_824
        );
    }

    #[test]
    fn capacity_defaults_fit_usize() {
        assert_eq!(
            nonnegative_i32_to_usize(RECENT_DECISION_CAPACITY_VALUE),
            1024
        );
        assert_eq!(nonnegative_i32_to_usize(PROFILE_CACHE_CAPACITY_VALUE), 4096);
    }

    #[test]
    fn negative_values_convert_to_zero_at_accessor_boundary() {
        assert_eq!(nonnegative_i32_to_wal_bytes(-1), 0);
        assert_eq!(nonnegative_i32_to_usize(-1), 0);
    }

    #[test]
    fn default_profile_ewma_alpha_matches_previous_half_weight() {
        let weights = alpha_to_profile_ewma_weights(DEFAULT_PROFILE_EWMA_ALPHA_VALUE)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(weights.numerator, 500_000);
        assert_eq!(weights.denominator, PROFILE_EWMA_ALPHA_DENOMINATOR);
    }

    #[test]
    fn profile_ewma_alpha_converts_common_values() {
        let low = alpha_to_profile_ewma_weights(0.2).unwrap_or_else(|error| panic!("{error}"));
        let max = alpha_to_profile_ewma_weights(1.0).unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(low.numerator, 200_000);
        assert_eq!(low.denominator, PROFILE_EWMA_ALPHA_DENOMINATOR);
        assert_eq!(max.numerator, PROFILE_EWMA_ALPHA_DENOMINATOR);
        assert_eq!(max.denominator, PROFILE_EWMA_ALPHA_DENOMINATOR);
    }

    #[test]
    fn minimum_profile_ewma_alpha_converts_to_nonzero_numerator() {
        let weights = alpha_to_profile_ewma_weights(MIN_PROFILE_EWMA_ALPHA_VALUE)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(weights.numerator, 1);
        assert_eq!(weights.denominator, PROFILE_EWMA_ALPHA_DENOMINATOR);
    }

    #[test]
    fn invalid_profile_ewma_alpha_values_are_rejected() {
        assert!(alpha_to_profile_ewma_weights(0.0).is_err());
        assert!(alpha_to_profile_ewma_weights(MIN_PROFILE_EWMA_ALPHA_VALUE / 2.0).is_err());
        assert!(alpha_to_profile_ewma_weights(-0.1).is_err());
        assert!(alpha_to_profile_ewma_weights(f64::NAN).is_err());
        assert!(alpha_to_profile_ewma_weights(f64::INFINITY).is_err());
        assert!(alpha_to_profile_ewma_weights(1.1).is_err());
    }
}
