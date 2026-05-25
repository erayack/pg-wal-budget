use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting, PostgresGucEnum};

use crate::errors::{PwbError, PwbResult};
use crate::types::{ProfileEwmaWeights, WalBytes};

const DEFAULT_WRITE_WAL_BYTES_VALUE: i32 = 16 * 1024;
const DEFAULT_UTILITY_WAL_BYTES_VALUE: i32 = 1024 * 1024;
const MAX_PREDICTION_BYTES_VALUE: i32 = 1024 * 1024 * 1024;
const SHMEM_CAPACITY_VALUE: i32 = 4096;
const INHERIT_CAPACITY_VALUE: i32 = -1;
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
static SHMEM_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(SHMEM_CAPACITY_VALUE);
static RECENT_DECISION_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(INHERIT_CAPACITY_VALUE);
static PROFILE_CACHE_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(INHERIT_CAPACITY_VALUE);
static BUDGET_BUCKET_CAPACITY: GucSetting<i32> = GucSetting::<i32>::new(INHERIT_CAPACITY_VALUE);
static COMPOSITE_SCOPE_ENABLED: GucSetting<bool> = GucSetting::<bool>::new(false);
static PREDICTOR: GucSetting<PredictorKind> =
    GucSetting::<PredictorKind>::new(PredictorKind::ProfileEwma);
static PROFILE_EWMA_ALPHA: GucSetting<f64> =
    GucSetting::<f64>::new(DEFAULT_PROFILE_EWMA_ALPHA_VALUE);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PostgresGucEnum)]
pub(crate) enum PredictorKind {
    #[name = c"profile_ewma"]
    ProfileEwma,
    #[name = c"statement_class_fallback"]
    StatementClassFallback,
}

pub(crate) fn register_gucs() {
    register_runtime_gucs();
    register_prediction_gucs();
    register_capacity_gucs();
}

fn register_runtime_gucs() {
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
}

fn register_prediction_gucs() {
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

    GucRegistry::define_bool_guc(
        c"pwb.composite_scope_enabled",
        c"Enables composite pg_wal_budget scope classification.",
        c"Classifies statements by a canonical composite scope when at least two scope components are available. When disabled, pg_wal_budget uses tenant, role, database, then application precedence.",
        &COMPOSITE_SCOPE_ENABLED,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_enum_guc(
        c"pwb.predictor",
        c"Selects the pg_wal_budget WAL predictor.",
        c"Supported values are profile_ewma and statement_class_fallback.",
        &PREDICTOR,
        GucContext::Sighup,
        GucFlags::default(),
    );
}

fn register_capacity_gucs() {
    GucRegistry::define_int_guc(
        c"pwb.shmem_capacity",
        c"Default shared memory array capacity.",
        c"Legacy default capacity for pg_wal_budget shared-memory arrays. Specific capacity GUCs override it for recent decisions, query WAL profiles, and budget buckets. Changes require restart.",
        &SHMEM_CAPACITY,
        0,
        MAX_CAPACITY_VALUE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pwb.recent_decision_capacity",
        c"Recent decision ring capacity.",
        c"Capacity of the pg_wal_budget recent-decision shared-memory ring. -1 inherits pwb.shmem_capacity. Changes require restart.",
        &RECENT_DECISION_CAPACITY,
        INHERIT_CAPACITY_VALUE,
        MAX_CAPACITY_VALUE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pwb.profile_cache_capacity",
        c"Query WAL profile cache capacity.",
        c"Capacity of the pg_wal_budget shared-memory query profile cache. -1 inherits pwb.shmem_capacity. Changes require restart.",
        &PROFILE_CACHE_CAPACITY,
        INHERIT_CAPACITY_VALUE,
        MAX_CAPACITY_VALUE,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"pwb.budget_bucket_capacity",
        c"Budget bucket capacity.",
        c"Capacity of the pg_wal_budget shared-memory budget bucket array. -1 inherits pwb.shmem_capacity. Changes require restart.",
        &BUDGET_BUCKET_CAPACITY,
        INHERIT_CAPACITY_VALUE,
        MAX_CAPACITY_VALUE,
        GucContext::Postmaster,
        GucFlags::default(),
    );
}

pub(crate) fn enabled() -> bool {
    ENABLED.get()
}

pub(crate) fn fail_open() -> bool {
    FAIL_OPEN.get()
}

pub(crate) fn default_write_wal_bytes() -> WalBytes {
    nonnegative_i32_to_wal_bytes(DEFAULT_WRITE_WAL_BYTES.get())
}

pub(crate) fn default_utility_wal_bytes() -> WalBytes {
    nonnegative_i32_to_wal_bytes(DEFAULT_UTILITY_WAL_BYTES.get())
}

pub(crate) fn max_prediction_bytes() -> WalBytes {
    nonnegative_i32_to_wal_bytes(MAX_PREDICTION_BYTES.get())
}

pub(crate) fn shmem_capacity() -> usize {
    nonnegative_i32_to_usize(SHMEM_CAPACITY.get())
}

pub(crate) fn recent_decision_capacity() -> usize {
    specific_or_legacy_capacity(RECENT_DECISION_CAPACITY.get())
}

pub(crate) fn profile_cache_capacity() -> usize {
    specific_or_legacy_capacity(PROFILE_CACHE_CAPACITY.get())
}

pub(crate) fn budget_bucket_capacity() -> usize {
    specific_or_legacy_capacity(BUDGET_BUCKET_CAPACITY.get())
}

pub(crate) fn composite_scope_enabled() -> bool {
    COMPOSITE_SCOPE_ENABLED.get()
}

pub(crate) fn predictor_kind() -> PredictorKind {
    PREDICTOR.get()
}

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

fn specific_or_legacy_capacity(value: i32) -> usize {
    if value == INHERIT_CAPACITY_VALUE {
        shmem_capacity()
    } else {
        nonnegative_i32_to_usize(value)
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
        assert_eq!(nonnegative_i32_to_usize(SHMEM_CAPACITY_VALUE), 4096);
    }

    #[test]
    fn explicit_default_capacity_does_not_mean_inherit() {
        assert_eq!(
            nonnegative_i32_to_usize(SHMEM_CAPACITY_VALUE),
            SHMEM_CAPACITY_VALUE as usize
        );
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
