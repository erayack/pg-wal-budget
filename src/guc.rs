use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};

use crate::types::WalBytes;

const DEFAULT_WRITE_WAL_BYTES_VALUE: i32 = 16 * 1024;
const DEFAULT_UTILITY_WAL_BYTES_VALUE: i32 = 1024 * 1024;
const MAX_PREDICTION_BYTES_VALUE: i32 = 1024 * 1024 * 1024;
const RECENT_DECISION_CAPACITY_VALUE: i32 = 1024;
const PROFILE_CACHE_CAPACITY_VALUE: i32 = 4096;
const MAX_CAPACITY_VALUE: i32 = 1_000_000;

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
}
