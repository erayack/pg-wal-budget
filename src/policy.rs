#![allow(dead_code)]
#![allow(clippy::redundant_pub_crate)]

use std::cell::RefCell;
use std::time::{SystemTime, UNIX_EPOCH};

use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi;
use pgrx::{PgLogLevel, PgSqlErrorCode, Spi, ereport};

use crate::budget::EffectivePolicy;
use crate::errors::{PwbError, PwbResult};
use crate::types::{BudgetMode, EpochMillis, PolicyId, ScopeKey, ScopeKind, WalBytes};

const POLICY_CACHE_REFRESH_INTERVAL_MS: EpochMillis = 1000;

thread_local! {
    static POLICY_CACHE: RefCell<Option<PolicyCache>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PolicyDefinition {
    pub(crate) enabled: bool,
    pub(crate) mode: BudgetMode,
    pub(crate) scope_kind: ScopeKind,
    pub(crate) scope_value: Option<String>,
    pub(crate) wal_rate_bytes_per_sec: WalBytes,
    pub(crate) wal_burst_bytes: WalBytes,
    pub(crate) priority: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawEffectivePolicy {
    policy_id: Option<PolicyId>,
    scope_kind: Option<String>,
    scope_value: Option<String>,
    enabled: Option<bool>,
    mode: Option<String>,
    wal_rate_bytes_per_sec: Option<i64>,
    wal_burst_bytes: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedEffectivePolicy {
    scope_kind: ScopeKind,
    scope_value: Option<String>,
    policy: EffectivePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyCache {
    refreshed_epoch_ms: EpochMillis,
    policies: Vec<CachedEffectivePolicy>,
}

pub(crate) fn validate_policy_definition(
    scope_kind: &str,
    scope_value: Option<&str>,
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
    mode: &str,
    priority: i32,
) -> PwbResult<PolicyDefinition> {
    let mode = BudgetMode::parse_sql(mode)?;
    let scope_kind = ScopeKind::parse_sql(scope_kind)?;
    let wal_rate_bytes_per_sec =
        validate_positive_wal_bytes("wal_rate_bytes_per_sec", wal_rate_bytes_per_sec)?;
    let wal_burst_bytes = validate_positive_wal_bytes("wal_burst_bytes", wal_burst_bytes)?;

    if wal_burst_bytes < wal_rate_bytes_per_sec {
        return Err(PwbError::InvalidPolicyValue {
            field: "wal_burst_bytes",
            value: wal_burst_bytes.to_string(),
            reason: "must be greater than or equal to wal_rate_bytes_per_sec",
        });
    }

    Ok(PolicyDefinition {
        enabled: true,
        mode,
        scope_kind,
        scope_value: normalize_scope_value(scope_value),
        wal_rate_bytes_per_sec,
        wal_burst_bytes,
        priority,
    })
}

pub(crate) fn validate_policy_mode(mode: &str) -> PwbResult<BudgetMode> {
    BudgetMode::parse_sql(mode)
}

pub(crate) fn effective_policy_for_scope(scope: &ScopeKey) -> PwbResult<Option<EffectivePolicy>> {
    let now_epoch_ms = current_epoch_ms();

    let should_reload = POLICY_CACHE.with(|cache| {
        cache
            .borrow()
            .as_ref()
            .is_none_or(|cache| cache_is_stale(cache, now_epoch_ms))
    });
    if should_reload {
        let loaded_cache = load_policy_cache(now_epoch_ms)?;
        POLICY_CACHE.with(|cache| {
            *cache.borrow_mut() = Some(loaded_cache);
        });
    }

    POLICY_CACHE.with(|cache| {
        let cache = cache.borrow();
        let cache = cache.as_ref().ok_or_else(|| PwbError::Internal {
            message: "policy cache was not initialized".to_string(),
        })?;
        Ok(find_effective_policy(cache, scope))
    })
}

pub(crate) fn invalidate_backend_policy_cache() {
    POLICY_CACHE.with(|cache| {
        *cache.borrow_mut() = None;
    });
}

fn load_policy_cache(now_epoch_ms: EpochMillis) -> PwbResult<PolicyCache> {
    let raw_policy = Spi::connect(|client| {
        let table = client.select(
            "
                select
                  policy_id,
                  scope_kind,
                  scope_value,
                  enabled,
                  mode,
                  wal_rate_bytes_per_sec,
                  wal_burst_bytes
                from pwb.policy
                where enabled = true
                order by priority desc, policy_id asc
                ",
            None,
            &[],
        )?;

        let mut policies = Vec::with_capacity(table.len());
        for row in table {
            policies.push(RawEffectivePolicy {
                policy_id: row.get_by_name::<PolicyId, _>("policy_id")?,
                scope_kind: row.get_by_name::<String, _>("scope_kind")?,
                scope_value: row.get_by_name::<String, _>("scope_value")?,
                enabled: row.get_by_name::<bool, _>("enabled")?,
                mode: row.get_by_name::<String, _>("mode")?,
                wal_rate_bytes_per_sec: row.get_by_name::<i64, _>("wal_rate_bytes_per_sec")?,
                wal_burst_bytes: row.get_by_name::<i64, _>("wal_burst_bytes")?,
            });
        }
        Ok(policies)
    })
    .map_err(spi_error)?;

    let mut policies = Vec::with_capacity(raw_policy.len());
    for raw_policy in raw_policy {
        policies.push(decode_cached_policy(raw_policy)?);
    }

    Ok(PolicyCache {
        refreshed_epoch_ms: now_epoch_ms,
        policies,
    })
}

fn decode_cached_policy(raw_policy: RawEffectivePolicy) -> PwbResult<CachedEffectivePolicy> {
    let policy_id = raw_policy.policy_id.ok_or_else(|| PwbError::Internal {
        message: "effective policy row is missing policy_id".to_string(),
    })?;
    let scope_kind = raw_policy
        .scope_kind
        .ok_or_else(|| PwbError::Internal {
            message: "effective policy row is missing scope_kind".to_string(),
        })
        .and_then(|scope_kind| ScopeKind::parse_sql(&scope_kind))?;
    let enabled = raw_policy.enabled.ok_or_else(|| PwbError::Internal {
        message: "effective policy row is missing enabled".to_string(),
    })?;
    let mode = raw_policy
        .mode
        .ok_or_else(|| PwbError::Internal {
            message: "effective policy row is missing mode".to_string(),
        })
        .and_then(|mode| BudgetMode::parse_sql(&mode))?;
    let wal_rate_bytes_per_sec = raw_policy
        .wal_rate_bytes_per_sec
        .ok_or_else(|| PwbError::Internal {
            message: "effective policy row is missing wal_rate_bytes_per_sec".to_string(),
        })
        .and_then(|value| validate_positive_wal_bytes("wal_rate_bytes_per_sec", value))?;
    let wal_burst_bytes = raw_policy
        .wal_burst_bytes
        .ok_or_else(|| PwbError::Internal {
            message: "effective policy row is missing wal_burst_bytes".to_string(),
        })
        .and_then(|value| validate_positive_wal_bytes("wal_burst_bytes", value))?;

    Ok(CachedEffectivePolicy {
        scope_kind,
        scope_value: normalize_scope_value(raw_policy.scope_value.as_deref()),
        policy: EffectivePolicy {
            policy_id,
            enabled,
            mode,
            wal_rate_bytes_per_sec,
            wal_burst_bytes,
        },
    })
}

fn find_effective_policy(cache: &PolicyCache, scope: &ScopeKey) -> Option<EffectivePolicy> {
    cache
        .policies
        .iter()
        .find(|policy| policy_matches_scope(policy, scope))
        .map(|policy| policy.policy)
}

fn policy_matches_scope(policy: &CachedEffectivePolicy, scope: &ScopeKey) -> bool {
    policy.scope_kind == scope.kind
        && policy
            .scope_value
            .as_deref()
            .is_none_or(|policy_scope| Some(policy_scope) == scope.debug_value.as_deref())
}

const fn cache_is_stale(cache: &PolicyCache, now_epoch_ms: EpochMillis) -> bool {
    now_epoch_ms.saturating_sub(cache.refreshed_epoch_ms) >= POLICY_CACHE_REFRESH_INTERVAL_MS
}

#[pg_extern]
fn pwb_create_policy(
    scope_kind: &str,
    scope_value: default!(Option<&str>, "NULL"),
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
    mode: default!(&str, "'observe'"),
    priority: default!(i32, "100"),
) -> i32 {
    create_policy_impl(
        scope_kind,
        scope_value,
        wal_rate_bytes_per_sec,
        wal_burst_bytes,
        mode,
        priority,
    )
    .unwrap_or_else(raise_pwb_error)
}

#[pg_extern]
fn pwb_set_policy_mode(policy_id: i32, mode: &str) {
    set_policy_mode_impl(policy_id, mode).unwrap_or_else(raise_pwb_error);
}

#[pg_extern]
fn pwb_disable_policy(policy_id: i32) {
    disable_policy_impl(policy_id).unwrap_or_else(raise_pwb_error);
}

fn create_policy_impl(
    scope_kind: &str,
    scope_value: Option<&str>,
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
    mode: &str,
    priority: i32,
) -> PwbResult<PolicyId> {
    let policy = validate_policy_definition(
        scope_kind,
        scope_value,
        wal_rate_bytes_per_sec,
        wal_burst_bytes,
        mode,
        priority,
    )?;

    let rate = wal_bytes_to_i64("wal_rate_bytes_per_sec", policy.wal_rate_bytes_per_sec)?;
    let burst = wal_bytes_to_i64("wal_burst_bytes", policy.wal_burst_bytes)?;
    let mode = policy.mode.as_sql_str();
    let scope_kind = policy.scope_kind.as_sql_str();

    let policy_id = Spi::get_one_with_args::<i32>(
        "
        insert into pwb.policy (
          enabled,
          mode,
          scope_kind,
          scope_value,
          wal_rate_bytes_per_sec,
          wal_burst_bytes,
          priority
        )
        values ($1, $2, $3, $4, $5, $6, $7)
        returning policy_id
        ",
        &[
            policy.enabled.into(),
            mode.into(),
            scope_kind.into(),
            nullable_text_arg(policy.scope_value.as_deref()),
            rate.into(),
            burst.into(),
            policy.priority.into(),
        ],
    )
    .map_err(spi_error)?;

    let policy_id = policy_id.ok_or_else(|| PwbError::Internal {
        message: "policy insert did not return policy_id".to_string(),
    })?;
    invalidate_backend_policy_cache();
    Ok(policy_id)
}

fn set_policy_mode_impl(policy_id: PolicyId, mode: &str) -> PwbResult<()> {
    let mode = validate_policy_mode(mode)?;
    let updated = Spi::get_one_with_args::<bool>(
        "
        update pwb.policy
           set mode = $2
         where policy_id = $1
        returning true
        ",
        &[policy_id.into(), mode.as_sql_str().into()],
    )
    .map_err(spi_error)?;

    require_policy_updated(policy_id, updated)?;
    invalidate_backend_policy_cache();
    Ok(())
}

fn disable_policy_impl(policy_id: PolicyId) -> PwbResult<()> {
    let updated = Spi::get_one_with_args::<bool>(
        "
        update pwb.policy
           set enabled = false
         where policy_id = $1
        returning true
        ",
        &[policy_id.into()],
    )
    .map_err(spi_error)?;

    require_policy_updated(policy_id, updated)?;
    invalidate_backend_policy_cache();
    Ok(())
}

fn validate_positive_wal_bytes(field: &'static str, value: i64) -> PwbResult<WalBytes> {
    if value <= 0 {
        return Err(PwbError::InvalidPolicyValue {
            field,
            value: value.to_string(),
            reason: "must be greater than zero",
        });
    }

    u64::try_from(value).map_err(|_| PwbError::InvalidPolicyValue {
        field,
        value: value.to_string(),
        reason: "must fit in PostgreSQL bigint",
    })
}

fn wal_bytes_to_i64(field: &'static str, value: WalBytes) -> PwbResult<i64> {
    i64::try_from(value).map_err(|_| PwbError::InvalidPolicyValue {
        field,
        value: value.to_string(),
        reason: "must fit in PostgreSQL bigint",
    })
}

fn normalize_scope_value(scope_value: Option<&str>) -> Option<String> {
    scope_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn nullable_text_arg(value: Option<&str>) -> DatumWithOid<'_> {
    value.map_or_else(DatumWithOid::null::<String>, DatumWithOid::from)
}

fn current_epoch_ms() -> EpochMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            EpochMillis::try_from(duration.as_millis()).unwrap_or(EpochMillis::MAX)
        })
}

fn require_policy_updated(policy_id: PolicyId, updated: Option<bool>) -> PwbResult<()> {
    if updated == Some(true) {
        Ok(())
    } else {
        Err(PwbError::InvalidPolicyValue {
            field: "policy_id",
            value: policy_id.to_string(),
            reason: "policy does not exist",
        })
    }
}

#[allow(clippy::needless_pass_by_value)]
fn spi_error(error: spi::Error) -> PwbError {
    PwbError::Internal {
        message: format!("SPI policy operation failed: {error}"),
    }
}

#[allow(clippy::needless_pass_by_value)]
fn raise_pwb_error<T>(error: PwbError) -> T {
    let message = error.message();
    let detail = error.to_string();
    let sqlstate = match error {
        PwbError::InvalidBudgetMode { .. }
        | PwbError::InvalidScopeKind { .. }
        | PwbError::InvalidPolicyValue { .. } => PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
        PwbError::InsufficientPrivilege { .. } => PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
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
    use crate::types::ScopeHash;

    const POLICY_ID: PolicyId = 11;
    const SCOPE_HASH: ScopeHash = 99;

    #[test]
    fn validates_and_normalizes_policy_definition() {
        let policy =
            match validate_policy_definition(" ROLE ", Some(" tenant-a "), 100, 500, "SHADOW", 7) {
                Ok(policy) => policy,
                Err(error) => panic!("expected valid policy, got {error}"),
            };

        assert!(policy.enabled);
        assert_eq!(policy.mode, BudgetMode::Shadow);
        assert_eq!(policy.scope_kind, ScopeKind::Role);
        assert_eq!(policy.scope_value, Some("tenant-a".to_string()));
        assert_eq!(policy.wal_rate_bytes_per_sec, 100);
        assert_eq!(policy.wal_burst_bytes, 500);
        assert_eq!(policy.priority, 7);
    }

    #[test]
    fn normalizes_empty_scope_value_to_none() {
        let policy = match validate_policy_definition("database", Some("   "), 1, 1, "observe", 100)
        {
            Ok(policy) => policy,
            Err(error) => panic!("expected valid policy, got {error}"),
        };

        assert_eq!(policy.scope_value, None);
    }

    #[test]
    fn rejects_nonpositive_rate() {
        let error = match validate_policy_definition("database", None, 0, 1, "observe", 100) {
            Ok(policy) => panic!("expected invalid rate, got {policy:?}"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            PwbError::InvalidPolicyValue {
                field: "wal_rate_bytes_per_sec",
                ..
            }
        ));
    }

    #[test]
    fn rejects_burst_below_rate() {
        let error = match validate_policy_definition("database", None, 10, 9, "observe", 100) {
            Ok(policy) => panic!("expected invalid burst, got {policy:?}"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            PwbError::InvalidPolicyValue {
                field: "wal_burst_bytes",
                ..
            }
        ));
    }

    #[test]
    fn rejects_missing_policy_update() {
        let error = match require_policy_updated(42, None) {
            Ok(()) => panic!("expected missing policy error"),
            Err(error) => error,
        };

        assert!(matches!(
            error,
            PwbError::InvalidPolicyValue {
                field: "policy_id",
                ..
            }
        ));
    }

    #[test]
    fn policy_cache_matches_first_enabled_policy_by_scope_order() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![
                cached_policy(POLICY_ID, ScopeKind::Tenant, Some("tenant-a")),
                cached_policy(POLICY_ID + 1, ScopeKind::Tenant, None),
            ],
        };
        let scope = ScopeKey::with_debug_value(ScopeKind::Tenant, SCOPE_HASH, "tenant-a");

        assert_eq!(
            find_effective_policy(&cache, &scope).map(|policy| policy.policy_id),
            Some(POLICY_ID)
        );
    }

    #[test]
    fn policy_cache_uses_wildcard_scope_when_exact_value_does_not_match() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![
                cached_policy(POLICY_ID, ScopeKind::Tenant, Some("tenant-a")),
                cached_policy(POLICY_ID + 1, ScopeKind::Tenant, None),
            ],
        };
        let scope = ScopeKey::with_debug_value(ScopeKind::Tenant, SCOPE_HASH, "tenant-b");

        assert_eq!(
            find_effective_policy(&cache, &scope).map(|policy| policy.policy_id),
            Some(POLICY_ID + 1)
        );
    }

    #[test]
    fn policy_cache_does_not_match_missing_debug_value_to_exact_policy() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![cached_policy(
                POLICY_ID,
                ScopeKind::Tenant,
                Some("tenant-a"),
            )],
        };
        let scope = ScopeKey::new(ScopeKind::Tenant, SCOPE_HASH);

        assert_eq!(find_effective_policy(&cache, &scope), None);
    }

    #[test]
    fn policy_cache_refresh_interval_bounds_staleness() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: Vec::new(),
        };

        assert!(!cache_is_stale(&cache, 1999));
        assert!(cache_is_stale(&cache, 2000));
    }

    fn cached_policy(
        policy_id: PolicyId,
        scope_kind: ScopeKind,
        scope_value: Option<&str>,
    ) -> CachedEffectivePolicy {
        CachedEffectivePolicy {
            scope_kind,
            scope_value: scope_value.map(ToString::to_string),
            policy: EffectivePolicy {
                policy_id,
                enabled: true,
                mode: BudgetMode::Reject,
                wal_rate_bytes_per_sec: 100,
                wal_burst_bytes: 100,
            },
        }
    }
}
