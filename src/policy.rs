use pgrx::Spi;
use pgrx::datum::DatumWithOid;
use pgrx::prelude::*;
use pgrx::spi;

use crate::errors::{self, PwbError, PwbResult};
use crate::hooks;
use crate::privileges::{self, PrivilegeGate};
use crate::types::{BudgetMode, PolicyId, ScopeKind, WalBytes};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PolicyBudgetUpdate {
    pub(crate) wal_rate_bytes_per_sec: WalBytes,
    pub(crate) wal_burst_bytes: WalBytes,
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
    let budget = validate_policy_budget_update(wal_rate_bytes_per_sec, wal_burst_bytes)?;

    Ok(PolicyDefinition {
        enabled: true,
        mode,
        scope_kind,
        scope_value: normalize_scope_value(scope_value),
        wal_rate_bytes_per_sec: budget.wal_rate_bytes_per_sec,
        wal_burst_bytes: budget.wal_burst_bytes,
        priority,
    })
}

pub(crate) fn validate_policy_mode(mode: &str) -> PwbResult<BudgetMode> {
    BudgetMode::parse_sql(mode)
}

pub(crate) use crate::policy_cache::{effective_policy_for_scope, invalidate_backend_policy_cache};

#[pg_extern]
fn pwb_create_policy(
    scope_kind: &str,
    scope_value: Option<&str>,
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
    mode: default!(&str, "'observe'"),
    priority: default!(i32, "100"),
) -> i32 {
    privileges::require(PrivilegeGate::Admin, "create pg_wal_budget policy")
        .unwrap_or_else(errors::raise);
    create_policy_impl(
        scope_kind,
        scope_value,
        wal_rate_bytes_per_sec,
        wal_burst_bytes,
        mode,
        priority,
    )
    .unwrap_or_else(errors::raise)
}

#[pg_extern]
fn pwb_set_policy_mode(policy_id: i32, mode: &str) {
    privileges::require(PrivilegeGate::Admin, "set pg_wal_budget policy mode")
        .unwrap_or_else(errors::raise);
    set_policy_mode_impl(policy_id, mode).unwrap_or_else(errors::raise);
}

#[pg_extern]
fn pwb_update_policy(
    policy_id: i32,
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
    priority: default!(Option<i32>, "NULL"),
) {
    privileges::require(PrivilegeGate::Admin, "update pg_wal_budget policy")
        .unwrap_or_else(errors::raise);
    update_policy_impl(policy_id, wal_rate_bytes_per_sec, wal_burst_bytes, priority)
        .unwrap_or_else(errors::raise);
}

#[pg_extern]
fn pwb_disable_policy(policy_id: i32) {
    privileges::require(PrivilegeGate::Admin, "disable pg_wal_budget policy")
        .unwrap_or_else(errors::raise);
    disable_policy_impl(policy_id).unwrap_or_else(errors::raise);
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

    let policy_id = hooks::with_admission_bypass(|| {
        Spi::get_one_with_args::<i32>(
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
    })
    .map_err(spi_error)?;

    let policy_id = policy_id.ok_or_else(|| PwbError::Internal {
        message: "policy insert did not return policy_id".to_string(),
    })?;
    invalidate_backend_policy_cache();
    Ok(policy_id)
}

fn set_policy_mode_impl(policy_id: PolicyId, mode: &str) -> PwbResult<()> {
    let mode = validate_policy_mode(mode)?;

    let updated = hooks::with_admission_bypass(|| {
        Spi::get_one_with_args::<bool>(
            "
        with updated as (
        update pwb.policy
           set mode = $2
         where policy_id = $1
        returning true
        )
        select exists (select 1 from updated)
        ",
            &[policy_id.into(), mode.as_sql_str().into()],
        )
    })
    .map_err(spi_error)?;

    require_policy_updated(policy_id, updated)?;
    invalidate_backend_policy_cache();
    Ok(())
}

fn update_policy_impl(
    policy_id: PolicyId,
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
    priority: Option<i32>,
) -> PwbResult<()> {
    let budget = validate_policy_budget_update(wal_rate_bytes_per_sec, wal_burst_bytes)?;
    let rate = wal_bytes_to_i64("wal_rate_bytes_per_sec", budget.wal_rate_bytes_per_sec)?;
    let burst = wal_bytes_to_i64("wal_burst_bytes", budget.wal_burst_bytes)?;

    let updated = hooks::with_admission_bypass(|| {
        Spi::get_one_with_args::<bool>(
            "
        with updated as (
        update pwb.policy
           set wal_rate_bytes_per_sec = $2,
               wal_burst_bytes = $3,
               priority = coalesce($4, priority)
         where policy_id = $1
        returning true
        )
        select exists (select 1 from updated)
        ",
            &[
                policy_id.into(),
                rate.into(),
                burst.into(),
                nullable_i32_arg(priority),
            ],
        )
    })
    .map_err(spi_error)?;

    require_policy_updated(policy_id, updated)?;
    invalidate_backend_policy_cache();
    Ok(())
}

fn disable_policy_impl(policy_id: PolicyId) -> PwbResult<()> {
    let updated = hooks::with_admission_bypass(|| {
        Spi::get_one_with_args::<bool>(
            "
        with updated as (
        update pwb.policy
           set enabled = false
         where policy_id = $1
        returning true
        )
        select exists (select 1 from updated)
        ",
            &[policy_id.into()],
        )
    })
    .map_err(spi_error)?;

    require_policy_updated(policy_id, updated)?;
    invalidate_backend_policy_cache();
    Ok(())
}

pub(crate) fn validate_policy_budget_update(
    wal_rate_bytes_per_sec: i64,
    wal_burst_bytes: i64,
) -> PwbResult<PolicyBudgetUpdate> {
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

    Ok(PolicyBudgetUpdate {
        wal_rate_bytes_per_sec,
        wal_burst_bytes,
    })
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

pub(crate) fn normalize_scope_value(scope_value: Option<&str>) -> Option<String> {
    scope_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn nullable_text_arg(value: Option<&str>) -> DatumWithOid<'_> {
    value.map_or_else(DatumWithOid::null::<String>, DatumWithOid::from)
}

fn nullable_i32_arg(value: Option<i32>) -> DatumWithOid<'static> {
    value.map_or_else(DatumWithOid::null::<i32>, DatumWithOid::from)
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

#[cfg(test)]
mod tests {
    use super::*;
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
    fn validates_queue_policy_mode() {
        let policy = match validate_policy_definition("role", None, 100, 500, " QUEUE ", 7) {
            Ok(policy) => policy,
            Err(error) => panic!("expected valid queue policy, got {error}"),
        };

        assert_eq!(policy.mode, BudgetMode::Queue);
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
}
