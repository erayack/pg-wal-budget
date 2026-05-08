#![allow(clippy::redundant_pub_crate)]

use pgrx::pg_sys;

use crate::admission;
use crate::budget;
use crate::errors::PwbResult;
use crate::profile;
use crate::shmem::{self, CounterDelta, RecentDecisionRecord};
use crate::types::{ActiveStatementState, DecisionKind, WalBytes, WalMeasurementKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BudgetReconciliation {
    None,
    Refund(WalBytes),
    Debt(WalBytes),
}

pub(crate) fn capture_start(mut statement: ActiveStatementState) -> ActiveStatementState {
    if let Some(wal_bytes) = current_backend_wal_bytes() {
        statement.start_wal_bytes = Some(wal_bytes);
        statement.measurement_kind = WalMeasurementKind::ExactBackend;
    } else if let Some(lsn) = current_wal_insert_lsn() {
        statement.start_wal_bytes = Some(lsn);
        statement.measurement_kind = WalMeasurementKind::ApproximateInsertLsn;
    } else {
        statement.start_wal_bytes = None;
        statement.measurement_kind = WalMeasurementKind::Unavailable;
    }

    statement
}

pub(crate) fn reconcile_completed_statement(statement: &ActiveStatementState) {
    let Some(start_wal_bytes) = statement.start_wal_bytes else {
        admission::record_missing_actual_wal();
        return;
    };
    let Some(end_wal_bytes) = current_measurement_for_kind(statement.measurement_kind) else {
        admission::record_missing_actual_wal();
        return;
    };

    let actual_wal_bytes = wal_delta(start_wal_bytes, end_wal_bytes);
    let now_epoch_ms = admission::current_epoch_ms();
    let debt_bytes = exact_reconciliation_debt_bytes(statement, actual_wal_bytes);

    record_reconciliation_result(&shmem::add_counters(CounterDelta {
        actual_wal_bytes,
        absolute_prediction_error: prediction_error(
            statement.predicted_wal_bytes,
            actual_wal_bytes,
        ),
        scope_debt_bytes: debt_bytes,
        ..CounterDelta::default()
    }));

    record_reconciliation_result(&shmem::record_recent_decision(RecentDecisionRecord {
        timestamp_epoch_ms: now_epoch_ms,
        decision_kind: statement.decision.kind,
        policy_id: statement.decision.policy_id,
        scope_kind: statement.scope_kind,
        scope_hash: statement.scope_hash,
        query_id: statement.query_id,
        statement_class: statement.statement_class,
        predicted_wal_bytes: statement.predicted_wal_bytes,
        actual_wal_bytes: Some(actual_wal_bytes),
        available_before: statement.available_before,
        available_after: statement.available_after,
        reason_code: statement.decision.reason_code,
    }));

    if should_record_profile_observation(statement) {
        record_reconciliation_result(&profile::record_observation(
            statement.scope_hash,
            statement.query_id,
            actual_wal_bytes,
            now_epoch_ms,
        ));
    }

    record_reconciliation_result(&reconcile_budget(statement, actual_wal_bytes));
}

pub(crate) fn record_aborted_statement(statement: &ActiveStatementState) -> PwbResult<()> {
    if statement.decision.charged_bytes > 0 {
        shmem::add_counters(CounterDelta {
            aborted_after_charge_count: 1,
            ..CounterDelta::default()
        })?;
    }

    Ok(())
}

fn reconcile_budget(statement: &ActiveStatementState, actual_wal_bytes: WalBytes) -> PwbResult<()> {
    if !should_reconcile_budget(statement) {
        return Ok(());
    }

    let Some(policy_id) = statement.decision.policy_id else {
        return Ok(());
    };

    match budget_reconciliation(statement, actual_wal_bytes) {
        BudgetReconciliation::None => {}
        BudgetReconciliation::Refund(refund_bytes) => {
            budget::refund_charged_bytes(policy_id, statement.scope_hash, refund_bytes)?;
        }
        BudgetReconciliation::Debt(debt_bytes) => {
            budget::record_underprediction_debt(policy_id, statement.scope_hash, debt_bytes)?;
        }
    }

    Ok(())
}

const fn should_record_profile_observation(statement: &ActiveStatementState) -> bool {
    matches!(statement.measurement_kind, WalMeasurementKind::ExactBackend)
        && statement.query_id.is_some()
}

const fn should_reconcile_budget(statement: &ActiveStatementState) -> bool {
    // The insert-LSN fallback is cluster-wide and must never adjust enforcement buckets.
    matches!(statement.decision.kind, DecisionKind::Allowed)
        && statement.decision.charged_bytes > 0
        && matches!(statement.measurement_kind, WalMeasurementKind::ExactBackend)
}

const fn wal_delta(start: WalBytes, end: WalBytes) -> WalBytes {
    end.saturating_sub(start)
}

const fn prediction_error(predicted: WalBytes, actual: WalBytes) -> WalBytes {
    predicted.abs_diff(actual)
}

const fn exact_reconciliation_debt_bytes(
    statement: &ActiveStatementState,
    actual_wal_bytes: WalBytes,
) -> WalBytes {
    if should_reconcile_budget(statement) && actual_wal_bytes > statement.decision.charged_bytes {
        actual_wal_bytes - statement.decision.charged_bytes
    } else {
        0
    }
}

const fn budget_reconciliation(
    statement: &ActiveStatementState,
    actual_wal_bytes: WalBytes,
) -> BudgetReconciliation {
    if !should_reconcile_budget(statement) {
        return BudgetReconciliation::None;
    }

    if actual_wal_bytes < statement.decision.charged_bytes {
        BudgetReconciliation::Refund(statement.decision.charged_bytes - actual_wal_bytes)
    } else if actual_wal_bytes > statement.decision.charged_bytes {
        BudgetReconciliation::Debt(actual_wal_bytes - statement.decision.charged_bytes)
    } else {
        BudgetReconciliation::None
    }
}

fn current_wal_insert_lsn() -> Option<WalBytes> {
    // SAFETY: These PostgreSQL WAL accessors are read-only and are called from a live backend
    // during hook execution. XLogInsertAllowed guards contexts where insert WAL state is not usable.
    unsafe {
        if !pg_sys::XLogInsertAllowed() {
            return None;
        }

        Some(pg_sys::GetXLogInsertRecPtr() as WalBytes)
    }
}

fn current_measurement_for_kind(kind: WalMeasurementKind) -> Option<WalBytes> {
    match kind {
        WalMeasurementKind::ExactBackend => current_backend_wal_bytes(),
        WalMeasurementKind::ApproximateInsertLsn => current_wal_insert_lsn(),
        WalMeasurementKind::Unavailable => None,
    }
}

#[cfg(feature = "pg17")]
fn current_backend_wal_bytes() -> Option<WalBytes> {
    // SAFETY: `pgWalUsage` is PostgreSQL's backend-local WAL usage accumulator for the current
    // process. Reading its `wal_bytes` field is a non-mutating snapshot taken inside a live
    // backend hook. It is exact for this backend and does not include concurrent backends.
    Some(unsafe { pg_sys::pgWalUsage.wal_bytes as WalBytes })
}

#[cfg(not(feature = "pg17"))]
const fn current_backend_wal_bytes() -> Option<WalBytes> {
    // Keep the optional return type so unsupported targets can fall back to insert-LSN telemetry.
    None
}

fn record_reconciliation_result(result: &PwbResult<()>) {
    if result.is_err() {
        let _ = shmem::add_counters(CounterDelta {
            internal_fail_open_count: 1,
            ..CounterDelta::default()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        ActiveStatementOrigin, AdmissionDecision, ReasonCode, ScopeKind, StatementClass,
    };

    #[test]
    fn computes_saturating_wal_delta() {
        assert_eq!(wal_delta(10, 25), 15);
        assert_eq!(wal_delta(25, 10), 0);
    }

    #[test]
    fn computes_absolute_prediction_error() {
        assert_eq!(prediction_error(100, 125), 25);
        assert_eq!(prediction_error(125, 100), 25);
    }

    #[test]
    fn only_reconciles_budget_for_charged_allowed_statements() {
        let mut statement = test_statement(AdmissionDecision::allowed(
            Some(7),
            100,
            ReasonCode::BudgetAvailable,
        ));
        assert!(should_reconcile_budget(&statement));

        statement.decision = AdmissionDecision::would_reject(7, 100);
        assert!(!should_reconcile_budget(&statement));

        statement.decision = AdmissionDecision::allowed(Some(7), 0, ReasonCode::ObserveMode);
        assert!(!should_reconcile_budget(&statement));

        statement.decision = AdmissionDecision::allowed(Some(7), 100, ReasonCode::BudgetAvailable);
        statement.measurement_kind = WalMeasurementKind::ApproximateInsertLsn;
        assert!(!should_reconcile_budget(&statement));
    }

    #[test]
    fn profile_observations_require_exact_measurement_and_query_id() {
        let mut statement = test_statement(AdmissionDecision::allowed(
            Some(7),
            100,
            ReasonCode::BudgetAvailable,
        ));
        assert!(should_record_profile_observation(&statement));

        statement.measurement_kind = WalMeasurementKind::ApproximateInsertLsn;
        assert!(!should_record_profile_observation(&statement));

        statement.measurement_kind = WalMeasurementKind::ExactBackend;
        statement.query_id = None;
        assert!(!should_record_profile_observation(&statement));
    }

    #[test]
    fn budget_reconciliation_refunds_or_records_debt_only_for_exact_charges() {
        let mut statement = test_statement(AdmissionDecision::allowed(
            Some(7),
            100,
            ReasonCode::BudgetAvailable,
        ));

        assert_eq!(
            budget_reconciliation(&statement, 60),
            BudgetReconciliation::Refund(40)
        );
        assert_eq!(
            budget_reconciliation(&statement, 140),
            BudgetReconciliation::Debt(40)
        );
        assert_eq!(
            budget_reconciliation(&statement, 100),
            BudgetReconciliation::None
        );

        statement.measurement_kind = WalMeasurementKind::ApproximateInsertLsn;
        assert_eq!(
            budget_reconciliation(&statement, 140),
            BudgetReconciliation::None
        );

        statement.measurement_kind = WalMeasurementKind::ExactBackend;
        statement.decision = AdmissionDecision::allowed(Some(7), 0, ReasonCode::ObserveMode);
        assert_eq!(
            budget_reconciliation(&statement, 140),
            BudgetReconciliation::None
        );
    }

    fn test_statement(decision: AdmissionDecision) -> ActiveStatementState {
        ActiveStatementState {
            origin: ActiveStatementOrigin::Executor,
            decision,
            start_wal_bytes: Some(10),
            measurement_kind: WalMeasurementKind::ExactBackend,
            query_id: Some(42),
            scope_kind: ScopeKind::Tenant,
            scope_hash: 99,
            statement_class: StatementClass::Write,
            predicted_wal_bytes: 100,
            available_before: 200,
            available_after: 100,
        }
    }
}
