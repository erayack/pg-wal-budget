#![allow(clippy::redundant_pub_crate)]

use pgrx::pg_sys;

use crate::admission;
use crate::budget;
use crate::errors::PwbResult;
use crate::profile;
use crate::shmem::{self, CounterDelta, RecentDecisionRecord};
use crate::types::{ActiveStatementState, DecisionKind, WalBytes, WalMeasurementKind};

pub(crate) fn capture_start(mut statement: ActiveStatementState) -> ActiveStatementState {
    if let Some(lsn) = current_wal_insert_lsn() {
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
    let Some(end_wal_bytes) = current_wal_insert_lsn() else {
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

    if matches!(statement.measurement_kind, WalMeasurementKind::ExactBackend)
        && let Some(query_id) = statement.query_id
    {
        record_reconciliation_result(&profile::record_observation(
            statement.scope_hash,
            Some(query_id),
            actual_wal_bytes,
            now_epoch_ms,
        ));
    }

    record_reconciliation_result(&reconcile_budget(statement, actual_wal_bytes, debt_bytes));
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

fn reconcile_budget(
    statement: &ActiveStatementState,
    actual_wal_bytes: WalBytes,
    debt_bytes: WalBytes,
) -> PwbResult<()> {
    if !should_reconcile_budget(statement) {
        return Ok(());
    }

    let Some(policy_id) = statement.decision.policy_id else {
        return Ok(());
    };

    if actual_wal_bytes < statement.decision.charged_bytes {
        budget::refund_charged_bytes(
            policy_id,
            statement.scope_hash,
            statement.decision.charged_bytes - actual_wal_bytes,
        )?;
    } else if debt_bytes > 0 {
        budget::record_underprediction_debt(policy_id, statement.scope_hash, debt_bytes)?;
    }

    Ok(())
}

const fn should_reconcile_budget(statement: &ActiveStatementState) -> bool {
    // `ExactBackend` is reserved for a future per-backend WAL counter. The pg17 insert-LSN
    // fallback is cluster-wide and must never adjust enforcement buckets.
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
