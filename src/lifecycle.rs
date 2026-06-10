use std::cell::RefCell;

use pgrx::pg_sys;

use crate::admission::{self, AdmissionError, AdmissionErrorContext};
use crate::errors::{self, PwbError};
use crate::guc;
use crate::profile;
use crate::reconcile;
use crate::scope;
use crate::types::{ActiveStatementState, AdmissionContext, QueryId, StatementClass};
use crate::utility;

thread_local! {
    static ACTIVE_STATEMENTS: RefCell<Vec<ActiveStatementState>> = const { RefCell::new(Vec::new()) };
    static EXECUTOR_ADMISSION_STACK: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };
    static ADMISSION_BYPASS_DEPTH: RefCell<usize> = const { RefCell::new(0) };
}

struct AdmissionBypassGuard;

pub(crate) fn executor_statement_start(query_desc: *mut pg_sys::QueryDesc) {
    let admitted = if guc::enabled() && !admission_is_bypassed() {
        handle_admission_result(admit_normal_statement(query_desc)).is_some_and(
            |active_statement| {
                push_active_statement(reconcile::capture_start(active_statement));
                true
            },
        )
    } else {
        false
    };
    push_executor_admission_marker(admitted);
}

pub(crate) fn executor_statement_complete() {
    if pop_executor_admission_marker()
        && let Some(active_statement) = pop_active_statement()
    {
        reconcile::reconcile_completed_statement(&active_statement);
    }
}

pub(crate) fn utility_statement_start(
    pstmt: *mut pg_sys::PlannedStmt,
    read_only_tree: bool,
) -> bool {
    if !guc::enabled() || admission_is_bypassed() {
        return false;
    }

    // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility for this
    // invocation; the helper validates node tags before reading extension-specific fields.
    if unsafe { utility::is_pg_wal_budget_create_extension(pstmt) } {
        return false;
    }

    handle_admission_result(admit_utility_statement(pstmt, read_only_tree)).is_some_and(
        |active_statement| {
            push_active_statement(reconcile::capture_start(active_statement));
            true
        },
    )
}

pub(crate) fn utility_statement_complete(admitted: bool) {
    if admitted && let Some(active_statement) = pop_active_statement() {
        reconcile::reconcile_completed_statement(&active_statement);
    }
}

pub(crate) fn transaction_ending(event: pg_sys::XactEvent::Type) {
    if !matches!(
        event,
        pg_sys::XactEvent::XACT_EVENT_COMMIT
            | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
            | pg_sys::XactEvent::XACT_EVENT_ABORT
            | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
    ) {
        return;
    }

    let aborting = matches!(
        event,
        pg_sys::XactEvent::XACT_EVENT_ABORT | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
    );
    for active_statement in drain_active_statements() {
        if aborting {
            let _ = reconcile::record_aborted_statement(&active_statement);
        } else {
            admission::record_missing_actual_wal();
        }
    }
}

fn drain_active_statements() -> Vec<ActiveStatementState> {
    EXECUTOR_ADMISSION_STACK.with(|markers| {
        markers.borrow_mut().clear();
    });
    ACTIVE_STATEMENTS.with(|statements| {
        let mut statements = statements.borrow_mut();
        let mut drained = Vec::with_capacity(statements.len());
        while let Some(statement) = statements.pop() {
            drained.push(statement);
        }
        drained
    })
}

fn admit_utility_statement(
    pstmt: *mut pg_sys::PlannedStmt,
    read_only_tree: bool,
) -> Result<Option<ActiveStatementState>, AdmissionError> {
    let _guard = AdmissionBypassGuard::enter();
    utility::admit_utility_statement(pstmt, read_only_tree)
}

fn admit_normal_statement(
    query_desc: *mut pg_sys::QueryDesc,
) -> Result<Option<ActiveStatementState>, AdmissionError> {
    let _guard = AdmissionBypassGuard::enter();
    // SAFETY: `query_desc` is the pointer PostgreSQL passed to ExecutorStart for this backend.
    let planned_statement = unsafe { planned_statement_ref(query_desc) }?;
    let statement_class = classify_planned_statement(planned_statement);
    if matches!(statement_class, StatementClass::ReadOnly) {
        return Ok(None);
    }

    let query_id = extract_query_id(planned_statement);
    let scope = scope::classify_current_scope().map_err(|error| {
        AdmissionError::internal_with_context(
            error,
            AdmissionErrorContext::from_statement(query_id, statement_class),
        )
    })?;
    let predicted_wal_bytes = profile::predict_context(&profile::PredictionContext {
        statement_class,
        query_id,
        scope_hash: scope.value_hash,
    });
    let context = AdmissionContext {
        query_id,
        scope,
        statement_class,
        predicted_wal_bytes,
    };
    admission::admit_context(&context).map(Some)
}

pub(crate) fn with_admission_bypass<R>(callback: impl FnOnce() -> R) -> R {
    let _guard = AdmissionBypassGuard::enter();
    callback()
}

fn handle_admission_result(
    result: Result<Option<ActiveStatementState>, AdmissionError>,
) -> Option<ActiveStatementState> {
    match result {
        Ok(active_statement) => active_statement,
        Err(AdmissionError::Rejected {
            policy_id,
            predicted_wal_bytes,
            available_wal_bytes,
        }) => errors::raise(PwbError::BudgetExceeded {
            policy_id,
            predicted_wal_bytes,
            available_wal_bytes,
        }),
        Err(AdmissionError::Internal { error, context }) => {
            handle_internal_admission_error(error, context);
            None
        }
    }
}

unsafe fn planned_statement_ref<'a>(
    query_desc: *mut pg_sys::QueryDesc,
) -> Result<&'a pg_sys::PlannedStmt, AdmissionError> {
    if query_desc.is_null() {
        return Err(AdmissionError::internal(PwbError::Internal {
            message: "executor start received a null QueryDesc".to_string(),
        }));
    }

    // SAFETY: The caller passes PostgreSQL's QueryDesc pointer for the current executor hook.
    let planned_statement = unsafe { (*query_desc).plannedstmt };
    if planned_statement.is_null() {
        return Err(AdmissionError::internal(PwbError::Internal {
            message: "executor start received a QueryDesc without a PlannedStmt".to_string(),
        }));
    }

    // SAFETY: The planned statement pointer was read from a live QueryDesc.
    Ok(unsafe { &*planned_statement })
}

const fn classify_planned_statement(planned_statement: &pg_sys::PlannedStmt) -> StatementClass {
    match planned_statement.commandType {
        pg_sys::CmdType::CMD_SELECT if planned_statement.hasModifyingCTE => StatementClass::Write,
        pg_sys::CmdType::CMD_SELECT => StatementClass::ReadOnly,
        pg_sys::CmdType::CMD_INSERT
        | pg_sys::CmdType::CMD_UPDATE
        | pg_sys::CmdType::CMD_DELETE
        | pg_sys::CmdType::CMD_MERGE => StatementClass::Write,
        _ => StatementClass::Unknown,
    }
}

const fn extract_query_id(planned_statement: &pg_sys::PlannedStmt) -> Option<QueryId> {
    let query_id = planned_statement.queryId;
    if query_id == 0 {
        None
    } else {
        Some(query_id as QueryId)
    }
}

fn handle_internal_admission_error(error: PwbError, context: Option<AdmissionErrorContext>) {
    if guc::fail_open() {
        admission::record_internal_fail_open(context);
        return;
    }

    errors::raise::<()>(error);
}

fn push_active_statement(statement: ActiveStatementState) {
    ACTIVE_STATEMENTS.with(|statements| {
        statements.borrow_mut().push(statement);
    });
}

fn pop_active_statement() -> Option<ActiveStatementState> {
    ACTIVE_STATEMENTS.with(|statements| statements.borrow_mut().pop())
}

fn push_executor_admission_marker(admitted: bool) {
    EXECUTOR_ADMISSION_STACK.with(|markers| {
        markers.borrow_mut().push(admitted);
    });
}

fn pop_executor_admission_marker() -> bool {
    EXECUTOR_ADMISSION_STACK.with(|markers| markers.borrow_mut().pop().unwrap_or(false))
}

fn admission_is_bypassed() -> bool {
    ADMISSION_BYPASS_DEPTH.with(|depth| *depth.borrow() > 0)
}

impl AdmissionBypassGuard {
    fn enter() -> Self {
        ADMISSION_BYPASS_DEPTH.with(|depth| {
            let mut depth = depth.borrow_mut();
            *depth = depth.saturating_add(1);
        });
        Self
    }
}

impl Drop for AdmissionBypassGuard {
    fn drop(&mut self) {
        ADMISSION_BYPASS_DEPTH.with(|depth| {
            let mut depth = depth.borrow_mut();
            *depth = depth.saturating_sub(1);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        AdmissionDecision, DecisionKind, ReasonCode, ScopeKind, WalMeasurementKind,
    };

    #[test]
    fn admission_bypass_is_scoped_and_nested() {
        reset_lifecycle_state();
        assert!(!admission_is_bypassed());

        with_admission_bypass(|| {
            assert!(admission_is_bypassed());
            with_admission_bypass(|| assert!(admission_is_bypassed()));
            assert!(admission_is_bypassed());
        });

        assert!(!admission_is_bypassed());
    }

    #[test]
    fn executor_completion_pops_only_admitted_markers() {
        reset_lifecycle_state();
        push_active_statement(test_statement(11));
        push_executor_admission_marker(true);
        push_executor_admission_marker(false);

        executor_statement_complete();
        assert_eq!(active_statement_count(), 1);
        executor_statement_complete();
        assert_eq!(active_statement_count(), 0);
    }

    #[test]
    fn drain_active_statements_clears_markers_and_preserves_lifo_order() {
        reset_lifecycle_state();
        push_active_statement(test_statement(1));
        push_active_statement(test_statement(2));
        push_executor_admission_marker(true);
        push_executor_admission_marker(true);

        let drained = drain_active_statements();

        assert_eq!(
            drained
                .iter()
                .map(|statement| statement.scope_hash)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        assert_eq!(active_statement_count(), 0);
        assert!(!pop_executor_admission_marker());
    }

    fn reset_lifecycle_state() {
        ACTIVE_STATEMENTS.with(|statements| statements.borrow_mut().clear());
        EXECUTOR_ADMISSION_STACK.with(|markers| markers.borrow_mut().clear());
        ADMISSION_BYPASS_DEPTH.with(|depth| *depth.borrow_mut() = 0);
    }

    fn active_statement_count() -> usize {
        ACTIVE_STATEMENTS.with(|statements| statements.borrow().len())
    }

    fn test_statement(scope_hash: u64) -> ActiveStatementState {
        ActiveStatementState {
            decision: AdmissionDecision {
                kind: DecisionKind::NoMatchingPolicy,
                policy_id: None,
                charged_bytes: 0,
                available_before: 0,
                available_after: 0,
                reason_code: ReasonCode::NoMatchingPolicy,
            },
            start_wal_bytes: None,
            measurement_kind: WalMeasurementKind::Unavailable,
            query_id: None,
            scope_kind: ScopeKind::Database,
            scope_hash,
            statement_class: StatementClass::Write,
            predicted_wal_bytes: 0,
        }
    }
}
