use std::cell::RefCell;

use pgrx::pg_sys;

use crate::admission::{self, AdmissionError};
use crate::errors::{self, PwbError};
use crate::guc;
use crate::profile;
use crate::reconcile;
use crate::scope;
use crate::types::{ActiveStatementState, AdmissionContext, QueryId, StatementClass};
use crate::utility;

static mut PREV_EXECUTOR_START_HOOK: pg_sys::ExecutorStart_hook_type = None;
static mut PREV_EXECUTOR_END_HOOK: pg_sys::ExecutorEnd_hook_type = None;
static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

thread_local! {
    static ACTIVE_STATEMENTS: RefCell<Vec<ActiveStatementState>> = const { RefCell::new(Vec::new()) };
    static EXECUTOR_ADMISSION_STACK: RefCell<Vec<bool>> = const { RefCell::new(Vec::new()) };
    static ADMISSION_BYPASS_DEPTH: RefCell<usize> = const { RefCell::new(0) };
}

struct AdmissionBypassGuard;

pub(crate) fn install_hooks() {
    // SAFETY: _PG_init runs while PostgreSQL is loading the extension. Hook installation follows
    // PostgreSQL's extension convention: save the existing hook, then publish this extension's hook.
    unsafe {
        PREV_EXECUTOR_START_HOOK = pg_sys::ExecutorStart_hook;
        pg_sys::ExecutorStart_hook = Some(executor_start_hook);

        PREV_EXECUTOR_END_HOOK = pg_sys::ExecutorEnd_hook;
        pg_sys::ExecutorEnd_hook = Some(executor_end_hook);

        PREV_PROCESS_UTILITY_HOOK = pg_sys::ProcessUtility_hook;
        pg_sys::ProcessUtility_hook = Some(process_utility_hook);

        pg_sys::RegisterXactCallback(Some(xact_callback), core::ptr::null_mut());
    }
}

#[pgrx::pg_guard]
#[allow(clippy::collapsible_if)]
unsafe extern "C-unwind" fn executor_start_hook(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: core::ffi::c_int,
) {
    let mut admitted = false;
    if guc::enabled() && !admission_is_bypassed() {
        if let Some(active_statement) = handle_admission_result(admit_normal_statement(query_desc))
        {
            push_active_statement(reconcile::capture_start(active_statement));
            admitted = true;
        }
    }
    push_executor_admission_marker(admitted);

    // SAFETY: PostgreSQL invokes this hook with the same arguments expected by either the previous
    // hook or standard_ExecutorStart. This no-op hook only preserves hook chaining semantics.
    unsafe {
        if let Some(prev_hook) = PREV_EXECUTOR_START_HOOK {
            prev_hook(query_desc, eflags);
        } else {
            pg_sys::standard_ExecutorStart(query_desc, eflags);
        }
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn executor_end_hook(query_desc: *mut pg_sys::QueryDesc) {
    // SAFETY: PostgreSQL invokes this hook with the same argument expected by either the previous
    // hook or standard_ExecutorEnd. This no-op hook only preserves hook chaining semantics.
    unsafe {
        if let Some(prev_hook) = PREV_EXECUTOR_END_HOOK {
            prev_hook(query_desc);
        } else {
            pg_sys::standard_ExecutorEnd(query_desc);
        }
    }

    if pop_executor_admission_marker()
        && let Some(active_statement) = pop_active_statement()
    {
        reconcile::reconcile_completed_statement(&active_statement);
    }
}

#[allow(clippy::too_many_arguments)]
#[pgrx::pg_guard]
unsafe extern "C-unwind" fn process_utility_hook(
    pstmt: *mut pg_sys::PlannedStmt,
    query_string: *const core::ffi::c_char,
    read_only_tree: bool,
    context: pg_sys::ProcessUtilityContext::Type,
    params: pg_sys::ParamListInfo,
    query_env: *mut pg_sys::QueryEnvironment,
    dest: *mut pg_sys::DestReceiver,
    qc: *mut pg_sys::QueryCompletion,
) {
    let utility_admitted = if guc::enabled() && !admission_is_bypassed() {
        // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility for this
        // invocation; the helper validates node tags before reading extension-specific fields.
        let bypass_extension_install = unsafe { utility::is_pg_wal_budget_create_extension(pstmt) };
        if bypass_extension_install {
            false
        } else if let Some(active_statement) =
            handle_admission_result(admit_utility_statement(pstmt, read_only_tree))
        {
            push_active_statement(reconcile::capture_start(active_statement));
            true
        } else {
            false
        }
    } else {
        false
    };

    // SAFETY: PostgreSQL invokes this hook with the same arguments expected by either the previous
    // hook or standard_ProcessUtility. Admission has completed, so the internal bypass guard is no
    // longer active while the user utility statement runs.
    unsafe {
        if let Some(prev_hook) = PREV_PROCESS_UTILITY_HOOK {
            prev_hook(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        } else {
            pg_sys::standard_ProcessUtility(
                pstmt,
                query_string,
                read_only_tree,
                context,
                params,
                query_env,
                dest,
                qc,
            );
        }
    }

    if utility_admitted && let Some(active_statement) = pop_active_statement() {
        reconcile::reconcile_completed_statement(&active_statement);
    }
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut core::ffi::c_void,
) {
    match event {
        pg_sys::XactEvent::XACT_EVENT_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_COMMIT
        | pg_sys::XactEvent::XACT_EVENT_ABORT
        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT => {
            for active_statement in drain_active_statements() {
                if matches!(
                    event,
                    pg_sys::XactEvent::XACT_EVENT_ABORT
                        | pg_sys::XactEvent::XACT_EVENT_PARALLEL_ABORT
                ) {
                    let _ = reconcile::record_aborted_statement(&active_statement);
                } else {
                    admission::record_missing_actual_wal();
                }
            }
        }
        _ => {}
    }
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
    let scope = scope::classify_current_scope().map_err(AdmissionError::Internal)?;
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
        Err(AdmissionError::Internal(error)) => {
            handle_internal_admission_error(error);
            None
        }
    }
}

unsafe fn planned_statement_ref<'a>(
    query_desc: *mut pg_sys::QueryDesc,
) -> Result<&'a pg_sys::PlannedStmt, AdmissionError> {
    if query_desc.is_null() {
        return Err(AdmissionError::Internal(PwbError::Internal {
            message: "executor start received a null QueryDesc".to_string(),
        }));
    }

    // SAFETY: The caller passes PostgreSQL's QueryDesc pointer for the current executor hook.
    let planned_statement = unsafe { (*query_desc).plannedstmt };
    if planned_statement.is_null() {
        return Err(AdmissionError::Internal(PwbError::Internal {
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

fn handle_internal_admission_error(error: PwbError) {
    if guc::fail_open() {
        admission::record_internal_fail_open();
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
