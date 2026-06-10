use pgrx::pg_sys;

use crate::lifecycle;

static mut PREV_EXECUTOR_START_HOOK: pg_sys::ExecutorStart_hook_type = None;
static mut PREV_EXECUTOR_END_HOOK: pg_sys::ExecutorEnd_hook_type = None;
static mut PREV_PROCESS_UTILITY_HOOK: pg_sys::ProcessUtility_hook_type = None;

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
unsafe extern "C-unwind" fn executor_start_hook(
    query_desc: *mut pg_sys::QueryDesc,
    eflags: core::ffi::c_int,
) {
    lifecycle::executor_statement_start(query_desc);

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

    lifecycle::executor_statement_complete();
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
    let utility_admitted = lifecycle::utility_statement_start(pstmt, read_only_tree);

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

    lifecycle::utility_statement_complete(utility_admitted);
}

#[pgrx::pg_guard]
unsafe extern "C-unwind" fn xact_callback(
    event: pg_sys::XactEvent::Type,
    _arg: *mut core::ffi::c_void,
) {
    lifecycle::transaction_ending(event);
}

pub(crate) fn with_admission_bypass<R>(callback: impl FnOnce() -> R) -> R {
    lifecycle::with_admission_bypass(callback)
}
