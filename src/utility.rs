#![allow(dead_code)]
#![allow(clippy::redundant_pub_crate)]

use std::ffi::CStr;

use pgrx::pg_sys;

use crate::admission::{self, AdmissionError};
use crate::errors::{PwbError, PwbResult};
use crate::predict;
use crate::scope;
use crate::types::{
    ActiveStatementOrigin, ActiveStatementState, AdmissionContext, QueryId, StatementClass,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UtilityAdmission {
    pub(crate) active_statement: ActiveStatementState,
}

pub(crate) fn admit_utility_statement(
    pstmt: *mut pg_sys::PlannedStmt,
    read_only_tree: bool,
) -> Result<Option<UtilityAdmission>, AdmissionError> {
    // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility.
    let statement_class = unsafe { classify_utility_statement(pstmt, read_only_tree) }
        .map_err(AdmissionError::Internal)?;
    // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility.
    let query_id = unsafe { extract_utility_query_id(pstmt) };
    let scope = scope::classify_current_scope().map_err(AdmissionError::Internal)?;
    let predicted_wal_bytes =
        predict::predict_wal_bytes(statement_class, query_id, scope.value_hash);
    let context = AdmissionContext {
        query_id,
        scope,
        statement_class,
        predicted_wal_bytes,
    };

    admission::admit_context(&context, ActiveStatementOrigin::Utility)
        .map(|active_statement| Some(UtilityAdmission { active_statement }))
}

#[allow(clippy::cast_ptr_alignment)]
pub(crate) unsafe fn is_pg_wal_budget_create_extension(pstmt: *mut pg_sys::PlannedStmt) -> bool {
    // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility.
    let Ok(planned_statement) = (unsafe { planned_statement_ref(pstmt) }) else {
        return false;
    };
    if planned_statement.commandType != pg_sys::CmdType::CMD_UTILITY
        || planned_statement.utilityStmt.is_null()
    {
        return false;
    }

    // SAFETY: utilityStmt is non-null and Node-compatible for utility planned statements.
    let node = unsafe { &*planned_statement.utilityStmt.cast::<pg_sys::Node>() };
    if node.type_ != pg_sys::NodeTag::T_CreateExtensionStmt {
        return false;
    }

    // SAFETY: The node tag confirms this is a CreateExtensionStmt. PostgreSQL allocates parse
    // nodes with alignment suitable for their concrete struct type.
    let create_extension = unsafe {
        &*planned_statement
            .utilityStmt
            .cast::<pg_sys::CreateExtensionStmt>()
    };
    if create_extension.extname.is_null() {
        return false;
    }

    // SAFETY: PostgreSQL stores extension names as null-terminated C strings in the parse node.
    let extension_name = unsafe { CStr::from_ptr(create_extension.extname) };
    extension_name.to_bytes() == b"pg_wal_budget"
}

unsafe fn classify_utility_statement(
    pstmt: *mut pg_sys::PlannedStmt,
    read_only_tree: bool,
) -> PwbResult<StatementClass> {
    // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility.
    let planned_statement = unsafe { planned_statement_ref(pstmt)? };
    if planned_statement.commandType != pg_sys::CmdType::CMD_UTILITY {
        return Ok(StatementClass::Unknown);
    }

    if planned_statement.utilityStmt.is_null() {
        return Ok(StatementClass::Utility);
    }

    // SAFETY: utilityStmt is non-null and PostgreSQL Node-compatible for utility planned statements.
    let node = unsafe { &*planned_statement.utilityStmt.cast::<pg_sys::Node>() };
    Ok(classify_utility_node_tag(
        node.type_,
        planned_statement.utilityStmt,
        read_only_tree,
    ))
}

fn classify_utility_node_tag(
    tag: pg_sys::NodeTag,
    utility_stmt: *mut pg_sys::Node,
    read_only_tree: bool,
) -> StatementClass {
    if tag == pg_sys::NodeTag::T_CopyStmt {
        return classify_copy_statement(utility_stmt);
    }
    if tag == pg_sys::NodeTag::T_ExplainStmt {
        return classify_explain_statement(utility_stmt);
    }
    if is_known_read_only_utility(tag) {
        return StatementClass::ReadOnly;
    }
    if is_known_wal_candidate_utility(tag) {
        return StatementClass::Utility;
    }
    if read_only_tree {
        StatementClass::ReadOnly
    } else {
        StatementClass::Utility
    }
}

const fn is_known_read_only_utility(tag: pg_sys::NodeTag) -> bool {
    matches!(
        tag,
        pg_sys::NodeTag::T_VariableSetStmt
            | pg_sys::NodeTag::T_VariableShowStmt
            | pg_sys::NodeTag::T_TransactionStmt
            | pg_sys::NodeTag::T_DeclareCursorStmt
            | pg_sys::NodeTag::T_ClosePortalStmt
            | pg_sys::NodeTag::T_FetchStmt
            | pg_sys::NodeTag::T_ListenStmt
            | pg_sys::NodeTag::T_UnlistenStmt
            | pg_sys::NodeTag::T_DiscardStmt
            | pg_sys::NodeTag::T_LockStmt
            | pg_sys::NodeTag::T_ConstraintsSetStmt
            | pg_sys::NodeTag::T_PrepareStmt
            | pg_sys::NodeTag::T_DeallocateStmt
    )
}

#[allow(clippy::too_many_lines)]
const fn is_known_wal_candidate_utility(tag: pg_sys::NodeTag) -> bool {
    matches!(
        tag,
        pg_sys::NodeTag::T_CreateSchemaStmt
            | pg_sys::NodeTag::T_AlterTableStmt
            | pg_sys::NodeTag::T_AlterCollationStmt
            | pg_sys::NodeTag::T_AlterDomainStmt
            | pg_sys::NodeTag::T_GrantStmt
            | pg_sys::NodeTag::T_GrantRoleStmt
            | pg_sys::NodeTag::T_AlterDefaultPrivilegesStmt
            | pg_sys::NodeTag::T_CreateStmt
            | pg_sys::NodeTag::T_CreateTableSpaceStmt
            | pg_sys::NodeTag::T_DropTableSpaceStmt
            | pg_sys::NodeTag::T_AlterTableSpaceOptionsStmt
            | pg_sys::NodeTag::T_AlterTableMoveAllStmt
            | pg_sys::NodeTag::T_CreateExtensionStmt
            | pg_sys::NodeTag::T_AlterExtensionStmt
            | pg_sys::NodeTag::T_AlterExtensionContentsStmt
            | pg_sys::NodeTag::T_CreateFdwStmt
            | pg_sys::NodeTag::T_AlterFdwStmt
            | pg_sys::NodeTag::T_CreateForeignServerStmt
            | pg_sys::NodeTag::T_AlterForeignServerStmt
            | pg_sys::NodeTag::T_CreateForeignTableStmt
            | pg_sys::NodeTag::T_AlterUserMappingStmt
            | pg_sys::NodeTag::T_DropUserMappingStmt
            | pg_sys::NodeTag::T_ImportForeignSchemaStmt
            | pg_sys::NodeTag::T_CreatePolicyStmt
            | pg_sys::NodeTag::T_AlterPolicyStmt
            | pg_sys::NodeTag::T_CreateAmStmt
            | pg_sys::NodeTag::T_CreateTrigStmt
            | pg_sys::NodeTag::T_CreateEventTrigStmt
            | pg_sys::NodeTag::T_AlterEventTrigStmt
            | pg_sys::NodeTag::T_CreatePLangStmt
            | pg_sys::NodeTag::T_CreateRoleStmt
            | pg_sys::NodeTag::T_AlterRoleStmt
            | pg_sys::NodeTag::T_AlterRoleSetStmt
            | pg_sys::NodeTag::T_DropRoleStmt
            | pg_sys::NodeTag::T_CreateSeqStmt
            | pg_sys::NodeTag::T_AlterSeqStmt
            | pg_sys::NodeTag::T_DefineStmt
            | pg_sys::NodeTag::T_CreateDomainStmt
            | pg_sys::NodeTag::T_CreateOpClassStmt
            | pg_sys::NodeTag::T_CreateOpFamilyStmt
            | pg_sys::NodeTag::T_AlterOpFamilyStmt
            | pg_sys::NodeTag::T_DropStmt
            | pg_sys::NodeTag::T_TruncateStmt
            | pg_sys::NodeTag::T_CommentStmt
            | pg_sys::NodeTag::T_SecLabelStmt
            | pg_sys::NodeTag::T_IndexStmt
            | pg_sys::NodeTag::T_CreateStatsStmt
            | pg_sys::NodeTag::T_AlterStatsStmt
            | pg_sys::NodeTag::T_CreateFunctionStmt
            | pg_sys::NodeTag::T_AlterFunctionStmt
            | pg_sys::NodeTag::T_DoStmt
            | pg_sys::NodeTag::T_CallStmt
            | pg_sys::NodeTag::T_RenameStmt
            | pg_sys::NodeTag::T_AlterObjectDependsStmt
            | pg_sys::NodeTag::T_AlterObjectSchemaStmt
            | pg_sys::NodeTag::T_AlterOwnerStmt
            | pg_sys::NodeTag::T_AlterOperatorStmt
            | pg_sys::NodeTag::T_AlterTypeStmt
            | pg_sys::NodeTag::T_RuleStmt
            | pg_sys::NodeTag::T_ViewStmt
            | pg_sys::NodeTag::T_LoadStmt
            | pg_sys::NodeTag::T_CreatedbStmt
            | pg_sys::NodeTag::T_AlterDatabaseStmt
            | pg_sys::NodeTag::T_AlterDatabaseRefreshCollStmt
            | pg_sys::NodeTag::T_AlterDatabaseSetStmt
            | pg_sys::NodeTag::T_DropdbStmt
            | pg_sys::NodeTag::T_AlterSystemStmt
            | pg_sys::NodeTag::T_ClusterStmt
            | pg_sys::NodeTag::T_VacuumStmt
            | pg_sys::NodeTag::T_CheckPointStmt
            | pg_sys::NodeTag::T_CreateTableAsStmt
            | pg_sys::NodeTag::T_RefreshMatViewStmt
            | pg_sys::NodeTag::T_ReindexStmt
            | pg_sys::NodeTag::T_CreateConversionStmt
            | pg_sys::NodeTag::T_CreateCastStmt
            | pg_sys::NodeTag::T_CreateTransformStmt
            | pg_sys::NodeTag::T_ExecuteStmt
            | pg_sys::NodeTag::T_DropOwnedStmt
            | pg_sys::NodeTag::T_ReassignOwnedStmt
            | pg_sys::NodeTag::T_AlterTSDictionaryStmt
            | pg_sys::NodeTag::T_AlterTSConfigurationStmt
            | pg_sys::NodeTag::T_CreatePublicationStmt
            | pg_sys::NodeTag::T_AlterPublicationStmt
            | pg_sys::NodeTag::T_CreateSubscriptionStmt
            | pg_sys::NodeTag::T_AlterSubscriptionStmt
            | pg_sys::NodeTag::T_DropSubscriptionStmt
    )
}

#[allow(clippy::cast_ptr_alignment)]
fn classify_copy_statement(utility_stmt: *mut pg_sys::Node) -> StatementClass {
    if utility_stmt.is_null() {
        return StatementClass::Copy;
    }

    // SAFETY: The caller only passes a PostgreSQL-allocated node whose tag is T_CopyStmt, so the
    // layout and alignment are those of CopyStmt even though the erased pointer type is Node.
    let copy_statement = unsafe { &*utility_stmt.cast::<pg_sys::CopyStmt>() };
    if copy_statement.is_from {
        StatementClass::Copy
    } else {
        StatementClass::ReadOnly
    }
}

#[allow(clippy::cast_ptr_alignment)]
fn classify_explain_statement(utility_stmt: *mut pg_sys::Node) -> StatementClass {
    if utility_stmt.is_null() {
        return StatementClass::Utility;
    }

    // SAFETY: The caller only passes a PostgreSQL-allocated node whose tag is T_ExplainStmt, so the
    // layout and alignment are those of ExplainStmt even though the erased pointer type is Node.
    let explain_statement = unsafe { &*utility_stmt.cast::<pg_sys::ExplainStmt>() };
    if explain_has_analyze_option(explain_statement) {
        StatementClass::Utility
    } else {
        StatementClass::ReadOnly
    }
}

fn explain_has_analyze_option(explain_statement: &pg_sys::ExplainStmt) -> bool {
    // SAFETY: `options` is PostgreSQL's List of DefElem nodes for an ExplainStmt. The traversal
    // only reads list cells and copied option names from the current backend memory context.
    unsafe { explain_options_contain_analyze(explain_statement.options) }
}

unsafe fn explain_options_contain_analyze(options: *mut pg_sys::List) -> bool {
    if options.is_null() {
        return false;
    }

    // SAFETY: The caller guarantees `options` is a PostgreSQL List pointer.
    let mut cell = unsafe { pg_sys::list_head(options) };
    while !cell.is_null() {
        // SAFETY: This list contains DefElem pointers in PostgreSQL-owned cells.
        let def_elem = unsafe { (*cell).ptr_value.cast::<pg_sys::DefElem>() };
        if !def_elem.is_null() {
            // SAFETY: Non-null list entries for ExplainStmt options are DefElem nodes.
            let def_elem = unsafe { &*def_elem };
            if def_elem_name_is(def_elem, "analyze") {
                return def_elem_boolean_value(def_elem).unwrap_or(true);
            }
        }

        // SAFETY: `cell` belongs to `options`; lnext returns the next cell or null.
        cell = unsafe { pg_sys::lnext(options, cell) };
    }

    false
}

fn def_elem_name_is(def_elem: &pg_sys::DefElem, expected: &str) -> bool {
    if def_elem.defname.is_null() {
        return false;
    }

    // SAFETY: PostgreSQL stores DefElem names as null-terminated C strings.
    unsafe { CStr::from_ptr(def_elem.defname) }.to_bytes() == expected.as_bytes()
}

fn def_elem_boolean_value(def_elem: &pg_sys::DefElem) -> Option<bool> {
    if def_elem.arg.is_null() {
        return None;
    }

    // SAFETY: DefElem arg is a PostgreSQL Node when non-null.
    let node = unsafe { &*def_elem.arg.cast::<pg_sys::Node>() };
    if node.type_ != pg_sys::NodeTag::T_Boolean {
        return None;
    }

    // SAFETY: The node tag was checked as T_Boolean, so the layout is Boolean.
    Some(unsafe { (*def_elem.arg.cast::<pg_sys::Boolean>()).boolval })
}

unsafe fn extract_utility_query_id(pstmt: *mut pg_sys::PlannedStmt) -> Option<QueryId> {
    // SAFETY: `pstmt` is the PlannedStmt pointer PostgreSQL passed to ProcessUtility.
    let planned_statement = unsafe { planned_statement_ref(pstmt).ok()? };
    if planned_statement.queryId == 0 {
        None
    } else {
        Some(planned_statement.queryId as QueryId)
    }
}

unsafe fn planned_statement_ref<'a>(
    pstmt: *mut pg_sys::PlannedStmt,
) -> PwbResult<&'a pg_sys::PlannedStmt> {
    if pstmt.is_null() {
        return Err(PwbError::Internal {
            message: "process utility received a null PlannedStmt".to_string(),
        });
    }

    // SAFETY: The caller guarantees `pstmt` is PostgreSQL's live PlannedStmt pointer.
    Ok(unsafe { &*pstmt })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_read_only_utility_tags() {
        assert_eq!(
            classify_utility_node_tag(
                pg_sys::NodeTag::T_VariableShowStmt,
                core::ptr::null_mut(),
                false
            ),
            StatementClass::ReadOnly
        );
    }

    #[test]
    fn classifies_wal_candidate_utility_tags() {
        assert_eq!(
            classify_utility_node_tag(pg_sys::NodeTag::T_IndexStmt, core::ptr::null_mut(), true),
            StatementClass::Utility
        );
    }

    #[test]
    fn classifies_call_and_execute_as_conservative_utility() {
        assert_eq!(
            classify_utility_node_tag(pg_sys::NodeTag::T_CallStmt, core::ptr::null_mut(), true),
            StatementClass::Utility
        );
        assert_eq!(
            classify_utility_node_tag(pg_sys::NodeTag::T_ExecuteStmt, core::ptr::null_mut(), true),
            StatementClass::Utility
        );
    }

    #[test]
    fn classifies_explain_without_statement_as_conservative_utility() {
        assert_eq!(
            classify_utility_node_tag(pg_sys::NodeTag::T_ExplainStmt, core::ptr::null_mut(), false),
            StatementClass::Utility
        );
    }

    #[test]
    fn explain_analyze_defaults_to_false_without_options() {
        let explain = pg_sys::ExplainStmt {
            type_: pg_sys::NodeTag::T_ExplainStmt,
            query: core::ptr::null_mut(),
            options: core::ptr::null_mut(),
        };

        assert!(!explain_has_analyze_option(&explain));
    }

    #[test]
    fn unknown_utility_respects_read_only_tree() {
        assert_eq!(
            classify_utility_node_tag(pg_sys::NodeTag::T_Invalid, core::ptr::null_mut(), true),
            StatementClass::ReadOnly
        );
        assert_eq!(
            classify_utility_node_tag(pg_sys::NodeTag::T_Invalid, core::ptr::null_mut(), false),
            StatementClass::Utility
        );
    }
}
