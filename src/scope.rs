use std::cell::RefCell;
use std::ffi::CStr;

use pgrx::pg_sys;
use pgrx::prelude::*;

use crate::errors::{self, PwbError, PwbResult};
use crate::privileges::{self, PrivilegeGate};
use crate::types::{ScopeHash, ScopeKey, ScopeKind};

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

thread_local! {
    static BACKEND_SCOPE_STATE: RefCell<BackendScopeState> = RefCell::new(BackendScopeState::default());
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BackendScopeState {
    pub(crate) tenant: Option<String>,
    pub(crate) last_scope_hash: Option<ScopeHash>,
}

#[pg_extern]
fn pwb_set_tenant(tenant: &str) {
    privileges::require(PrivilegeGate::TenantSetter, "set trusted tenant scope")
        .unwrap_or_else(errors::raise);
    set_tenant_impl(tenant).unwrap_or_else(errors::raise);
}

#[pg_extern]
fn pwb_clear_tenant() {
    privileges::require(PrivilegeGate::TenantSetter, "clear trusted tenant scope")
        .unwrap_or_else(errors::raise);
    clear_tenant_impl();
}

pub(crate) fn classify_current_scope() -> PwbResult<ScopeKey> {
    if let Some(tenant) = current_tenant() {
        return Ok(scope_key_with_debug(ScopeKind::Tenant, tenant));
    }

    if let Some(role) = current_role_name() {
        return Ok(scope_key_with_debug(ScopeKind::Role, role));
    }

    if let Some(database) = current_database_name() {
        return Ok(scope_key_with_debug(ScopeKind::Database, database));
    }

    if let Some(application) = current_application_name() {
        return Ok(scope_key_with_debug(ScopeKind::Application, application));
    }

    Err(PwbError::MissingScope)
}

pub(crate) fn current_tenant() -> Option<String> {
    BACKEND_SCOPE_STATE.with(|state| state.borrow().tenant.clone())
}

fn set_tenant_impl(tenant: &str) -> PwbResult<()> {
    let tenant = normalize_tenant_value(tenant)?;
    let scope_hash = hash_scope_value(ScopeKind::Tenant, &tenant);

    BACKEND_SCOPE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.tenant = Some(tenant);
        state.last_scope_hash = Some(scope_hash);
    });

    Ok(())
}

fn clear_tenant_impl() {
    BACKEND_SCOPE_STATE.with(|state| {
        let mut state = state.borrow_mut();
        state.tenant = None;
        state.last_scope_hash = None;
    });
}

fn normalize_tenant_value(tenant: &str) -> PwbResult<String> {
    let normalized = tenant.trim();
    if normalized.is_empty() {
        return Err(PwbError::InvalidPolicyValue {
            field: "tenant",
            value: "<empty>".to_string(),
            reason: "must not be empty",
        });
    }

    Ok(normalized.to_string())
}

fn scope_key_with_debug(kind: ScopeKind, value: String) -> ScopeKey {
    let scope_hash = hash_scope_value(kind, &value);

    BACKEND_SCOPE_STATE.with(|state| {
        state.borrow_mut().last_scope_hash = Some(scope_hash);
    });

    ScopeKey::with_debug_value(kind, scope_hash, value)
}

fn current_role_name() -> Option<String> {
    // SAFETY: GetUserId returns the current backend role OID, and GetUserNameFromId returns either
    // a palloc'd null-terminated string or null when no name is available with noerr=true.
    unsafe {
        let role_name = pg_sys::GetUserNameFromId(pg_sys::GetUserId(), true);
        postgres_owned_cstring_to_string(role_name)
    }
}

fn current_database_name() -> Option<String> {
    // SAFETY: MyDatabaseId is the current backend database OID. get_database_name returns either a
    // palloc'd null-terminated string or null when no database name is available.
    unsafe {
        let database_name = pg_sys::get_database_name(pg_sys::MyDatabaseId);
        postgres_owned_cstring_to_string(database_name)
    }
}

fn current_application_name() -> Option<String> {
    // SAFETY: application_name points at PostgreSQL-owned backend configuration storage when set.
    // The value is copied immediately and is not retained as a borrowed pointer.
    unsafe { borrowed_cstring_to_nonempty_string(pg_sys::application_name.cast_const()) }
}

unsafe fn postgres_owned_cstring_to_string(ptr: *mut core::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }

    // SAFETY: The caller guarantees `ptr` is a valid PostgreSQL-owned C string. The value is copied
    // before freeing the PostgreSQL allocation.
    let value = unsafe { borrowed_cstring_to_nonempty_string(ptr.cast_const()) };

    // SAFETY: GetUserNameFromId and get_database_name return palloc'd strings for the caller.
    unsafe {
        pg_sys::pfree(ptr.cast());
    }

    value
}

unsafe fn borrowed_cstring_to_nonempty_string(ptr: *const core::ffi::c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }

    // SAFETY: The caller guarantees `ptr` is a valid null-terminated C string for this backend.
    let value = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .trim()
        .to_string();

    if value.is_empty() { None } else { Some(value) }
}

fn hash_scope_value(kind: ScopeKind, value: &str) -> ScopeHash {
    let mut hash = FNV_OFFSET_BASIS;
    hash = fnv1a_update(hash, &[scope_kind_hash_tag(kind)]);
    hash = fnv1a_update(hash, b":");
    fnv1a_update(hash, value.as_bytes())
}

const fn scope_kind_hash_tag(kind: ScopeKind) -> u8 {
    match kind {
        ScopeKind::Database => b'd',
        ScopeKind::Role => b'r',
        ScopeKind::Application => b'a',
        ScopeKind::Tenant => b't',
        ScopeKind::Composite => b'c',
    }
}

fn fnv1a_update(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalizes_tenant_values() {
        assert_eq!(
            normalize_tenant_value(" tenant-a ").unwrap_or_else(|error| panic!("{error}")),
            "tenant-a"
        );
    }

    #[test]
    fn rejects_empty_tenant_values() {
        assert!(normalize_tenant_value("   ").is_err());
    }

    #[test]
    fn scope_hash_includes_kind() {
        assert_ne!(
            hash_scope_value(ScopeKind::Tenant, "same-value"),
            hash_scope_value(ScopeKind::Role, "same-value")
        );
    }

    #[test]
    fn scope_hash_is_stable_for_same_kind_and_value() {
        assert_eq!(
            hash_scope_value(ScopeKind::Tenant, "tenant-a"),
            hash_scope_value(ScopeKind::Tenant, "tenant-a")
        );
    }

    #[test]
    fn clearing_tenant_state_clears_backend_scope_state() {
        BACKEND_SCOPE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.tenant = Some("tenant-a".to_string());
            state.last_scope_hash = Some(42);
        });

        BACKEND_SCOPE_STATE.with(|state| {
            let mut state = state.borrow_mut();
            state.tenant = None;
            state.last_scope_hash = None;
        });

        assert_eq!(current_tenant(), None);
        BACKEND_SCOPE_STATE.with(|state| {
            assert_eq!(state.borrow().last_scope_hash, None);
        });
    }
}
