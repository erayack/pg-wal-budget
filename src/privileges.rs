use std::ffi::CString;

use pgrx::pg_sys;

use crate::errors::{PwbError, PwbResult};

pub(crate) const ADMIN_ROLE: &str = "pwb_admin";
pub(crate) const TENANT_SETTER_ROLE: &str = "pwb_tenant_setter";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PrivilegeGate {
    Admin,
    TenantSetter,
}

impl PrivilegeGate {
    const fn role_names(self) -> &'static [&'static str] {
        match self {
            Self::Admin => &[ADMIN_ROLE],
            Self::TenantSetter => &[ADMIN_ROLE, TENANT_SETTER_ROLE],
        }
    }
}

pub(crate) fn require(gate: PrivilegeGate, operation: &'static str) -> PwbResult<()> {
    if invoking_user_is_superuser_or_member_of(gate.role_names())? {
        Ok(())
    } else {
        Err(PwbError::InsufficientPrivilege { operation })
    }
}

fn invoking_user_is_superuser_or_member_of(role_names: &[&str]) -> PwbResult<bool> {
    // SAFETY: PostgreSQL exposes the invoking role through GetOuterUserId, which remains the
    // authorization subject even while SECURITY DEFINER functions run as their owner. The role
    // membership helpers only read backend-local catalog/cache state.
    unsafe {
        let invoking_user = pg_sys::GetOuterUserId();
        if pg_sys::superuser_arg(invoking_user) {
            return Ok(true);
        }

        for role_name in role_names {
            let role_name = CString::new(*role_name).map_err(|error| PwbError::Internal {
                message: format!("trusted role name is invalid: {error}"),
            })?;
            let role_oid = pg_sys::get_role_oid(role_name.as_ptr(), true);
            if role_oid != pg_sys::Oid::INVALID
                && pg_sys::has_privs_of_role(invoking_user, role_oid)
            {
                return Ok(true);
            }
        }
    }

    Ok(false)
}
