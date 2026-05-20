use std::ffi::CString;

use pgrx::pg_sys;

use crate::errors::{PwbError, PwbResult};

pub(crate) const ADMIN_ROLE: &str = "pwb_admin";
pub(crate) const TENANT_SETTER_ROLE: &str = "pwb_tenant_setter";

pub(crate) fn current_user_is_superuser_or_member_of(role_names: &[&str]) -> PwbResult<bool> {
    // SAFETY: PostgreSQL exposes current-user and role-membership state through backend globals
    // and syscache helpers. This function only reads those values in the current backend.
    unsafe {
        if pg_sys::superuser() {
            return Ok(true);
        }

        let current_user = pg_sys::GetUserId();
        for role_name in role_names {
            let role_name = CString::new(*role_name).map_err(|error| PwbError::Internal {
                message: format!("trusted role name is invalid: {error}"),
            })?;
            let role_oid = pg_sys::get_role_oid(role_name.as_ptr(), true);
            if role_oid != pg_sys::Oid::INVALID && pg_sys::has_privs_of_role(current_user, role_oid)
            {
                return Ok(true);
            }
        }
    }

    Ok(false)
}
