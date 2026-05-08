use std::sync::atomic::{AtomicBool, Ordering};

use pgrx::{pg_sys, prelude::*};

pub(crate) mod admission;
pub(crate) mod budget;
pub(crate) mod errors;
pub(crate) mod guc;
pub(crate) mod hooks;
pub(crate) mod policy;
pub(crate) mod predict;
pub(crate) mod profile;
pub(crate) mod reconcile;
pub(crate) mod scope;
pub(crate) mod shmem;
pub(crate) mod stats;
pub(crate) mod types;
pub(crate) mod utility;

pgrx::pg_module_magic!();
pgrx::extension_sql_file!(
    "../sql/pgrx_bootstrap.sql",
    name = "pgrx_bootstrap",
    bootstrap
);

const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");
static PG_INIT_DONE: AtomicBool = AtomicBool::new(false);

#[pg_extern(immutable, parallel_safe)]
#[allow(clippy::missing_const_for_fn)]
fn pg_wal_budget_version() -> &'static str {
    EXTENSION_VERSION
}

#[pg_extern(stable, parallel_safe)]
#[allow(clippy::missing_const_for_fn)]
fn pg_wal_budget_preload_status() -> &'static str {
    if shmem::is_available() {
        "preloaded"
    } else {
        "sql_only_loaded"
    }
}

#[pg_guard]
#[allow(non_snake_case)]
#[allow(clippy::missing_const_for_fn)]
pub extern "C-unwind" fn _PG_init() {
    if PG_INIT_DONE.swap(true, Ordering::AcqRel) {
        return;
    }

    // SAFETY: These PostgreSQL globals indicate whether the library is being loaded from
    // shared_preload_libraries. Hook, GUC, and shared-memory setup must only happen in that path.
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        return;
    }

    guc::register_gucs();
    shmem::request_shared_memory();
    hooks::install_hooks();
}
