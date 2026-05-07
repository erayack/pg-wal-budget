use pgrx::prelude::*;

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
pub(crate) mod types;
pub(crate) mod utility;

pgrx::pg_module_magic!();

const EXTENSION_VERSION: &str = env!("CARGO_PKG_VERSION");

#[pg_extern(immutable, parallel_safe)]
#[allow(clippy::missing_const_for_fn)]
fn pg_wal_budget_version() -> &'static str {
    EXTENSION_VERSION
}

#[pg_extern(stable, parallel_safe)]
#[allow(clippy::missing_const_for_fn)]
fn pg_wal_budget_preload_status() -> &'static str {
    "loaded"
}

#[pg_guard]
#[allow(non_snake_case)]
#[allow(clippy::missing_const_for_fn)]
pub extern "C-unwind" fn _PG_init() {
    guc::register_gucs();
    shmem::request_shared_memory();
    hooks::install_hooks();
}
