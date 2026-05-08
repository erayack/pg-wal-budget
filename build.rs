use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=sql/pg_wal_budget--0.1.0.sql");
    println!("cargo:rerun-if-changed=scripts/generate_pgrx_bootstrap.sh");

    let script = Path::new("scripts/generate_pgrx_bootstrap.sh");
    let status = match Command::new("bash").arg(script).status() {
        Ok(status) => status,
        Err(error) => panic!("failed to run pgrx bootstrap SQL generator: {error}"),
    };

    assert!(status.success(), "pgrx bootstrap SQL generator failed");
}
