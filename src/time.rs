use std::time::{SystemTime, UNIX_EPOCH};

use pgrx::pg_sys;

use crate::types::EpochMillis;

const SLEEP_CHUNK_MS: EpochMillis = 100;

pub(crate) fn current_epoch_ms() -> EpochMillis {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            EpochMillis::try_from(duration.as_millis()).unwrap_or(EpochMillis::MAX)
        })
}

pub(crate) fn sleep_ms_interruptible(wait_ms: EpochMillis) {
    let mut remaining_ms = wait_ms;
    while remaining_ms > 0 {
        process_interrupts();
        let chunk_ms = remaining_ms.min(SLEEP_CHUNK_MS);
        sleep_chunk_ms(chunk_ms);
        remaining_ms -= chunk_ms;
    }
    process_interrupts();
}

fn sleep_chunk_ms(chunk_ms: EpochMillis) {
    let timeout_ms = core::ffi::c_long::try_from(chunk_ms).unwrap_or(core::ffi::c_long::MAX);
    let wake_events = pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_EXIT_ON_PM_DEATH;
    let wake_events = core::ffi::c_int::try_from(wake_events).unwrap_or(core::ffi::c_int::MAX);

    // SAFETY: `MyLatch` is PostgreSQL backend-local state and is valid while this code runs inside
    // a backend hook. `WaitLatch` borrows it for a timeout wait, does not take ownership of Rust
    // memory, and is called only outside extension shared-memory locks. Interrupts are checked
    // around every bounded wait chunk by `process_interrupts`.
    unsafe {
        pg_sys::ResetLatch(pg_sys::MyLatch);
        pg_sys::WaitLatch(
            pg_sys::MyLatch,
            wake_events,
            timeout_ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
    }
}

fn process_interrupts() {
    // SAFETY: PostgreSQL exposes `ProcessInterrupts` for backend code at safe interruption points.
    // Queue admission calls it only outside shared-memory locks and before statement execution.
    unsafe {
        pg_sys::ProcessInterrupts();
    }
}
