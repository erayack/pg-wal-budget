use std::time::{SystemTime, UNIX_EPOCH};

use pgrx::pg_sys;

use crate::types::EpochMillis;

const SLEEP_CHUNK_MS: EpochMillis = 100;
const MICROS_PER_MILLI: u128 = 1000;

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
    let micros = u128::from(chunk_ms) * MICROS_PER_MILLI;
    let micros = core::ffi::c_long::try_from(micros).unwrap_or(core::ffi::c_long::MAX);

    // SAFETY: `pg_usleep` is PostgreSQL's backend sleep primitive. It does not take ownership of
    // memory and is safe to call outside extension critical sections; interrupts are checked around
    // each bounded sleep chunk by `process_interrupts`.
    unsafe {
        pg_sys::pg_usleep(micros);
    }
}

fn process_interrupts() {
    // SAFETY: PostgreSQL exposes `ProcessInterrupts` for backend code at safe interruption points.
    // Queue admission calls it only outside shared-memory locks and before statement execution.
    unsafe {
        pg_sys::ProcessInterrupts();
    }
}
