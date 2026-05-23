use core::ptr;
use std::slice;

use pgrx::pg_sys;

use crate::errors::{PwbError, PwbResult};
use crate::guc;

mod budget_buckets;
mod counters;
mod layout;
mod profiles;
mod recent_decisions;
mod records;

pub(crate) use budget_buckets::{
    snapshot_budget_buckets, with_budget_bucket, with_existing_budget_bucket,
};
pub(crate) use counters::{add_counters, snapshot_counters};
use layout::{SharedLayout, capacity_to_u32, compute_layout};
pub(crate) use profiles::{
    begin_profile_restore, complete_profile_persist, finish_profile_restore,
    lookup_scoped_or_global_query_profile, mark_profile_restore_failed, reserve_profile_persist,
    reset_profiles, snapshot_profiles_for_persist, snapshot_query_profiles,
    upsert_scoped_and_global_query_profiles,
};
pub(crate) use recent_decisions::{record_recent_decision, snapshot_recent_decisions};
pub(crate) use records::{
    BudgetBucketSnapshot, BudgetBucketState, CounterDelta, PwbCounters, QueryProfileSnapshot,
    RecentDecisionRecord,
};
use records::{PwbBudgetBucket, PwbProfileEntry, PwbRecentDecision, PwbSharedState};

const SHMEM_NAME: &[u8] = b"pg_wal_budget shared state\0";
const LWLOCK_TRANCHE_NAME: &[u8] = b"pg_wal_budget\0";
const MAGIC: u32 = 0x5057_4201;
const LAYOUT_VERSION: u32 = 3;
static mut SHARED_STATE: *mut PwbSharedState = ptr::null_mut();
static mut SHARED_LOCK: *mut pg_sys::LWLock = ptr::null_mut();
static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;
static mut REQUESTED_LAYOUT: SharedLayout = SharedLayout::empty();

pub(crate) fn request_shared_memory() {
    // SAFETY: _PG_init runs during extension preload. Shared memory sizing must happen from
    // PostgreSQL's shmem_request_hook; startup initialization remains in shmem_startup_hook.
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(shmem_request);
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(shmem_startup);
    }
}

unsafe extern "C-unwind" fn shmem_request() {
    // SAFETY: PostgreSQL invokes this hook while addin shared memory requests are still allowed.
    // Chaining the previous hook preserves any hook installed before this extension.
    unsafe {
        if let Some(prev_hook) = PREV_SHMEM_REQUEST_HOOK {
            prev_hook();
        }
    }

    let layout = match compute_layout(
        guc::recent_decision_capacity(),
        guc::profile_cache_capacity(),
    ) {
        Ok(layout) => layout,
        Err(error) => raise_startup_error(&error),
    };

    let size = layout.total_bytes;

    // SAFETY: PostgreSQL invokes shmem_request_hook before shared memory is finalized.
    unsafe {
        REQUESTED_LAYOUT = layout;
        pg_sys::RequestAddinShmemSpace(size);
        pg_sys::RequestNamedLWLockTranche(LWLOCK_TRANCHE_NAME.as_ptr().cast(), 1);
    }
}

pub(crate) fn is_available() -> bool {
    // SAFETY: Reading the process-local pointer is safe for availability checks; callers do not
    // dereference it here.
    unsafe {
        let state = SHARED_STATE;
        !state.is_null()
            && (*state).magic == MAGIC
            && (*state).layout_version == LAYOUT_VERSION
            && !SHARED_LOCK.is_null()
    }
}

pub(crate) fn reset_stats() -> PwbResult<()> {
    with_locked_state(|state, recent_decisions, _profiles| {
        state.counters = PwbCounters::default();
        state.recent_decision_head = 0;
        state.recent_decision_count = 0;
        recent_decisions.fill(PwbRecentDecision::default());
        Ok(())
    })
}

unsafe extern "C-unwind" fn shmem_startup() {
    // SAFETY: PostgreSQL invokes this hook while initializing shared memory. Chaining the previous
    // hook preserves any hook installed before this extension.
    unsafe {
        if let Some(prev_hook) = PREV_SHMEM_STARTUP_HOOK {
            prev_hook();
        }

        let lock = pg_sys::GetNamedLWLockTranche(LWLOCK_TRANCHE_NAME.as_ptr().cast());
        if lock.is_null() {
            raise_startup_error(&PwbError::Internal {
                message: "failed to resolve pg_wal_budget LWLock tranche".to_string(),
            });
        }
        SHARED_LOCK = ptr::addr_of_mut!((*lock).lock);

        let mut found = false;
        let state = pg_sys::ShmemInitStruct(
            SHMEM_NAME.as_ptr().cast(),
            REQUESTED_LAYOUT.total_bytes as pg_sys::Size,
            &raw mut found,
        )
        .cast::<PwbSharedState>();

        if state.is_null() {
            raise_startup_error(&PwbError::Internal {
                message: "failed to initialize pg_wal_budget shared memory".to_string(),
            });
        }

        SHARED_STATE = state;

        if !found {
            ptr::write_bytes(state.cast::<u8>(), 0, REQUESTED_LAYOUT.total_bytes);
            initialize_state(state, REQUESTED_LAYOUT);
        } else if (*state).magic != MAGIC || (*state).layout_version != LAYOUT_VERSION {
            raise_startup_error(&PwbError::Internal {
                message: format!(
                    "pg_wal_budget shared memory layout mismatch: magic={}, version={}",
                    (*state).magic,
                    (*state).layout_version
                ),
            });
        }
    }
}

unsafe fn initialize_state(state: *mut PwbSharedState, layout: SharedLayout) {
    // SAFETY: The caller passes a non-null pointer to the newly allocated shared-memory region.
    unsafe {
        ptr::write(
            state,
            PwbSharedState {
                magic: MAGIC,
                layout_version: LAYOUT_VERSION,
                recent_decision_capacity: capacity_to_u32(layout.recent_decision_capacity),
                profile_cache_capacity: capacity_to_u32(layout.profile_cache_capacity),
                budget_bucket_capacity: capacity_to_u32(layout.budget_bucket_capacity),
                recent_decision_head: 0,
                recent_decision_count: 0,
                profiles_len: 0,
                budget_buckets_len: 0,
                profile_restore_state: profiles::initial_profile_restore_state(),
                _profile_restore_padding: [0; 7],
                profile_restore_started_epoch_ms: 0,
                last_profile_persist_epoch_ms: 0,
                profile_dirty_count: 0,
                counters: PwbCounters::default(),
            },
        );
    }
}

fn with_locked_state<R>(
    callback: impl FnOnce(
        &mut PwbSharedState,
        &mut [PwbRecentDecision],
        &mut [PwbProfileEntry],
    ) -> PwbResult<R>,
) -> PwbResult<R> {
    let _guard = SharedLockGuard::acquire()?;
    let state = shared_state_mut()?;
    let layout = current_layout()?;

    // SAFETY: The startup hook stores a pointer returned by ShmemInitStruct for this exact layout.
    // The extension LWLock is held for the duration of the returned mutable slices.
    let recent_decisions = unsafe {
        slice_from_region_mut::<PwbRecentDecision>(
            ptr::from_mut(state).cast::<u8>(),
            layout.recent_decisions_offset,
            layout.recent_decision_capacity,
        )
    };
    // SAFETY: Same as above; this region begins at the precomputed profile offset.
    let profiles = unsafe {
        slice_from_region_mut::<PwbProfileEntry>(
            ptr::from_mut(state).cast::<u8>(),
            layout.profiles_offset,
            layout.profile_cache_capacity,
        )
    };

    callback(state, recent_decisions, profiles)
}

fn with_locked_bucket_state<R>(
    callback: impl FnOnce(&mut PwbSharedState, &mut [PwbBudgetBucket]) -> PwbResult<R>,
) -> PwbResult<R> {
    let _guard = SharedLockGuard::acquire()?;
    let state = shared_state_mut()?;
    let layout = current_layout()?;

    // SAFETY: The startup hook stores a pointer returned by ShmemInitStruct for this exact layout.
    // The extension LWLock is held for the duration of the returned mutable slice.
    let budget_buckets = unsafe {
        slice_from_region_mut::<PwbBudgetBucket>(
            ptr::from_mut(state).cast::<u8>(),
            layout.budget_buckets_offset,
            layout.budget_bucket_capacity,
        )
    };

    callback(state, budget_buckets)
}

struct SharedLockGuard;

impl SharedLockGuard {
    fn acquire() -> PwbResult<Self> {
        // SAFETY: SHARED_LOCK is initialized by the shared-memory startup hook before runtime APIs
        // can successfully operate. The null check provides a clear error for non-preloaded use.
        unsafe {
            if SHARED_LOCK.is_null() {
                return Err(shared_memory_unavailable());
            }
            pg_sys::LWLockAcquire(SHARED_LOCK, pg_sys::LWLockMode::LW_EXCLUSIVE);
        }
        Ok(Self)
    }
}

impl Drop for SharedLockGuard {
    fn drop(&mut self) {
        // SAFETY: A guard is only constructed after successfully acquiring SHARED_LOCK.
        unsafe {
            pg_sys::LWLockRelease(SHARED_LOCK);
        }
    }
}

fn shared_state_mut() -> PwbResult<&'static mut PwbSharedState> {
    // SAFETY: The pointer is process-local and initialized by shmem_startup. The caller holds the
    // shared lock, so returning a temporary mutable reference is synchronized.
    unsafe {
        let state = SHARED_STATE;
        if state.is_null() {
            return Err(shared_memory_unavailable());
        }
        if (*state).magic != MAGIC || (*state).layout_version != LAYOUT_VERSION {
            return Err(PwbError::Internal {
                message: "pg_wal_budget shared memory has an incompatible layout".to_string(),
            });
        }
        Ok(&mut *state)
    }
}

fn current_layout() -> PwbResult<SharedLayout> {
    // SAFETY: REQUESTED_LAYOUT is written during _PG_init and remains immutable afterward.
    unsafe {
        if REQUESTED_LAYOUT.total_bytes == 0 {
            return Err(shared_memory_unavailable());
        }
        Ok(REQUESTED_LAYOUT)
    }
}

const unsafe fn slice_from_region_mut<T>(
    base: *mut u8,
    offset: usize,
    len: usize,
) -> &'static mut [T] {
    if len == 0 {
        return &mut [];
    }

    // SAFETY: The caller guarantees that `base + offset` points to a region containing `len`
    // properly aligned elements of T inside the extension's shared memory allocation.
    unsafe { slice::from_raw_parts_mut(base.add(offset).cast::<T>(), len) }
}

fn shared_memory_unavailable() -> PwbError {
    PwbError::Internal {
        message:
            "pg_wal_budget shared memory is not initialized; add pg_wal_budget to shared_preload_libraries and restart"
                .to_string(),
    }
}

fn budget_bucket_capacity_exhausted() -> PwbError {
    PwbError::Internal {
        message: "pg_wal_budget budget bucket capacity exhausted".to_string(),
    }
}

fn profile_cache_capacity_exhausted() -> PwbError {
    PwbError::Internal {
        message: "pg_wal_budget profile cache capacity exhausted".to_string(),
    }
}

fn raise_startup_error(error: &PwbError) -> ! {
    pgrx::error!("{}", error);
}
