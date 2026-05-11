#![allow(dead_code)]
#![allow(clippy::redundant_pub_crate)]

use core::mem::{align_of, size_of};
use core::ptr;
use std::slice;

use pgrx::pg_sys;

use crate::errors::{PwbError, PwbResult};
use crate::guc;
use crate::types::{
    DecisionKind, EpochMillis, PolicyId, QueryId, QueryWalProfile, ReasonCode, ScopeHash,
    ScopeKind, StatementClass, WalBytes,
};

const SHMEM_NAME: &[u8] = b"pg_wal_budget shared state\0";
const LWLOCK_TRANCHE_NAME: &[u8] = b"pg_wal_budget\0";
const MAGIC: u32 = 0x5057_4201;
const LAYOUT_VERSION: u32 = 2;
const UNSET_ENUM: u8 = 0;

static mut SHARED_STATE: *mut PwbSharedState = ptr::null_mut();
static mut SHARED_LOCK: *mut pg_sys::LWLock = ptr::null_mut();
static mut PREV_SHMEM_REQUEST_HOOK: pg_sys::shmem_request_hook_type = None;
static mut PREV_SHMEM_STARTUP_HOOK: pg_sys::shmem_startup_hook_type = None;
static mut REQUESTED_LAYOUT: SharedLayout = SharedLayout::empty();

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PwbSharedState {
    magic: u32,
    layout_version: u32,
    recent_decision_capacity: u32,
    profile_cache_capacity: u32,
    budget_bucket_capacity: u32,
    recent_decision_head: u64,
    recent_decision_count: u32,
    profiles_len: u32,
    budget_buckets_len: u32,
    counters: PwbCounters,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PwbCounters {
    pub(crate) accepted_statements: u64,
    pub(crate) rejected_statements: u64,
    pub(crate) shadow_would_reject_count: u64,
    pub(crate) predicted_wal_bytes: u64,
    pub(crate) actual_wal_bytes: u64,
    pub(crate) absolute_prediction_error: u64,
    pub(crate) scope_debt_bytes: u64,
    pub(crate) missing_actual_wal_count: u64,
    pub(crate) internal_fail_open_count: u64,
    pub(crate) aborted_after_charge_count: u64,
}

impl PwbCounters {
    const fn saturating_add_delta(&mut self, delta: CounterDelta) {
        self.accepted_statements = self
            .accepted_statements
            .saturating_add(delta.accepted_statements);
        self.rejected_statements = self
            .rejected_statements
            .saturating_add(delta.rejected_statements);
        self.shadow_would_reject_count = self
            .shadow_would_reject_count
            .saturating_add(delta.shadow_would_reject_count);
        self.predicted_wal_bytes = self
            .predicted_wal_bytes
            .saturating_add(delta.predicted_wal_bytes);
        self.actual_wal_bytes = self.actual_wal_bytes.saturating_add(delta.actual_wal_bytes);
        self.absolute_prediction_error = self
            .absolute_prediction_error
            .saturating_add(delta.absolute_prediction_error);
        self.scope_debt_bytes = self.scope_debt_bytes.saturating_add(delta.scope_debt_bytes);
        self.missing_actual_wal_count = self
            .missing_actual_wal_count
            .saturating_add(delta.missing_actual_wal_count);
        self.internal_fail_open_count = self
            .internal_fail_open_count
            .saturating_add(delta.internal_fail_open_count);
        self.aborted_after_charge_count = self
            .aborted_after_charge_count
            .saturating_add(delta.aborted_after_charge_count);
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PwbRecentDecision {
    timestamp_epoch_ms: EpochMillis,
    decision_kind: u8,
    reason_code: u8,
    scope_kind: u8,
    statement_class: u8,
    has_policy_id: u8,
    has_query_id: u8,
    has_actual_wal_bytes: u8,
    _padding: u8,
    policy_id: PolicyId,
    query_id: QueryId,
    scope_hash: ScopeHash,
    predicted_wal_bytes: WalBytes,
    actual_wal_bytes: WalBytes,
    available_before: WalBytes,
    available_after: WalBytes,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PwbProfileEntry {
    occupied: u8,
    has_scope_hash: u8,
    _padding: [u8; 6],
    scope_hash: ScopeHash,
    query_id: QueryId,
    profile: PwbQueryWalProfile,
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PwbQueryWalProfile {
    calls: u64,
    ewma_wal_bytes: WalBytes,
    max_wal_bytes: WalBytes,
    last_seen_epoch_ms: EpochMillis,
}

impl From<QueryWalProfile> for PwbQueryWalProfile {
    fn from(profile: QueryWalProfile) -> Self {
        Self {
            calls: profile.calls,
            ewma_wal_bytes: profile.ewma_wal_bytes,
            max_wal_bytes: profile.max_wal_bytes,
            last_seen_epoch_ms: profile.last_seen_epoch_ms,
        }
    }
}

impl From<PwbQueryWalProfile> for QueryWalProfile {
    fn from(profile: PwbQueryWalProfile) -> Self {
        Self {
            calls: profile.calls,
            ewma_wal_bytes: profile.ewma_wal_bytes,
            max_wal_bytes: profile.max_wal_bytes,
            last_seen_epoch_ms: profile.last_seen_epoch_ms,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct PwbBudgetBucket {
    occupied: u8,
    _padding: [u8; 3],
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    available_bytes: WalBytes,
    max_burst_bytes: WalBytes,
    rate_bytes_per_sec: WalBytes,
    last_refill_epoch_ms: EpochMillis,
    debt_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetBucketState {
    pub(crate) policy_id: PolicyId,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) available_bytes: WalBytes,
    pub(crate) max_burst_bytes: WalBytes,
    pub(crate) rate_bytes_per_sec: WalBytes,
    pub(crate) last_refill_epoch_ms: EpochMillis,
    pub(crate) debt_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct QueryProfileSnapshot {
    pub(crate) scope_hash: Option<ScopeHash>,
    pub(crate) query_id: QueryId,
    pub(crate) profile: QueryWalProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BudgetBucketSnapshot {
    pub(crate) policy_id: PolicyId,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) available_bytes: WalBytes,
    pub(crate) max_burst_bytes: WalBytes,
    pub(crate) rate_bytes_per_sec: WalBytes,
    pub(crate) last_refill_epoch_ms: EpochMillis,
    pub(crate) debt_bytes: WalBytes,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct CounterDelta {
    pub(crate) accepted_statements: u64,
    pub(crate) rejected_statements: u64,
    pub(crate) shadow_would_reject_count: u64,
    pub(crate) predicted_wal_bytes: u64,
    pub(crate) actual_wal_bytes: u64,
    pub(crate) absolute_prediction_error: u64,
    pub(crate) scope_debt_bytes: u64,
    pub(crate) missing_actual_wal_count: u64,
    pub(crate) internal_fail_open_count: u64,
    pub(crate) aborted_after_charge_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecentDecisionRecord {
    pub(crate) timestamp_epoch_ms: EpochMillis,
    pub(crate) decision_kind: DecisionKind,
    pub(crate) policy_id: Option<PolicyId>,
    pub(crate) scope_kind: ScopeKind,
    pub(crate) scope_hash: ScopeHash,
    pub(crate) query_id: Option<QueryId>,
    pub(crate) statement_class: StatementClass,
    pub(crate) predicted_wal_bytes: WalBytes,
    pub(crate) actual_wal_bytes: Option<WalBytes>,
    pub(crate) available_before: WalBytes,
    pub(crate) available_after: WalBytes,
    pub(crate) reason_code: ReasonCode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProfileEwmaWeights {
    numerator: u64,
    denominator: u64,
}

impl ProfileEwmaWeights {
    pub(crate) fn new(numerator: u64, denominator: u64) -> PwbResult<Self> {
        if denominator == 0 || numerator == 0 || numerator > denominator {
            return Err(PwbError::Internal {
                message: format!("invalid profile EWMA weights: {numerator}/{denominator}"),
            });
        }

        Ok(Self {
            numerator,
            denominator,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SharedLayout {
    total_bytes: usize,
    recent_decisions_offset: usize,
    profiles_offset: usize,
    budget_buckets_offset: usize,
    recent_decision_capacity: usize,
    profile_cache_capacity: usize,
    budget_bucket_capacity: usize,
}

impl SharedLayout {
    const fn empty() -> Self {
        Self {
            total_bytes: 0,
            recent_decisions_offset: 0,
            profiles_offset: 0,
            budget_buckets_offset: 0,
            recent_decision_capacity: 0,
            profile_cache_capacity: 0,
            budget_bucket_capacity: 0,
        }
    }
}

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

pub(crate) fn add_counters(delta: CounterDelta) -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, _profiles| {
        state.counters.saturating_add_delta(delta);
        Ok(())
    })
}

pub(crate) fn snapshot_counters() -> PwbResult<PwbCounters> {
    with_locked_state(|state, _recent_decisions, _profiles| Ok(state.counters))
}

pub(crate) fn reset_counters() -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, _profiles| {
        state.counters = PwbCounters::default();
        Ok(())
    })
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

pub(crate) fn record_recent_decision(record: RecentDecisionRecord) -> PwbResult<()> {
    with_locked_state(|state, recent_decisions, _profiles| {
        let capacity = recent_decision_capacity(state);
        if capacity == 0 {
            return Ok(());
        }

        if state.recent_decision_head == u64::MAX {
            state.recent_decision_head = 0;
            state.recent_decision_count = 0;
            recent_decisions.fill(PwbRecentDecision::default());
        }

        let slot = ring_slot(state.recent_decision_head, capacity);
        recent_decisions[slot] = PwbRecentDecision::encode(record);
        state.recent_decision_head = state.recent_decision_head.saturating_add(1);
        state.recent_decision_count = state
            .recent_decision_count
            .saturating_add(1)
            .min(state.recent_decision_capacity);
        Ok(())
    })
}

pub(crate) fn snapshot_recent_decisions(limit: usize) -> PwbResult<Vec<RecentDecisionRecord>> {
    with_locked_state(|state, recent_decisions, _profiles| {
        let capacity = recent_decision_capacity(state);
        if capacity == 0 || limit == 0 {
            return Ok(Vec::new());
        }

        let count = recent_decision_count(state);
        let snapshot_count = limit.min(count).min(capacity);
        let mut records = Vec::with_capacity(snapshot_count);
        let mut sequence =
            state
                .recent_decision_head
                .checked_sub(1)
                .ok_or_else(|| PwbError::Internal {
                    message: "recent decision ring head underflow".to_string(),
                })?;

        for _ in 0..snapshot_count {
            let slot = ring_slot(sequence, capacity);
            records.push(recent_decisions[slot].decode()?);
            sequence = sequence.saturating_sub(1);
        }

        Ok(records)
    })
}

pub(crate) fn reset_recent_decisions() -> PwbResult<()> {
    with_locked_state(|state, recent_decisions, _profiles| {
        state.recent_decision_head = 0;
        state.recent_decision_count = 0;
        recent_decisions.fill(PwbRecentDecision::default());
        Ok(())
    })
}

pub(crate) fn reset_profiles() -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, profiles| {
        state.profiles_len = 0;
        profiles.fill(PwbProfileEntry::default());
        Ok(())
    })
}

pub(crate) fn snapshot_query_profiles() -> PwbResult<Vec<QueryProfileSnapshot>> {
    with_locked_state(|_state, _recent_decisions, profiles| snapshot_profiles_from_slice(profiles))
}

pub(crate) fn lookup_query_profile(
    scope_hash: Option<ScopeHash>,
    query_id: QueryId,
) -> PwbResult<Option<QueryWalProfile>> {
    with_locked_state(|_state, _recent_decisions, profiles| {
        let Some(slot) = find_profile_slot(profiles, scope_hash, query_id) else {
            return Ok(None);
        };

        Ok(Some(profiles[slot].decode()?.profile.into()))
    })
}

pub(crate) fn lookup_scoped_or_global_query_profile(
    scope_hash: ScopeHash,
    query_id: QueryId,
) -> PwbResult<Option<QueryWalProfile>> {
    with_locked_state(|_state, _recent_decisions, profiles| {
        if let Some(slot) = find_profile_slot(profiles, Some(scope_hash), query_id) {
            return Ok(Some(profiles[slot].decode()?.profile.into()));
        }

        let Some(slot) = find_profile_slot(profiles, None, query_id) else {
            return Ok(None);
        };

        Ok(Some(profiles[slot].decode()?.profile.into()))
    })
}

pub(crate) fn upsert_query_profile(
    scope_hash: Option<ScopeHash>,
    query_id: QueryId,
    actual_wal_bytes: WalBytes,
    now_epoch_ms: EpochMillis,
    ewma_weights: ProfileEwmaWeights,
) -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, profiles| {
        upsert_query_profile_locked(
            state,
            profiles,
            scope_hash,
            query_id,
            actual_wal_bytes,
            now_epoch_ms,
            ewma_weights,
        )
    })
}

pub(crate) fn upsert_scoped_and_global_query_profiles(
    scope_hash: ScopeHash,
    query_id: QueryId,
    actual_wal_bytes: WalBytes,
    now_epoch_ms: EpochMillis,
    ewma_weights: ProfileEwmaWeights,
) -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, profiles| {
        upsert_scoped_and_global_query_profiles_locked(
            state,
            profiles,
            scope_hash,
            query_id,
            actual_wal_bytes,
            now_epoch_ms,
            ewma_weights,
        )
    })
}

fn upsert_scoped_and_global_query_profiles_locked(
    state: &mut PwbSharedState,
    profiles: &mut [PwbProfileEntry],
    scope_hash: ScopeHash,
    query_id: QueryId,
    actual_wal_bytes: WalBytes,
    now_epoch_ms: EpochMillis,
    ewma_weights: ProfileEwmaWeights,
) -> PwbResult<()> {
    upsert_query_profile_locked(
        state,
        profiles,
        Some(scope_hash),
        query_id,
        actual_wal_bytes,
        now_epoch_ms,
        ewma_weights,
    )?;
    upsert_query_profile_locked(
        state,
        profiles,
        None,
        query_id,
        actual_wal_bytes,
        now_epoch_ms,
        ewma_weights,
    )
}

fn upsert_query_profile_locked(
    state: &mut PwbSharedState,
    profiles: &mut [PwbProfileEntry],
    scope_hash: Option<ScopeHash>,
    query_id: QueryId,
    actual_wal_bytes: WalBytes,
    now_epoch_ms: EpochMillis,
    ewma_weights: ProfileEwmaWeights,
) -> PwbResult<()> {
    let slot = if let Some(slot) = find_profile_slot(profiles, scope_hash, query_id) {
        slot
    } else if let Some(slot) = find_empty_profile_slot(profiles) {
        state.profiles_len = state
            .profiles_len
            .saturating_add(1)
            .min(state.profile_cache_capacity);
        slot
    } else {
        find_profile_eviction_slot(profiles).ok_or_else(profile_cache_capacity_exhausted)?
    };

    let profile = if profiles[slot].occupied == 1 {
        let mut existing = profiles[slot].decode()?.profile;
        let mut profile: QueryWalProfile = existing.into();
        profile.record_observation(
            actual_wal_bytes,
            now_epoch_ms,
            ewma_weights.numerator,
            ewma_weights.denominator,
        );
        existing = profile.into();
        existing
    } else {
        QueryWalProfile::new(actual_wal_bytes, now_epoch_ms).into()
    };

    profiles[slot] = PwbProfileEntry::encode(scope_hash, query_id, profile);
    Ok(())
}

pub(crate) fn snapshot_budget_buckets() -> PwbResult<Vec<BudgetBucketSnapshot>> {
    with_locked_bucket_state(|_state, buckets| snapshot_budget_buckets_from_slice(buckets))
}

pub(crate) fn with_budget_bucket<R>(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    initializer: impl FnOnce() -> BudgetBucketState,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<R> {
    with_locked_bucket_state(|state, buckets| {
        apply_budget_bucket(state, buckets, policy_id, scope_hash, initializer, callback)
    })
}

pub(crate) fn with_existing_budget_bucket<R>(
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<Option<R>> {
    with_locked_bucket_state(|_state, buckets| {
        let Some(slot) = find_budget_bucket_slot(buckets, policy_id, scope_hash) else {
            return Ok(None);
        };

        let mut bucket = buckets[slot].decode()?;
        let result = callback(&mut bucket)?;
        buckets[slot] = PwbBudgetBucket::encode(bucket);
        Ok(Some(result))
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

fn apply_budget_bucket<R>(
    state: &mut PwbSharedState,
    buckets: &mut [PwbBudgetBucket],
    policy_id: PolicyId,
    scope_hash: ScopeHash,
    initializer: impl FnOnce() -> BudgetBucketState,
    callback: impl FnOnce(&mut BudgetBucketState) -> PwbResult<R>,
) -> PwbResult<R> {
    if buckets.is_empty() {
        return Err(budget_bucket_capacity_exhausted());
    }

    if let Some(slot) = find_budget_bucket_slot(buckets, policy_id, scope_hash) {
        let mut bucket = buckets[slot].decode()?;
        let result = callback(&mut bucket)?;
        buckets[slot] = PwbBudgetBucket::encode(bucket);
        return Ok(result);
    }

    let slot =
        find_empty_budget_bucket_slot(buckets).ok_or_else(budget_bucket_capacity_exhausted)?;
    let mut bucket = initializer();
    let result = callback(&mut bucket)?;
    buckets[slot] = PwbBudgetBucket::encode(bucket);
    state.budget_buckets_len = state
        .budget_buckets_len
        .saturating_add(1)
        .min(state.budget_bucket_capacity);
    Ok(result)
}

fn snapshot_profiles_from_slice(
    profiles: &[PwbProfileEntry],
) -> PwbResult<Vec<QueryProfileSnapshot>> {
    let mut snapshots = Vec::new();

    for profile in profiles.iter().filter(|profile| profile.occupied == 1) {
        let decoded = profile.decode()?;
        snapshots.push(QueryProfileSnapshot {
            scope_hash: decoded.scope_hash,
            query_id: decoded.query_id,
            profile: decoded.profile.into(),
        });
    }

    Ok(snapshots)
}

fn snapshot_budget_buckets_from_slice(
    buckets: &[PwbBudgetBucket],
) -> PwbResult<Vec<BudgetBucketSnapshot>> {
    let mut snapshots = Vec::new();

    for bucket in buckets.iter().filter(|bucket| bucket.occupied == 1) {
        let decoded = bucket.decode()?;
        snapshots.push(BudgetBucketSnapshot {
            policy_id: decoded.policy_id,
            scope_hash: decoded.scope_hash,
            available_bytes: decoded.available_bytes,
            max_burst_bytes: decoded.max_burst_bytes,
            rate_bytes_per_sec: decoded.rate_bytes_per_sec,
            last_refill_epoch_ms: decoded.last_refill_epoch_ms,
            debt_bytes: decoded.debt_bytes,
        });
    }

    Ok(snapshots)
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

fn compute_layout(
    recent_decision_capacity: usize,
    profile_cache_capacity: usize,
) -> PwbResult<SharedLayout> {
    let mut offset = size_of::<PwbSharedState>();

    offset = align_up(offset, align_of::<PwbRecentDecision>())?;
    let recent_decisions_offset = offset;
    offset = checked_add(
        offset,
        checked_mul(recent_decision_capacity, size_of::<PwbRecentDecision>())?,
    )?;

    offset = align_up(offset, align_of::<PwbProfileEntry>())?;
    let profiles_offset = offset;
    offset = checked_add(
        offset,
        checked_mul(profile_cache_capacity, size_of::<PwbProfileEntry>())?,
    )?;

    offset = align_up(offset, align_of::<PwbBudgetBucket>())?;
    let budget_buckets_offset = offset;
    // Until pg_wal_budget has a dedicated bucket-capacity GUC, bucket capacity tracks the profile
    // cache capacity so shared-memory sizing remains bounded by existing postmaster settings.
    let budget_bucket_capacity = profile_cache_capacity;
    offset = checked_add(
        offset,
        checked_mul(budget_bucket_capacity, size_of::<PwbBudgetBucket>())?,
    )?;

    Ok(SharedLayout {
        total_bytes: offset,
        recent_decisions_offset,
        profiles_offset,
        budget_buckets_offset,
        recent_decision_capacity,
        profile_cache_capacity,
        budget_bucket_capacity,
    })
}

fn align_up(value: usize, alignment: usize) -> PwbResult<usize> {
    debug_assert!(alignment.is_power_of_two());
    let mask = alignment - 1;
    checked_add(value, mask).map(|adjusted| adjusted & !mask)
}

fn checked_add(left: usize, right: usize) -> PwbResult<usize> {
    left.checked_add(right).ok_or_else(|| PwbError::Internal {
        message: "shared memory size calculation overflowed".to_string(),
    })
}

fn checked_mul(left: usize, right: usize) -> PwbResult<usize> {
    left.checked_mul(right).ok_or_else(|| PwbError::Internal {
        message: "shared memory size calculation overflowed".to_string(),
    })
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "modulo result is strictly less than the usize capacity"
)]
fn ring_slot(sequence: u64, capacity: usize) -> usize {
    // Callers check the ring capacity once at the boundary; this helper stays infallible for the
    // shared-memory hot path.
    debug_assert!(capacity > 0);
    (sequence % capacity as u64) as usize
}

const fn recent_decision_capacity(state: &PwbSharedState) -> usize {
    state.recent_decision_capacity as usize
}

const fn recent_decision_count(state: &PwbSharedState) -> usize {
    state.recent_decision_count as usize
}

const fn budget_bucket_capacity(state: &PwbSharedState) -> usize {
    state.budget_bucket_capacity as usize
}

#[allow(
    clippy::cast_possible_truncation,
    reason = "capacity GUCs are bounded to 1,000,000 before layout construction"
)]
const fn capacity_to_u32(capacity: usize) -> u32 {
    // Shared memory layout capacities are derived from postmaster GUCs with a u32-safe upper bound.
    capacity as u32
}

impl PwbRecentDecision {
    fn encode(record: RecentDecisionRecord) -> Self {
        Self {
            timestamp_epoch_ms: record.timestamp_epoch_ms,
            decision_kind: encode_decision_kind(record.decision_kind),
            reason_code: encode_reason_code(record.reason_code),
            scope_kind: encode_scope_kind(record.scope_kind),
            statement_class: encode_statement_class(record.statement_class),
            has_policy_id: u8::from(record.policy_id.is_some()),
            has_query_id: u8::from(record.query_id.is_some()),
            has_actual_wal_bytes: u8::from(record.actual_wal_bytes.is_some()),
            _padding: 0,
            policy_id: record.policy_id.unwrap_or_default(),
            query_id: record.query_id.unwrap_or_default(),
            scope_hash: record.scope_hash,
            predicted_wal_bytes: record.predicted_wal_bytes,
            actual_wal_bytes: record.actual_wal_bytes.unwrap_or_default(),
            available_before: record.available_before,
            available_after: record.available_after,
        }
    }

    fn decode(self) -> PwbResult<RecentDecisionRecord> {
        Ok(RecentDecisionRecord {
            timestamp_epoch_ms: self.timestamp_epoch_ms,
            decision_kind: decode_decision_kind(self.decision_kind)?,
            policy_id: decode_optional(self.has_policy_id, self.policy_id)?,
            scope_kind: decode_scope_kind(self.scope_kind)?,
            scope_hash: self.scope_hash,
            query_id: decode_optional(self.has_query_id, self.query_id)?,
            statement_class: decode_statement_class(self.statement_class)?,
            predicted_wal_bytes: self.predicted_wal_bytes,
            actual_wal_bytes: decode_optional(self.has_actual_wal_bytes, self.actual_wal_bytes)?,
            available_before: self.available_before,
            available_after: self.available_after,
            reason_code: decode_reason_code(self.reason_code)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DecodedProfileEntry {
    scope_hash: Option<ScopeHash>,
    query_id: QueryId,
    profile: PwbQueryWalProfile,
}

impl PwbProfileEntry {
    const fn encode(
        scope_hash: Option<ScopeHash>,
        query_id: QueryId,
        profile: PwbQueryWalProfile,
    ) -> Self {
        Self {
            occupied: 1,
            has_scope_hash: if scope_hash.is_some() { 1 } else { 0 },
            _padding: [0; 6],
            scope_hash: match scope_hash {
                Some(scope_hash) => scope_hash,
                None => 0,
            },
            query_id,
            profile,
        }
    }

    fn decode(self) -> PwbResult<DecodedProfileEntry> {
        if self.occupied != 1 {
            return Err(PwbError::Internal {
                message: format!("invalid profile occupied flag: {}", self.occupied),
            });
        }

        Ok(DecodedProfileEntry {
            scope_hash: decode_optional(self.has_scope_hash, self.scope_hash)?,
            query_id: self.query_id,
            profile: self.profile,
        })
    }
}

impl PwbBudgetBucket {
    const fn encode(bucket: BudgetBucketState) -> Self {
        Self {
            occupied: 1,
            _padding: [0; 3],
            policy_id: bucket.policy_id,
            scope_hash: bucket.scope_hash,
            available_bytes: bucket.available_bytes,
            max_burst_bytes: bucket.max_burst_bytes,
            rate_bytes_per_sec: bucket.rate_bytes_per_sec,
            last_refill_epoch_ms: bucket.last_refill_epoch_ms,
            debt_bytes: bucket.debt_bytes,
        }
    }

    fn decode(self) -> PwbResult<BudgetBucketState> {
        if self.occupied != 1 {
            return Err(PwbError::Internal {
                message: format!("invalid budget bucket occupied flag: {}", self.occupied),
            });
        }

        Ok(BudgetBucketState {
            policy_id: self.policy_id,
            scope_hash: self.scope_hash,
            available_bytes: self.available_bytes,
            max_burst_bytes: self.max_burst_bytes,
            rate_bytes_per_sec: self.rate_bytes_per_sec,
            last_refill_epoch_ms: self.last_refill_epoch_ms,
            debt_bytes: self.debt_bytes,
        })
    }
}

fn find_profile_slot(
    profiles: &[PwbProfileEntry],
    scope_hash: Option<ScopeHash>,
    query_id: QueryId,
) -> Option<usize> {
    profiles.iter().position(|profile| {
        profile.occupied == 1
            && profile.query_id == query_id
            && scope_hash.map_or(profile.has_scope_hash == 0, |scope_hash| {
                profile.has_scope_hash == 1 && profile.scope_hash == scope_hash
            })
    })
}

fn find_empty_profile_slot(profiles: &[PwbProfileEntry]) -> Option<usize> {
    profiles.iter().position(|profile| profile.occupied == 0)
}

fn find_profile_eviction_slot(profiles: &[PwbProfileEntry]) -> Option<usize> {
    profiles
        .iter()
        .enumerate()
        .filter(|(_slot, profile)| profile.occupied == 1)
        .min_by_key(|(slot, profile)| (profile.profile.last_seen_epoch_ms, *slot))
        .map(|(slot, _profile)| slot)
}

fn find_budget_bucket_slot(
    buckets: &[PwbBudgetBucket],
    policy_id: PolicyId,
    scope_hash: ScopeHash,
) -> Option<usize> {
    buckets.iter().position(|bucket| {
        bucket.occupied == 1 && bucket.policy_id == policy_id && bucket.scope_hash == scope_hash
    })
}

fn find_empty_budget_bucket_slot(buckets: &[PwbBudgetBucket]) -> Option<usize> {
    buckets.iter().position(|bucket| bucket.occupied == 0)
}

fn decode_optional<T: Copy>(flag: u8, value: T) -> PwbResult<Option<T>> {
    match flag {
        0 => Ok(None),
        1 => Ok(Some(value)),
        _ => Err(PwbError::Internal {
            message: format!("invalid optional field flag in shared memory: {flag}"),
        }),
    }
}

const fn encode_decision_kind(kind: DecisionKind) -> u8 {
    match kind {
        DecisionKind::Allowed => 1,
        DecisionKind::WouldReject => 2,
        DecisionKind::Rejected => 3,
        DecisionKind::NoMatchingPolicy => 4,
        DecisionKind::MissingScope => 5,
        DecisionKind::InternalErrorFailOpen => 6,
    }
}

fn decode_decision_kind(value: u8) -> PwbResult<DecisionKind> {
    match value {
        1 => Ok(DecisionKind::Allowed),
        2 => Ok(DecisionKind::WouldReject),
        3 => Ok(DecisionKind::Rejected),
        4 => Ok(DecisionKind::NoMatchingPolicy),
        5 => Ok(DecisionKind::MissingScope),
        6 => Ok(DecisionKind::InternalErrorFailOpen),
        _ => invalid_enum("decision_kind", value),
    }
}

const fn encode_reason_code(code: ReasonCode) -> u8 {
    match code {
        ReasonCode::PolicyDisabled => 1,
        ReasonCode::PolicyMatched => 2,
        ReasonCode::BudgetAvailable => 3,
        ReasonCode::BudgetExceeded => 4,
        ReasonCode::ObserveMode => 5,
        ReasonCode::ShadowMode => 6,
        ReasonCode::NoMatchingPolicy => 7,
        ReasonCode::MissingScope => 8,
        ReasonCode::PredictionUnavailable => 9,
        ReasonCode::InternalErrorFailOpen => 10,
    }
}

fn decode_reason_code(value: u8) -> PwbResult<ReasonCode> {
    match value {
        1 => Ok(ReasonCode::PolicyDisabled),
        2 => Ok(ReasonCode::PolicyMatched),
        3 => Ok(ReasonCode::BudgetAvailable),
        4 => Ok(ReasonCode::BudgetExceeded),
        5 => Ok(ReasonCode::ObserveMode),
        6 => Ok(ReasonCode::ShadowMode),
        7 => Ok(ReasonCode::NoMatchingPolicy),
        8 => Ok(ReasonCode::MissingScope),
        9 => Ok(ReasonCode::PredictionUnavailable),
        10 => Ok(ReasonCode::InternalErrorFailOpen),
        _ => invalid_enum("reason_code", value),
    }
}

const fn encode_scope_kind(kind: ScopeKind) -> u8 {
    match kind {
        ScopeKind::Database => 1,
        ScopeKind::Role => 2,
        ScopeKind::Application => 3,
        ScopeKind::Tenant => 4,
        ScopeKind::Composite => 5,
    }
}

fn decode_scope_kind(value: u8) -> PwbResult<ScopeKind> {
    match value {
        1 => Ok(ScopeKind::Database),
        2 => Ok(ScopeKind::Role),
        3 => Ok(ScopeKind::Application),
        4 => Ok(ScopeKind::Tenant),
        5 => Ok(ScopeKind::Composite),
        _ => invalid_enum("scope_kind", value),
    }
}

const fn encode_statement_class(class: StatementClass) -> u8 {
    match class {
        StatementClass::ReadOnly => 1,
        StatementClass::Write => 2,
        StatementClass::Utility => 3,
        StatementClass::Copy => 4,
        StatementClass::Unknown => 5,
    }
}

fn decode_statement_class(value: u8) -> PwbResult<StatementClass> {
    match value {
        1 => Ok(StatementClass::ReadOnly),
        2 => Ok(StatementClass::Write),
        3 => Ok(StatementClass::Utility),
        4 => Ok(StatementClass::Copy),
        5 => Ok(StatementClass::Unknown),
        _ => invalid_enum("statement_class", value),
    }
}

fn invalid_enum<T>(field: &'static str, value: u8) -> PwbResult<T> {
    Err(PwbError::Internal {
        message: format!("invalid {field} enum value in shared memory: {value}"),
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state(budget_bucket_capacity: u32) -> PwbSharedState {
        PwbSharedState {
            magic: MAGIC,
            layout_version: LAYOUT_VERSION,
            recent_decision_capacity: 0,
            profile_cache_capacity: 0,
            budget_bucket_capacity,
            recent_decision_head: 0,
            recent_decision_count: 0,
            profiles_len: 0,
            budget_buckets_len: 0,
            counters: PwbCounters::default(),
        }
    }

    #[test]
    fn computes_aligned_layout() {
        let layout = compute_layout(7, 11).unwrap_or_else(|error| panic!("{error}"));

        assert!(layout.total_bytes >= size_of::<PwbSharedState>());
        assert_eq!(
            layout.recent_decisions_offset % align_of::<PwbRecentDecision>(),
            0
        );
        assert_eq!(layout.profiles_offset % align_of::<PwbProfileEntry>(), 0);
        assert_eq!(
            layout.budget_buckets_offset % align_of::<PwbBudgetBucket>(),
            0
        );
        assert_eq!(layout.recent_decision_capacity, 7);
        assert_eq!(layout.profile_cache_capacity, 11);
        assert_eq!(layout.budget_bucket_capacity, 11);
    }

    #[test]
    fn allows_zero_capacity_layout() {
        let layout = compute_layout(0, 0).unwrap_or_else(|error| panic!("{error}"));

        assert!(layout.total_bytes >= size_of::<PwbSharedState>());
        assert_eq!(layout.recent_decision_capacity, 0);
        assert_eq!(layout.profile_cache_capacity, 0);
    }

    #[test]
    fn rejects_layout_overflow() {
        assert!(compute_layout(usize::MAX, 1).is_err());
    }

    #[test]
    fn maps_ring_sequence_to_slot() {
        assert_eq!(ring_slot(0, 4), 0);
        assert_eq!(ring_slot(3, 4), 3);
        assert_eq!(ring_slot(4, 4), 0);
        assert_eq!(ring_slot(9, 4), 1);
    }

    #[test]
    fn encodes_and_decodes_recent_decision() {
        let record = RecentDecisionRecord {
            timestamp_epoch_ms: 123,
            decision_kind: DecisionKind::WouldReject,
            policy_id: Some(7),
            scope_kind: ScopeKind::Tenant,
            scope_hash: 99,
            query_id: Some(42),
            statement_class: StatementClass::Write,
            predicted_wal_bytes: 2048,
            actual_wal_bytes: Some(1024),
            available_before: 4096,
            available_after: 2048,
            reason_code: ReasonCode::BudgetExceeded,
        };

        let encoded = PwbRecentDecision::encode(record);
        let decoded = encoded.decode().unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(decoded, record);
    }

    #[test]
    fn encodes_and_decodes_budget_bucket() {
        let bucket = BudgetBucketState {
            policy_id: 7,
            scope_hash: 99,
            available_bytes: 1024,
            max_burst_bytes: 4096,
            rate_bytes_per_sec: 512,
            last_refill_epoch_ms: 123,
            debt_bytes: 64,
        };

        let encoded = PwbBudgetBucket::encode(bucket);
        let decoded = encoded.decode().unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(decoded, bucket);
    }

    #[test]
    fn snapshots_only_occupied_budget_buckets() {
        let buckets = [
            PwbBudgetBucket::encode(BudgetBucketState {
                policy_id: 7,
                scope_hash: 99,
                available_bytes: 1024,
                max_burst_bytes: 4096,
                rate_bytes_per_sec: 512,
                last_refill_epoch_ms: 123,
                debt_bytes: 64,
            }),
            PwbBudgetBucket::default(),
        ];

        let snapshots =
            snapshot_budget_buckets_from_slice(&buckets).unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].policy_id, 7);
        assert_eq!(snapshots[0].scope_hash, 99);
        assert_eq!(snapshots[0].debt_bytes, 64);
    }

    #[test]
    fn encodes_and_decodes_scoped_and_global_profiles() {
        let scoped =
            PwbProfileEntry::encode(Some(99), 42, PwbQueryWalProfile::from(profile(100, 1)));
        let global = PwbProfileEntry::encode(None, 42, PwbQueryWalProfile::from(profile(200, 2)));

        let decoded_scoped = scoped.decode().unwrap_or_else(|error| panic!("{error}"));
        let decoded_global = global.decode().unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(decoded_scoped.scope_hash, Some(99));
        assert_eq!(decoded_scoped.query_id, 42);
        assert_eq!(decoded_scoped.profile.ewma_wal_bytes, 100);
        assert_eq!(decoded_global.scope_hash, None);
        assert_eq!(decoded_global.query_id, 42);
        assert_eq!(decoded_global.profile.ewma_wal_bytes, 200);
    }

    #[test]
    fn finds_scoped_and_global_profile_slots_separately() {
        let profiles = [
            PwbProfileEntry::encode(Some(99), 42, PwbQueryWalProfile::from(profile(100, 1))),
            PwbProfileEntry::encode(None, 42, PwbQueryWalProfile::from(profile(200, 2))),
        ];

        assert_eq!(find_profile_slot(&profiles, Some(99), 42), Some(0));
        assert_eq!(find_profile_slot(&profiles, None, 42), Some(1));
        assert_eq!(find_profile_slot(&profiles, Some(100), 42), None);
    }

    #[test]
    fn snapshots_only_occupied_profiles() {
        let profiles = [
            PwbProfileEntry::encode(Some(99), 42, PwbQueryWalProfile::from(profile(100, 1))),
            PwbProfileEntry::default(),
            PwbProfileEntry::encode(None, 42, PwbQueryWalProfile::from(profile(200, 2))),
        ];

        let snapshots =
            snapshot_profiles_from_slice(&profiles).unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(snapshots.len(), 2);
        assert_eq!(snapshots[0].scope_hash, Some(99));
        assert_eq!(snapshots[1].scope_hash, None);
        assert_eq!(snapshots[1].profile.ewma_wal_bytes, 200);
    }

    #[test]
    fn upserts_profile_into_first_empty_slot() {
        let mut state = test_state(0);
        state.profile_cache_capacity = 2;
        let mut profiles = [PwbProfileEntry::default(), PwbProfileEntry::default()];

        upsert_query_profile_locked(
            &mut state,
            &mut profiles,
            Some(99),
            42,
            100,
            1,
            test_profile_weights(),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(state.profiles_len, 1);
        let decoded = profiles[0]
            .decode()
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(decoded.scope_hash, Some(99));
        assert_eq!(decoded.query_id, 42);
        assert_eq!(decoded.profile.ewma_wal_bytes, 100);
    }

    #[test]
    fn upserts_existing_profile_with_ewma() {
        let mut state = test_state(0);
        state.profile_cache_capacity = 1;
        state.profiles_len = 1;
        let mut profiles = [PwbProfileEntry::encode(
            Some(99),
            42,
            PwbQueryWalProfile::from(profile(100, 1)),
        )];

        upsert_query_profile_locked(
            &mut state,
            &mut profiles,
            Some(99),
            42,
            300,
            2,
            test_profile_weights(),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        let decoded = profiles[0]
            .decode()
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(decoded.profile.calls, 2);
        assert_eq!(decoded.profile.ewma_wal_bytes, 200);
        assert_eq!(decoded.profile.max_wal_bytes, 300);
        assert_eq!(decoded.profile.last_seen_epoch_ms, 2);
    }

    #[test]
    fn evicts_oldest_profile_when_capacity_is_full() {
        let mut state = test_state(0);
        state.profile_cache_capacity = 2;
        state.profiles_len = 2;
        let mut profiles = [
            PwbProfileEntry::encode(Some(1), 1, PwbQueryWalProfile::from(profile(100, 10))),
            PwbProfileEntry::encode(Some(2), 2, PwbQueryWalProfile::from(profile(200, 5))),
        ];

        upsert_query_profile_locked(
            &mut state,
            &mut profiles,
            Some(3),
            3,
            300,
            20,
            test_profile_weights(),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(find_profile_slot(&profiles, Some(1), 1), Some(0));
        assert_eq!(find_profile_slot(&profiles, Some(2), 2), None);
        assert_eq!(find_profile_slot(&profiles, Some(3), 3), Some(1));
        assert_eq!(state.profiles_len, 2);
    }

    #[test]
    fn rejects_profile_upsert_when_capacity_is_zero() {
        let mut state = test_state(0);
        let mut profiles = [];

        assert!(
            upsert_query_profile_locked(
                &mut state,
                &mut profiles,
                Some(99),
                42,
                100,
                1,
                test_profile_weights(),
            )
            .is_err()
        );
    }

    #[test]
    fn batched_profile_upsert_with_capacity_one_keeps_one_profile() {
        let mut state = test_state(0);
        state.profile_cache_capacity = 1;
        let mut profiles = [PwbProfileEntry::default()];

        upsert_scoped_and_global_query_profiles_locked(
            &mut state,
            &mut profiles,
            99,
            42,
            100,
            1,
            test_profile_weights(),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(state.profiles_len, 1);
        assert_eq!(
            profiles
                .iter()
                .filter(|profile| profile.occupied == 1)
                .count(),
            1
        );
    }

    #[test]
    fn batched_profile_upsert_updates_scoped_and_global_entries_when_capacity_allows() {
        let mut state = test_state(0);
        state.profile_cache_capacity = 2;
        let mut profiles = [PwbProfileEntry::default(), PwbProfileEntry::default()];

        upsert_scoped_and_global_query_profiles_locked(
            &mut state,
            &mut profiles,
            99,
            42,
            100,
            1,
            test_profile_weights(),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(find_profile_slot(&profiles, Some(99), 42), Some(0));
        assert_eq!(find_profile_slot(&profiles, None, 42), Some(1));
        assert_eq!(state.profiles_len, 2);
    }

    #[test]
    fn batched_profile_upsert_can_evict_twice_when_full() {
        let mut state = test_state(0);
        state.profile_cache_capacity = 2;
        state.profiles_len = 2;
        let mut profiles = [
            PwbProfileEntry::encode(Some(1), 1, PwbQueryWalProfile::from(profile(100, 1))),
            PwbProfileEntry::encode(Some(2), 2, PwbQueryWalProfile::from(profile(200, 2))),
        ];

        upsert_scoped_and_global_query_profiles_locked(
            &mut state,
            &mut profiles,
            99,
            42,
            300,
            3,
            test_profile_weights(),
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(find_profile_slot(&profiles, Some(99), 42), Some(0));
        assert_eq!(find_profile_slot(&profiles, None, 42), Some(1));
        assert_eq!(find_profile_slot(&profiles, Some(1), 1), None);
        assert_eq!(find_profile_slot(&profiles, Some(2), 2), None);
        assert_eq!(state.profiles_len, 2);
    }

    #[test]
    fn new_budget_bucket_is_not_persisted_when_callback_errors() {
        let mut state = test_state(1);
        let mut buckets = [PwbBudgetBucket::default()];
        let error = match apply_budget_bucket(
            &mut state,
            &mut buckets,
            7,
            99,
            || BudgetBucketState {
                policy_id: 7,
                scope_hash: 99,
                available_bytes: 1024,
                max_burst_bytes: 4096,
                rate_bytes_per_sec: 512,
                last_refill_epoch_ms: 123,
                debt_bytes: 0,
            },
            |_bucket| {
                Err(PwbError::BudgetExceeded {
                    policy_id: 7,
                    predicted_wal_bytes: 2048,
                    available_wal_bytes: 1024,
                })
            },
        ) {
            Ok(()) => panic!("expected callback error"),
            Err(error) => error,
        };

        assert!(matches!(error, PwbError::BudgetExceeded { .. }));
        assert_eq!(state.budget_buckets_len, 0);
        assert_eq!(buckets[0], PwbBudgetBucket::default());
    }

    #[test]
    fn existing_budget_bucket_is_persisted_when_callback_succeeds() {
        let mut state = test_state(1);
        state.budget_buckets_len = 1;
        let initial = BudgetBucketState {
            policy_id: 7,
            scope_hash: 99,
            available_bytes: 1024,
            max_burst_bytes: 4096,
            rate_bytes_per_sec: 512,
            last_refill_epoch_ms: 123,
            debt_bytes: 0,
        };
        let mut buckets = [PwbBudgetBucket::encode(initial)];

        apply_budget_bucket(
            &mut state,
            &mut buckets,
            7,
            99,
            || panic!("existing bucket should not be initialized"),
            |bucket| {
                bucket.available_bytes = 256;
                Ok(())
            },
        )
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(state.budget_buckets_len, 1);
        assert_eq!(
            buckets[0]
                .decode()
                .unwrap_or_else(|error| panic!("{error}"))
                .available_bytes,
            256
        );
    }

    #[test]
    fn rejects_invalid_enum_decode() {
        let mut encoded = PwbRecentDecision::encode(RecentDecisionRecord {
            timestamp_epoch_ms: 123,
            decision_kind: DecisionKind::Allowed,
            policy_id: None,
            scope_kind: ScopeKind::Database,
            scope_hash: 99,
            query_id: None,
            statement_class: StatementClass::ReadOnly,
            predicted_wal_bytes: 0,
            actual_wal_bytes: None,
            available_before: 0,
            available_after: 0,
            reason_code: ReasonCode::BudgetAvailable,
        });
        encoded.decision_kind = UNSET_ENUM;

        assert!(encoded.decode().is_err());
    }

    const fn profile(ewma_wal_bytes: WalBytes, last_seen_epoch_ms: EpochMillis) -> QueryWalProfile {
        QueryWalProfile {
            calls: 1,
            ewma_wal_bytes,
            max_wal_bytes: ewma_wal_bytes,
            last_seen_epoch_ms,
        }
    }

    fn test_profile_weights() -> ProfileEwmaWeights {
        ProfileEwmaWeights::new(1, 2).unwrap_or_else(|error| panic!("{error}"))
    }
}
