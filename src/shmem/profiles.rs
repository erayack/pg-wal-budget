use crate::errors::PwbResult;
use crate::types::{
    EpochMillis, ProfileEwmaWeights, QueryId, QueryWalProfile, ScopeHash, WalBytes,
};

use super::records::{PwbProfileEntry, PwbSharedState, QueryProfileSnapshot};
use super::{profile_cache_capacity_exhausted, with_locked_state};

const PROFILE_RESTORE_NOT_ATTEMPTED: u8 = 0;
const PROFILE_RESTORE_IN_PROGRESS: u8 = 1;
const PROFILE_RESTORE_LOADED: u8 = 2;
const PROFILE_RESTORE_FAILED: u8 = 3;

pub(super) const fn initial_profile_restore_state() -> u8 {
    PROFILE_RESTORE_NOT_ATTEMPTED
}

pub(crate) fn reset_profiles() -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, profiles| {
        state.profiles_len = 0;
        state.profile_restore_state = PROFILE_RESTORE_LOADED;
        state.profile_restore_started_epoch_ms = 0;
        state.last_profile_persist_epoch_ms = 0;
        state.profile_dirty_count = 0;
        profiles.fill(PwbProfileEntry::default());
        Ok(())
    })
}

pub(crate) fn snapshot_query_profiles() -> PwbResult<Vec<QueryProfileSnapshot>> {
    with_locked_state(|_state, _recent_decisions, profiles| snapshot_profiles_from_slice(profiles))
}

pub(crate) fn begin_profile_restore(
    now_epoch_ms: EpochMillis,
    stale_after_ms: EpochMillis,
) -> PwbResult<bool> {
    with_locked_state(|state, _recent_decisions, _profiles| {
        let stale_restore = state.profile_restore_state == PROFILE_RESTORE_IN_PROGRESS
            && now_epoch_ms.saturating_sub(state.profile_restore_started_epoch_ms)
                >= stale_after_ms;

        if matches!(
            state.profile_restore_state,
            PROFILE_RESTORE_NOT_ATTEMPTED | PROFILE_RESTORE_FAILED
        ) || stale_restore
        {
            state.profile_restore_state = PROFILE_RESTORE_IN_PROGRESS;
            state.profile_restore_started_epoch_ms = now_epoch_ms;
            return Ok(true);
        }

        Ok(false)
    })
}

pub(crate) fn finish_profile_restore(restored: &[QueryProfileSnapshot]) -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, profiles| {
        for snapshot in restored {
            upsert_restored_query_profile_locked(state, profiles, *snapshot)?;
        }
        state.profile_restore_state = PROFILE_RESTORE_LOADED;
        state.profile_restore_started_epoch_ms = 0;
        Ok(())
    })
}

pub(crate) fn mark_profile_restore_failed() -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, _profiles| {
        if state.profile_restore_state == PROFILE_RESTORE_IN_PROGRESS {
            state.profile_restore_state = PROFILE_RESTORE_FAILED;
            state.profile_restore_started_epoch_ms = 0;
        }
        Ok(())
    })
}

pub(crate) fn reserve_profile_persist(
    now_epoch_ms: EpochMillis,
    interval_ms: EpochMillis,
) -> PwbResult<Option<Vec<QueryProfileSnapshot>>> {
    with_locked_state(|state, _recent_decisions, profiles| {
        if state.profile_dirty_count == 0
            || now_epoch_ms.saturating_sub(state.last_profile_persist_epoch_ms) < interval_ms
        {
            return Ok(None);
        }

        let snapshots = snapshot_profiles_from_slice(profiles)?;
        state.last_profile_persist_epoch_ms = now_epoch_ms;
        Ok(Some(snapshots))
    })
}

pub(crate) fn snapshot_profiles_for_persist(
    now_epoch_ms: EpochMillis,
) -> PwbResult<Vec<QueryProfileSnapshot>> {
    with_locked_state(|state, _recent_decisions, profiles| {
        state.last_profile_persist_epoch_ms = now_epoch_ms;
        snapshot_profiles_from_slice(profiles)
    })
}

pub(crate) fn complete_profile_persist(success: bool) -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, _profiles| {
        if success {
            state.profile_dirty_count = 0;
        }
        Ok(())
    })
}

pub(crate) fn lookup_scoped_or_global_query_profile(
    scope_hash: ScopeHash,
    query_id: QueryId,
) -> PwbResult<Option<QueryWalProfile>> {
    with_locked_state(|_state, _recent_decisions, profiles| {
        if let Some(slot) = find_profile_slot(profiles, Some(scope_hash), query_id) {
            return Ok(Some(profiles[slot].profile.into()));
        }

        let Some(slot) = find_profile_slot(profiles, None, query_id) else {
            return Ok(None);
        };

        Ok(Some(profiles[slot].profile.into()))
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
        )?;
        state.profile_dirty_count = state.profile_dirty_count.saturating_add(1);
        Ok(())
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
    let (slot, profile) = if let Some(slot) = find_profile_slot(profiles, scope_hash, query_id) {
        // `find_profile_slot` only returns occupied entries matching the trusted profile key.
        let mut profile: QueryWalProfile = profiles[slot].profile.into();
        profile.record_observation(
            actual_wal_bytes,
            now_epoch_ms,
            ewma_weights.numerator,
            ewma_weights.denominator,
        );
        (slot, profile.into())
    } else {
        let slot = if let Some(slot) = find_empty_profile_slot(profiles) {
            state.profiles_len = state
                .profiles_len
                .saturating_add(1)
                .min(state.profile_cache_capacity);
            slot
        } else {
            find_profile_eviction_slot(profiles).ok_or_else(profile_cache_capacity_exhausted)?
        };
        (
            slot,
            QueryWalProfile::new(actual_wal_bytes, now_epoch_ms).into(),
        )
    };

    profiles[slot] = PwbProfileEntry::encode(scope_hash, query_id, profile);
    Ok(())
}

fn upsert_restored_query_profile_locked(
    state: &mut PwbSharedState,
    profiles: &mut [PwbProfileEntry],
    snapshot: QueryProfileSnapshot,
) -> PwbResult<()> {
    if profiles.is_empty() {
        return Err(profile_cache_capacity_exhausted());
    }

    if let Some(slot) = find_profile_slot(profiles, snapshot.scope_hash, snapshot.query_id) {
        let existing: QueryWalProfile = profiles[slot].profile.into();
        if existing.last_seen_epoch_ms <= snapshot.profile.last_seen_epoch_ms {
            profiles[slot] = PwbProfileEntry::encode(
                snapshot.scope_hash,
                snapshot.query_id,
                snapshot.profile.into(),
            );
        }
        return Ok(());
    }

    let slot = if let Some(slot) = find_empty_profile_slot(profiles) {
        state.profiles_len = state
            .profiles_len
            .saturating_add(1)
            .min(state.profile_cache_capacity);
        slot
    } else {
        let eviction_slot =
            find_profile_eviction_slot(profiles).ok_or_else(profile_cache_capacity_exhausted)?;
        let existing: QueryWalProfile = profiles[eviction_slot].profile.into();
        if existing.last_seen_epoch_ms > snapshot.profile.last_seen_epoch_ms {
            return Ok(());
        }
        eviction_slot
    };

    profiles[slot] = PwbProfileEntry::encode(
        snapshot.scope_hash,
        snapshot.query_id,
        snapshot.profile.into(),
    );
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shmem::records::{PwbQueryWalProfile, test_state};

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
