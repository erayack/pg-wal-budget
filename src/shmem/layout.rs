use core::mem::{align_of, size_of};

use crate::errors::{PwbError, PwbResult};

use super::records::{
    PwbBudgetBucket, PwbProfileEntry, PwbRecentDecision, PwbScopeName, PwbSharedState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SharedLayout {
    pub(super) total_bytes: usize,
    pub(super) recent_decisions_offset: usize,
    pub(super) profiles_offset: usize,
    pub(super) budget_buckets_offset: usize,
    pub(super) scope_names_offset: usize,
    pub(super) recent_decision_capacity: usize,
    pub(super) profile_cache_capacity: usize,
    pub(super) budget_bucket_capacity: usize,
    pub(super) scope_name_capacity: usize,
}

impl SharedLayout {
    pub(super) const fn empty() -> Self {
        Self {
            total_bytes: 0,
            recent_decisions_offset: 0,
            profiles_offset: 0,
            budget_buckets_offset: 0,
            scope_names_offset: 0,
            recent_decision_capacity: 0,
            profile_cache_capacity: 0,
            budget_bucket_capacity: 0,
            scope_name_capacity: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SharedCapacities {
    pub(super) recent_decisions: usize,
    pub(super) profiles: usize,
    pub(super) budget_buckets: usize,
    pub(super) scope_names: usize,
}

pub(super) fn compute_layout(capacities: SharedCapacities) -> PwbResult<SharedLayout> {
    let mut offset = size_of::<PwbSharedState>();

    offset = align_up(offset, align_of::<PwbRecentDecision>())?;
    let recent_decisions_offset = offset;
    offset = checked_add(
        offset,
        checked_mul(capacities.recent_decisions, size_of::<PwbRecentDecision>())?,
    )?;

    offset = align_up(offset, align_of::<PwbProfileEntry>())?;
    let profiles_offset = offset;
    offset = checked_add(
        offset,
        checked_mul(capacities.profiles, size_of::<PwbProfileEntry>())?,
    )?;

    offset = align_up(offset, align_of::<PwbBudgetBucket>())?;
    let budget_buckets_offset = offset;
    offset = checked_add(
        offset,
        checked_mul(capacities.budget_buckets, size_of::<PwbBudgetBucket>())?,
    )?;

    offset = align_up(offset, align_of::<PwbScopeName>())?;
    let scope_names_offset = offset;
    offset = checked_add(
        offset,
        checked_mul(capacities.scope_names, size_of::<PwbScopeName>())?,
    )?;

    Ok(SharedLayout {
        total_bytes: offset,
        recent_decisions_offset,
        profiles_offset,
        budget_buckets_offset,
        scope_names_offset,
        recent_decision_capacity: capacities.recent_decisions,
        profile_cache_capacity: capacities.profiles,
        budget_bucket_capacity: capacities.budget_buckets,
        scope_name_capacity: capacities.scope_names,
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
    reason = "capacity GUCs are bounded to 1,000,000 before layout construction"
)]
pub(super) const fn capacity_to_u32(capacity: usize) -> u32 {
    // Shared memory layout capacities are derived from postmaster GUCs with a u32-safe upper bound.
    capacity as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_aligned_layout() {
        let layout = compute_layout(SharedCapacities {
            recent_decisions: 3,
            profiles: 5,
            budget_buckets: 7,
            scope_names: 11,
        })
        .unwrap_or_else(|error| panic!("{error}"));

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
        assert_eq!(layout.scope_names_offset % align_of::<PwbScopeName>(), 0);
        assert_eq!(layout.recent_decision_capacity, 3);
        assert_eq!(layout.profile_cache_capacity, 5);
        assert_eq!(layout.budget_bucket_capacity, 7);
        assert_eq!(layout.scope_name_capacity, 11);
    }

    #[test]
    fn allows_zero_capacity_layout() {
        let layout = compute_layout(SharedCapacities {
            recent_decisions: 0,
            profiles: 0,
            budget_buckets: 0,
            scope_names: 0,
        })
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(layout.recent_decision_capacity, 0);
        assert_eq!(layout.profile_cache_capacity, 0);
        assert_eq!(layout.budget_bucket_capacity, 0);
        assert_eq!(layout.scope_name_capacity, 0);
    }

    #[test]
    fn rejects_layout_overflow() {
        assert!(
            compute_layout(SharedCapacities {
                recent_decisions: usize::MAX,
                profiles: 0,
                budget_buckets: 0,
                scope_names: 0,
            })
            .is_err()
        );
    }
}
