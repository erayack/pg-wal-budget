use crate::errors::{PwbError, PwbResult};

use super::records::{PwbRecentDecision, PwbSharedState, RecentDecisionRecord};
use super::with_locked_state;

pub(crate) fn record_recent_decision(record: RecentDecisionRecord) -> PwbResult<()> {
    with_locked_state(|state, recent_decisions, _profiles| {
        record_recent_decision_locked(state, recent_decisions, record);
        Ok(())
    })
}

pub(super) fn record_recent_decision_locked(
    state: &mut PwbSharedState,
    recent_decisions: &mut [PwbRecentDecision],
    record: RecentDecisionRecord,
) {
    let capacity = recent_decision_capacity(state);
    if capacity == 0 {
        return;
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
}

pub(crate) fn snapshot_recent_decisions(limit: usize) -> PwbResult<Vec<RecentDecisionRecord>> {
    with_locked_state(|state, recent_decisions, _profiles| {
        snapshot_recent_decisions_from_slice(state, recent_decisions, limit)
    })
}

fn snapshot_recent_decisions_from_slice(
    state: &PwbSharedState,
    recent_decisions: &[PwbRecentDecision],
    limit: usize,
) -> PwbResult<Vec<RecentDecisionRecord>> {
    let capacity = recent_decision_capacity(state);
    if capacity == 0 || limit == 0 {
        return Ok(Vec::new());
    }

    let count = recent_decision_count(state);
    let snapshot_count = limit.min(count).min(capacity);
    if snapshot_count == 0 {
        return Ok(Vec::new());
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_ring_sequence_to_slot() {
        assert_eq!(ring_slot(0, 4), 0);
        assert_eq!(ring_slot(3, 4), 3);
        assert_eq!(ring_slot(4, 4), 0);
        assert_eq!(ring_slot(9, 4), 1);
    }

    #[test]
    fn snapshots_empty_ring_without_underflow() {
        let state = PwbSharedState {
            recent_decision_capacity: 4,
            ..crate::shmem::records::test_state(0)
        };
        let recent_decisions = [PwbRecentDecision::default(); 4];

        let snapshot = snapshot_recent_decisions_from_slice(&state, &recent_decisions, 20)
            .unwrap_or_else(|error| panic!("{error}"));

        assert!(snapshot.is_empty());
    }
}
