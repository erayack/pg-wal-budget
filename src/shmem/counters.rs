use crate::errors::PwbResult;

use super::{CounterDelta, PwbCounters, with_locked_state};

pub(crate) fn add_counters(delta: CounterDelta) -> PwbResult<()> {
    with_locked_state(|state, _recent_decisions, _profiles| {
        state.counters.saturating_add_delta(delta);
        Ok(())
    })
}

pub(crate) fn snapshot_counters() -> PwbResult<PwbCounters> {
    with_locked_state(|state, _recent_decisions, _profiles| Ok(state.counters))
}
