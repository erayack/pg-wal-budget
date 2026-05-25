use crate::errors::PwbResult;
use crate::types::{ScopeHash, ScopeKind};

use super::records::{PwbScopeName, ScopeNameSnapshot};
use super::with_locked_scope_name_state;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ScopeNameRecordResult {
    Recorded,
    Dropped,
}

pub(crate) fn snapshot_scope_names() -> PwbResult<Vec<ScopeNameSnapshot>> {
    with_locked_scope_name_state(|_state, scope_names| snapshot_scope_names_from_slice(scope_names))
}

pub(super) fn record_scope_name_in_slice(
    scope_names: &mut [PwbScopeName],
    scope_kind: ScopeKind,
    scope_hash: ScopeHash,
    scope_value: &str,
    now_epoch_ms: u64,
) -> ScopeNameRecordResult {
    let mut first_empty_slot = None;

    for (slot, scope_name) in scope_names.iter_mut().enumerate() {
        if scope_name.matches_key(scope_kind, scope_hash) {
            scope_name.last_seen_epoch_ms = now_epoch_ms;
            scope_name.seen_count = scope_name.seen_count.saturating_add(1);
            return ScopeNameRecordResult::Recorded;
        } else if scope_name.occupied == 0 && first_empty_slot.is_none() {
            first_empty_slot = Some(slot);
        }
    }

    let Some(slot) = first_empty_slot else {
        return ScopeNameRecordResult::Dropped;
    };

    scope_names[slot] = PwbScopeName::encode_value(
        scope_kind,
        scope_hash,
        scope_value,
        now_epoch_ms,
        now_epoch_ms,
        1,
    );
    ScopeNameRecordResult::Recorded
}

fn snapshot_scope_names_from_slice(
    scope_names: &[PwbScopeName],
) -> PwbResult<Vec<ScopeNameSnapshot>> {
    let mut snapshots = Vec::new();

    for scope_name in scope_names
        .iter()
        .filter(|scope_name| scope_name.occupied == 1)
    {
        snapshots.push(scope_name.decode()?);
    }

    Ok(snapshots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_new_scope_name() {
        let mut scope_names = [PwbScopeName::default()];

        record_scope_name_in_slice(&mut scope_names, ScopeKind::Tenant, 99, "tenant-a", 123);

        let snapshots =
            snapshot_scope_names_from_slice(&scope_names).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].scope_kind, ScopeKind::Tenant);
        assert_eq!(snapshots[0].scope_hash, 99);
        assert_eq!(snapshots[0].scope_value, "tenant-a");
        assert_eq!(snapshots[0].first_seen_epoch_ms, 123);
        assert_eq!(snapshots[0].last_seen_epoch_ms, 123);
        assert_eq!(snapshots[0].seen_count, 1);
    }

    #[test]
    fn updates_existing_scope_name_without_replacing_value() {
        let mut scope_names = [PwbScopeName::default()];

        record_scope_name_in_slice(&mut scope_names, ScopeKind::Tenant, 99, "tenant-a", 123);
        record_scope_name_in_slice(
            &mut scope_names,
            ScopeKind::Tenant,
            99,
            "tenant-renamed",
            456,
        );

        let snapshots =
            snapshot_scope_names_from_slice(&scope_names).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].scope_value, "tenant-a");
        assert_eq!(snapshots[0].first_seen_epoch_ms, 123);
        assert_eq!(snapshots[0].last_seen_epoch_ms, 456);
        assert_eq!(snapshots[0].seen_count, 2);
    }

    #[test]
    fn drops_new_scope_name_when_capacity_is_full() {
        let mut scope_names = [PwbScopeName::encode_value(
            ScopeKind::Tenant,
            99,
            "tenant-a",
            123,
            123,
            1,
        )];

        let result =
            record_scope_name_in_slice(&mut scope_names, ScopeKind::Tenant, 100, "tenant-b", 456);

        let snapshots =
            snapshot_scope_names_from_slice(&scope_names).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(result, ScopeNameRecordResult::Dropped);
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].scope_hash, 99);
    }
}
