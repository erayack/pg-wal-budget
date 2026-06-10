use crate::catalog::{DurableCatalogStore, SpiCatalogStore};
use crate::errors::PwbResult;
use crate::shmem::QueryProfileSnapshot;

pub(crate) fn load_profiles(limit: usize) -> PwbResult<Vec<QueryProfileSnapshot>> {
    load_profiles_from(&SpiCatalogStore, limit)
}

fn load_profiles_from(
    store: &impl DurableCatalogStore,
    limit: usize,
) -> PwbResult<Vec<QueryProfileSnapshot>> {
    store.load_profiles(limit)
}

pub(crate) fn persist_profiles(profiles: &[QueryProfileSnapshot]) -> PwbResult<()> {
    persist_profiles_with(&SpiCatalogStore, profiles)
}

fn persist_profiles_with(
    store: &impl DurableCatalogStore,
    profiles: &[QueryProfileSnapshot],
) -> PwbResult<()> {
    store.persist_profiles(profiles)
}

pub(crate) fn delete_profiles() -> PwbResult<()> {
    delete_profiles_with(&SpiCatalogStore)
}

fn delete_profiles_with(store: &impl DurableCatalogStore) -> PwbResult<()> {
    store.delete_profiles()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MemoryCatalogStore;
    use crate::types::QueryWalProfile;

    #[test]
    fn profile_store_entrypoints_work_with_memory_store() {
        let store = MemoryCatalogStore::with_rows(Vec::new(), Vec::new());
        let snapshot = QueryProfileSnapshot {
            scope_hash: Some(7),
            query_id: 42,
            profile: QueryWalProfile {
                calls: 1,
                ewma_wal_bytes: 10,
                max_wal_bytes: 10,
                last_seen_epoch_ms: 100,
            },
        };

        persist_profiles_with(&store, &[snapshot]).unwrap_or_else(|error| panic!("{error}"));
        let loaded = load_profiles_from(&store, 10).unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(loaded, vec![snapshot]);
        delete_profiles_with(&store).unwrap_or_else(|error| panic!("{error}"));
        let loaded = load_profiles_from(&store, 10).unwrap_or_else(|error| panic!("{error}"));
        assert!(loaded.is_empty());
    }
}
