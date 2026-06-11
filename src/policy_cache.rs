use std::cell::RefCell;

use crate::budget::EffectivePolicy;
use crate::catalog::{DurableCatalogStore, DurablePolicyRow, SpiCatalogStore};
use crate::errors::{PwbError, PwbResult};
use crate::policy::{normalize_scope_value, validate_policy_budget_update};
use crate::types::{BudgetMode, EpochMillis, ScopeKey, ScopeKind};

const POLICY_CACHE_REFRESH_INTERVAL_MS: EpochMillis = 1000;

thread_local! {
    static POLICY_CACHE: RefCell<Option<PolicyCache>> = const { RefCell::new(None) };
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CachedEffectivePolicy {
    scope_kind: ScopeKind,
    scope_value: Option<String>,
    policy: EffectivePolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PolicyCache {
    refreshed_epoch_ms: EpochMillis,
    policies: Vec<CachedEffectivePolicy>,
}

pub(crate) fn effective_policy_for_scope(
    scope: &ScopeKey,
    now_epoch_ms: EpochMillis,
) -> PwbResult<Option<EffectivePolicy>> {
    let should_reload = POLICY_CACHE.with(|cache| {
        cache
            .borrow()
            .as_ref()
            .is_none_or(|cache| cache_is_stale(cache, now_epoch_ms))
    });
    if should_reload {
        let loaded_cache = load_policy_cache(now_epoch_ms)?;
        POLICY_CACHE.with(|cache| {
            *cache.borrow_mut() = Some(loaded_cache);
        });
    }

    POLICY_CACHE.with(|cache| {
        let cache = cache.borrow();
        let cache = cache.as_ref().ok_or_else(|| PwbError::Internal {
            message: "policy cache was not initialized".to_string(),
        })?;
        Ok(find_effective_policy(cache, scope))
    })
}

pub(crate) fn invalidate_backend_policy_cache() {
    POLICY_CACHE.with(|cache| {
        *cache.borrow_mut() = None;
    });
}

fn load_policy_cache(now_epoch_ms: EpochMillis) -> PwbResult<PolicyCache> {
    load_policy_cache_from(&SpiCatalogStore, now_epoch_ms)
}

fn load_policy_cache_from(
    store: &impl DurableCatalogStore,
    now_epoch_ms: EpochMillis,
) -> PwbResult<PolicyCache> {
    let policies = load_policy_rows(store.load_enabled_policy_rows()?)?;

    Ok(PolicyCache {
        refreshed_epoch_ms: now_epoch_ms,
        policies,
    })
}

fn load_policy_rows(rows: Vec<DurablePolicyRow>) -> PwbResult<Vec<CachedEffectivePolicy>> {
    rows.into_iter()
        .map(|row| {
            let budget =
                validate_policy_budget_update(row.wal_rate_bytes_per_sec, row.wal_burst_bytes)?;
            Ok(CachedEffectivePolicy {
                scope_kind: ScopeKind::parse_sql(&row.scope_kind)?,
                scope_value: normalize_scope_value(row.scope_value.as_deref()),
                policy: EffectivePolicy {
                    policy_id: row.policy_id,
                    enabled: row.enabled,
                    mode: BudgetMode::parse_sql(&row.mode)?,
                    wal_rate_bytes_per_sec: budget.wal_rate_bytes_per_sec,
                    wal_burst_bytes: budget.wal_burst_bytes,
                },
            })
        })
        .collect()
}

fn find_effective_policy(cache: &PolicyCache, scope: &ScopeKey) -> Option<EffectivePolicy> {
    for policy in &cache.policies {
        if policy_matches_scope(policy, scope) {
            return Some(policy.policy);
        }
    }
    None
}

fn policy_matches_scope(policy: &CachedEffectivePolicy, scope: &ScopeKey) -> bool {
    if policy.scope_kind != scope.kind {
        return false;
    }

    policy
        .scope_value
        .as_deref()
        .is_none_or(|policy_scope| Some(policy_scope) == scope.debug_value.as_deref())
}

const fn cache_is_stale(cache: &PolicyCache, now_epoch_ms: EpochMillis) -> bool {
    now_epoch_ms.saturating_sub(cache.refreshed_epoch_ms) >= POLICY_CACHE_REFRESH_INTERVAL_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::MemoryCatalogStore;
    use crate::types::{PolicyId, ScopeHash};

    const POLICY_ID: PolicyId = 11;
    const SCOPE_HASH: ScopeHash = 99;

    #[test]
    fn policy_cache_matches_first_enabled_policy_by_scope_order() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![
                cached_policy(POLICY_ID, ScopeKind::Tenant, Some("tenant-a")),
                cached_policy(POLICY_ID + 1, ScopeKind::Tenant, None),
            ],
        };
        let scope = ScopeKey::with_debug_value(ScopeKind::Tenant, SCOPE_HASH, "tenant-a");

        assert_eq!(
            find_effective_policy(&cache, &scope).map(|policy| policy.policy_id),
            Some(POLICY_ID)
        );
    }

    #[test]
    fn builds_policy_cache_entries_from_memory_store() {
        let store = MemoryCatalogStore::with_rows(
            vec![DurablePolicyRow {
                policy_id: POLICY_ID,
                scope_kind: "tenant".to_string(),
                scope_value: Some(" tenant-a ".to_string()),
                enabled: true,
                mode: "reject".to_string(),
                wal_rate_bytes_per_sec: 100,
                wal_burst_bytes: 500,
            }],
            Vec::new(),
        );
        let cache = load_policy_cache_from(&store, 1000).unwrap_or_else(|error| panic!("{error}"));
        let scope = ScopeKey::with_debug_value(ScopeKind::Tenant, SCOPE_HASH, "tenant-a");

        let policy =
            find_effective_policy(&cache, &scope).unwrap_or_else(|| panic!("policy should match"));
        assert_eq!(policy.policy_id, POLICY_ID);
        assert_eq!(policy.mode, BudgetMode::Reject);
        assert_eq!(policy.wal_burst_bytes, 500);
    }

    #[test]
    fn policy_cache_uses_wildcard_scope_when_exact_value_does_not_match() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![
                cached_policy(POLICY_ID, ScopeKind::Tenant, Some("tenant-a")),
                cached_policy(POLICY_ID + 1, ScopeKind::Tenant, None),
            ],
        };
        let scope = ScopeKey::with_debug_value(ScopeKind::Tenant, SCOPE_HASH, "tenant-b");

        assert_eq!(
            find_effective_policy(&cache, &scope).map(|policy| policy.policy_id),
            Some(POLICY_ID + 1)
        );
    }

    #[test]
    fn policy_cache_matches_composite_scope() {
        let composite = "tenant=tenant-a|role=app_user|database=postgres";
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![
                cached_policy(POLICY_ID, ScopeKind::Composite, Some(composite)),
                cached_policy(POLICY_ID + 1, ScopeKind::Composite, None),
            ],
        };
        let scope = ScopeKey::with_debug_value(ScopeKind::Composite, SCOPE_HASH, composite);

        assert_eq!(
            find_effective_policy(&cache, &scope).map(|policy| policy.policy_id),
            Some(POLICY_ID)
        );
    }

    #[test]
    fn policy_cache_does_not_match_missing_debug_value_to_exact_policy() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: vec![cached_policy(
                POLICY_ID,
                ScopeKind::Tenant,
                Some("tenant-a"),
            )],
        };
        let scope = ScopeKey {
            kind: ScopeKind::Tenant,
            value_hash: SCOPE_HASH,
            debug_value: None,
        };

        assert_eq!(find_effective_policy(&cache, &scope), None);
    }

    #[test]
    fn policy_cache_refresh_interval_bounds_staleness() {
        let cache = PolicyCache {
            refreshed_epoch_ms: 1000,
            policies: Vec::new(),
        };

        assert!(!cache_is_stale(&cache, 1999));
        assert!(cache_is_stale(&cache, 2000));
    }

    fn cached_policy(
        policy_id: PolicyId,
        scope_kind: ScopeKind,
        scope_value: Option<&str>,
    ) -> CachedEffectivePolicy {
        CachedEffectivePolicy {
            scope_kind,
            scope_value: scope_value.map(ToString::to_string),
            policy: EffectivePolicy {
                policy_id,
                enabled: true,
                mode: BudgetMode::Reject,
                wal_rate_bytes_per_sec: 100,
                wal_burst_bytes: 100,
            },
        }
    }
}
