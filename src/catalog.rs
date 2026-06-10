use pgrx::datum::DatumWithOid;
use pgrx::{Spi, spi};

use crate::errors::{PwbError, PwbResult};
use crate::hooks;
use crate::shmem::QueryProfileSnapshot;
use crate::types::{PolicyId, QueryWalProfile, ScopeHash};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DurablePolicyRow {
    pub(crate) policy_id: PolicyId,
    pub(crate) scope_kind: String,
    pub(crate) scope_value: Option<String>,
    pub(crate) enabled: bool,
    pub(crate) mode: String,
    pub(crate) wal_rate_bytes_per_sec: i64,
    pub(crate) wal_burst_bytes: i64,
}

pub(crate) trait DurableCatalogStore {
    fn load_enabled_policy_rows(&self) -> PwbResult<Vec<DurablePolicyRow>>;
    fn load_profiles(&self, limit: usize) -> PwbResult<Vec<QueryProfileSnapshot>>;
    fn persist_profiles(&self, profiles: &[QueryProfileSnapshot]) -> PwbResult<()>;
    fn delete_profiles(&self) -> PwbResult<()>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct SpiCatalogStore;

impl DurableCatalogStore for SpiCatalogStore {
    fn load_enabled_policy_rows(&self) -> PwbResult<Vec<DurablePolicyRow>> {
        hooks::with_admission_bypass(|| {
            if !table_exists("pwb.policy", "policy")? {
                return Ok(Vec::new());
            }

            Spi::connect(|client| -> PwbResult<Vec<DurablePolicyRow>> {
                let table = client
                    .select(
                        "
                        select
                          policy_id,
                          scope_kind,
                          scope_value,
                          enabled,
                          mode,
                          wal_rate_bytes_per_sec,
                          wal_burst_bytes
                        from pwb.policy
                        where enabled = true
                        order by priority desc, policy_id asc
                        ",
                        None,
                        &[],
                    )
                    .map_err(policy_spi_error)?;

                let mut policies = Vec::with_capacity(table.len());
                for row in table {
                    policies.push(DurablePolicyRow {
                        policy_id: required_policy_field(
                            "policy_id",
                            row.get_by_name::<PolicyId, _>("policy_id"),
                        )?,
                        scope_kind: required_policy_field(
                            "scope_kind",
                            row.get_by_name::<String, _>("scope_kind"),
                        )?,
                        scope_value: row
                            .get_by_name::<String, _>("scope_value")
                            .map_err(policy_spi_error)?,
                        enabled: required_policy_field(
                            "enabled",
                            row.get_by_name::<bool, _>("enabled"),
                        )?,
                        mode: required_policy_field("mode", row.get_by_name::<String, _>("mode"))?,
                        wal_rate_bytes_per_sec: required_policy_field(
                            "wal_rate_bytes_per_sec",
                            row.get_by_name::<i64, _>("wal_rate_bytes_per_sec"),
                        )?,
                        wal_burst_bytes: required_policy_field(
                            "wal_burst_bytes",
                            row.get_by_name::<i64, _>("wal_burst_bytes"),
                        )?,
                    });
                }
                Ok(policies)
            })
        })
    }

    fn load_profiles(&self, limit: usize) -> PwbResult<Vec<QueryProfileSnapshot>> {
        hooks::with_admission_bypass(|| {
            if !table_exists("pwb.query_profile", "query profile")? {
                return Ok(Vec::new());
            }

            let limit = i64::try_from(limit).map_err(|_| PwbError::Internal {
                message: format!("profile cache capacity does not fit PostgreSQL bigint: {limit}"),
            })?;

            let raw_profiles = Spi::connect(|client| {
                let table = client.select(
                    "
                    select
                      scope_hash::text as scope_hash,
                      query_id::text as query_id,
                      calls::text as calls,
                      ewma_wal_bytes::text as ewma_wal_bytes,
                      max_wal_bytes::text as max_wal_bytes,
                      last_seen_epoch_ms::text as last_seen_epoch_ms
                    from pwb.query_profile
                    order by last_seen_epoch_ms desc, query_id asc, scope_hash asc nulls first
                    limit $1
                    ",
                    None,
                    &[limit.into()],
                )?;

                let mut profiles = Vec::with_capacity(table.len());
                for row in table {
                    profiles.push(RawQueryProfile {
                        scope_hash: row.get_by_name::<String, _>("scope_hash")?,
                        query_id: row.get_by_name::<String, _>("query_id")?,
                        calls: row.get_by_name::<String, _>("calls")?,
                        ewma_wal_bytes: row.get_by_name::<String, _>("ewma_wal_bytes")?,
                        max_wal_bytes: row.get_by_name::<String, _>("max_wal_bytes")?,
                        last_seen_epoch_ms: row.get_by_name::<String, _>("last_seen_epoch_ms")?,
                    });
                }
                Ok(profiles)
            })
            .map_err(profile_spi_error)?;

            raw_profiles.iter().map(decode_profile).collect()
        })
    }

    fn persist_profiles(&self, profiles: &[QueryProfileSnapshot]) -> PwbResult<()> {
        hooks::with_admission_bypass(|| {
            if !table_exists("pwb.query_profile", "query profile")? {
                return Ok(());
            }

            for profile in profiles {
                let sql_profile = SqlQueryProfile::from_snapshot(*profile);
                Spi::get_one_with_args::<bool>(
                    "
                    insert into pwb.query_profile (
                      scope_hash,
                      query_id,
                      calls,
                      ewma_wal_bytes,
                      max_wal_bytes,
                      last_seen_epoch_ms
                    )
                    values (
                      $1::numeric(20, 0),
                      $2::numeric(20, 0),
                      $3::numeric(20, 0),
                      $4::numeric(20, 0),
                      $5::numeric(20, 0),
                      $6::numeric(20, 0)
                    )
                    on conflict on constraint query_profile_key
                    do update set
                      calls = excluded.calls,
                      ewma_wal_bytes = excluded.ewma_wal_bytes,
                      max_wal_bytes = excluded.max_wal_bytes,
                      last_seen_epoch_ms = excluded.last_seen_epoch_ms,
                      updated_at = now()
                    returning true
                    ",
                    &[
                        nullable_text_arg(sql_profile.scope_hash.as_deref()),
                        sql_profile.query_id.into(),
                        sql_profile.calls.into(),
                        sql_profile.ewma_wal_bytes.into(),
                        sql_profile.max_wal_bytes.into(),
                        sql_profile.last_seen_epoch_ms.into(),
                    ],
                )
                .map_err(profile_spi_error)?;
            }

            Ok(())
        })
    }

    fn delete_profiles(&self) -> PwbResult<()> {
        hooks::with_admission_bypass(|| {
            if !table_exists("pwb.query_profile", "query profile")? {
                return Ok(());
            }

            Spi::get_one::<bool>(
                "with deleted as (delete from pwb.query_profile returning true) select true",
            )
            .map_err(profile_spi_error)?;
            Ok(())
        })
    }
}

fn table_exists(regclass: &'static str, operation: &'static str) -> PwbResult<bool> {
    Spi::get_one_with_args::<bool>("select to_regclass($1) is not null", &[regclass.into()])
        .map(|exists| exists.unwrap_or(false))
        .map_err(|error| catalog_spi_error(operation, error))
}

fn required_policy_field<T>(field: &'static str, value: spi::SpiResult<Option<T>>) -> PwbResult<T> {
    value
        .map_err(policy_spi_error)?
        .ok_or_else(|| PwbError::Internal {
            message: format!("effective policy row is missing {field}"),
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawQueryProfile {
    scope_hash: Option<String>,
    query_id: Option<String>,
    calls: Option<String>,
    ewma_wal_bytes: Option<String>,
    max_wal_bytes: Option<String>,
    last_seen_epoch_ms: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SqlQueryProfile {
    scope_hash: Option<String>,
    query_id: String,
    calls: String,
    ewma_wal_bytes: String,
    max_wal_bytes: String,
    last_seen_epoch_ms: String,
}

impl SqlQueryProfile {
    fn from_snapshot(snapshot: QueryProfileSnapshot) -> Self {
        Self {
            scope_hash: snapshot.scope_hash.map(|scope_hash| scope_hash.to_string()),
            query_id: snapshot.query_id.to_string(),
            calls: snapshot.profile.calls.to_string(),
            ewma_wal_bytes: snapshot.profile.ewma_wal_bytes.to_string(),
            max_wal_bytes: snapshot.profile.max_wal_bytes.to_string(),
            last_seen_epoch_ms: snapshot.profile.last_seen_epoch_ms.to_string(),
        }
    }
}

fn decode_profile(raw: &RawQueryProfile) -> PwbResult<QueryProfileSnapshot> {
    let scope_hash = raw
        .scope_hash
        .as_deref()
        .map(|scope_hash| parse_u64("scope_hash", scope_hash))
        .transpose()?
        .map(|scope_hash| scope_hash as ScopeHash);

    Ok(QueryProfileSnapshot {
        scope_hash,
        query_id: required_parse_u64("query_id", raw.query_id.as_deref())?,
        profile: QueryWalProfile {
            calls: required_parse_u64("calls", raw.calls.as_deref())?,
            ewma_wal_bytes: required_parse_u64("ewma_wal_bytes", raw.ewma_wal_bytes.as_deref())?,
            max_wal_bytes: required_parse_u64("max_wal_bytes", raw.max_wal_bytes.as_deref())?,
            last_seen_epoch_ms: required_parse_u64(
                "last_seen_epoch_ms",
                raw.last_seen_epoch_ms.as_deref(),
            )?,
        },
    })
}

fn required_parse_u64(field: &'static str, value: Option<&str>) -> PwbResult<u64> {
    let value = value.ok_or_else(|| PwbError::Internal {
        message: format!("durable query profile row is missing {field}"),
    })?;
    parse_u64(field, value)
}

fn parse_u64(field: &'static str, value: &str) -> PwbResult<u64> {
    value.parse::<u64>().map_err(|_| PwbError::Internal {
        message: format!("durable query profile field {field} is not a valid u64: {value}"),
    })
}

fn nullable_text_arg(value: Option<&str>) -> DatumWithOid<'_> {
    value.map_or_else(DatumWithOid::null::<String>, DatumWithOid::from)
}

#[allow(clippy::needless_pass_by_value)]
fn policy_spi_error(error: spi::Error) -> PwbError {
    catalog_spi_error("policy", error)
}

#[allow(clippy::needless_pass_by_value)]
fn profile_spi_error(error: spi::Error) -> PwbError {
    catalog_spi_error("query profile", error)
}

#[allow(clippy::needless_pass_by_value)]
fn catalog_spi_error(operation: &'static str, error: spi::Error) -> PwbError {
    PwbError::Internal {
        message: format!("SPI {operation} operation failed: {error}"),
    }
}

#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub(crate) struct MemoryCatalogStore {
    policy_rows: Vec<DurablePolicyRow>,
    profiles: std::cell::RefCell<Vec<QueryProfileSnapshot>>,
    tables_exist: bool,
}

#[cfg(test)]
impl MemoryCatalogStore {
    pub(crate) fn with_rows(
        policy_rows: Vec<DurablePolicyRow>,
        profiles: Vec<QueryProfileSnapshot>,
    ) -> Self {
        Self {
            policy_rows,
            profiles: std::cell::RefCell::new(profiles),
            tables_exist: true,
        }
    }

    pub(crate) const fn missing_tables() -> Self {
        Self {
            policy_rows: Vec::new(),
            profiles: std::cell::RefCell::new(Vec::new()),
            tables_exist: false,
        }
    }
}

#[cfg(test)]
impl DurableCatalogStore for MemoryCatalogStore {
    fn load_enabled_policy_rows(&self) -> PwbResult<Vec<DurablePolicyRow>> {
        if self.tables_exist {
            Ok(self
                .policy_rows
                .iter()
                .filter(|row| row.enabled)
                .cloned()
                .collect())
        } else {
            Ok(Vec::new())
        }
    }

    fn load_profiles(&self, limit: usize) -> PwbResult<Vec<QueryProfileSnapshot>> {
        if !self.tables_exist {
            return Ok(Vec::new());
        }
        let mut profiles = self.profiles.borrow().clone();
        profiles.sort_by(|left, right| {
            right
                .profile
                .last_seen_epoch_ms
                .cmp(&left.profile.last_seen_epoch_ms)
                .then_with(|| left.query_id.cmp(&right.query_id))
                .then_with(|| left.scope_hash.cmp(&right.scope_hash))
        });
        Ok(profiles.into_iter().take(limit).collect())
    }

    fn persist_profiles(&self, profiles: &[QueryProfileSnapshot]) -> PwbResult<()> {
        if !self.tables_exist {
            return Ok(());
        }
        let mut stored = self.profiles.borrow_mut();
        for profile in profiles {
            if let Some(existing) = stored.iter_mut().find(|existing| {
                existing.scope_hash == profile.scope_hash && existing.query_id == profile.query_id
            }) {
                *existing = *profile;
            } else {
                stored.push(*profile);
            }
        }
        Ok(())
    }

    fn delete_profiles(&self) -> PwbResult<()> {
        if self.tables_exist {
            self.profiles.borrow_mut().clear();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_global_profile() {
        let profile = decode_profile(&RawQueryProfile {
            scope_hash: None,
            query_id: Some("42".to_string()),
            calls: Some("2".to_string()),
            ewma_wal_bytes: Some("1024".to_string()),
            max_wal_bytes: Some("2048".to_string()),
            last_seen_epoch_ms: Some("1234".to_string()),
        })
        .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(profile.scope_hash, None);
        assert_eq!(profile.query_id, 42);
        assert_eq!(profile.profile.calls, 2);
    }

    #[test]
    fn rejects_negative_profile_values() {
        let result = decode_profile(&RawQueryProfile {
            scope_hash: None,
            query_id: Some("-1".to_string()),
            calls: Some("2".to_string()),
            ewma_wal_bytes: Some("1024".to_string()),
            max_wal_bytes: Some("2048".to_string()),
            last_seen_epoch_ms: Some("1234".to_string()),
        });
        let error = match result {
            Ok(profile) => panic!("negative query_id should be rejected, got {profile:?}"),
            Err(error) => error,
        };

        assert!(matches!(error, PwbError::Internal { .. }));
    }

    #[test]
    fn memory_store_persists_and_deletes_profiles() {
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

        store
            .persist_profiles(&[snapshot])
            .unwrap_or_else(|error| panic!("{error}"));
        let loaded = store
            .load_profiles(10)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(loaded, vec![snapshot]);
        store
            .delete_profiles()
            .unwrap_or_else(|error| panic!("{error}"));
        let loaded = store
            .load_profiles(10)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(loaded.is_empty());
    }

    #[test]
    fn memory_store_orders_profiles_like_production_before_limiting() {
        let older = QueryProfileSnapshot {
            scope_hash: Some(9),
            query_id: 2,
            profile: QueryWalProfile {
                calls: 1,
                ewma_wal_bytes: 10,
                max_wal_bytes: 10,
                last_seen_epoch_ms: 100,
            },
        };
        let latest_query_two_null_scope = QueryProfileSnapshot {
            scope_hash: None,
            query_id: 2,
            profile: QueryWalProfile {
                calls: 1,
                ewma_wal_bytes: 10,
                max_wal_bytes: 10,
                last_seen_epoch_ms: 200,
            },
        };
        let latest_query_one = QueryProfileSnapshot {
            scope_hash: Some(7),
            query_id: 1,
            profile: QueryWalProfile {
                calls: 1,
                ewma_wal_bytes: 10,
                max_wal_bytes: 10,
                last_seen_epoch_ms: 200,
            },
        };
        let latest_query_two_scoped = QueryProfileSnapshot {
            scope_hash: Some(5),
            query_id: 2,
            profile: QueryWalProfile {
                calls: 1,
                ewma_wal_bytes: 10,
                max_wal_bytes: 10,
                last_seen_epoch_ms: 200,
            },
        };
        let store = MemoryCatalogStore::with_rows(
            Vec::new(),
            vec![
                older,
                latest_query_two_scoped,
                latest_query_two_null_scope,
                latest_query_one,
            ],
        );

        let loaded = store
            .load_profiles(3)
            .unwrap_or_else(|error| panic!("{error}"));

        assert_eq!(
            loaded,
            vec![
                latest_query_one,
                latest_query_two_null_scope,
                latest_query_two_scoped
            ]
        );
    }

    #[test]
    fn memory_store_missing_tables_are_empty_and_noop() {
        let store = MemoryCatalogStore::missing_tables();
        let policies = store
            .load_enabled_policy_rows()
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(policies.is_empty());
        let profiles = store
            .load_profiles(10)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(profiles.is_empty());
        store
            .persist_profiles(&[])
            .unwrap_or_else(|error| panic!("{error}"));
        store
            .delete_profiles()
            .unwrap_or_else(|error| panic!("{error}"));
    }
}
