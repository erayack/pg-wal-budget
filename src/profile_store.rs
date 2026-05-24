use pgrx::datum::DatumWithOid;
use pgrx::{Spi, spi};

use crate::errors::{PwbError, PwbResult};
use crate::hooks;
use crate::shmem::QueryProfileSnapshot;
use crate::types::{QueryWalProfile, ScopeHash};

pub(crate) fn load_profiles(limit: usize) -> PwbResult<Vec<QueryProfileSnapshot>> {
    hooks::with_admission_bypass(|| {
        if !query_profile_table_exists()? {
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
        .map_err(spi_error)?;

        raw_profiles.iter().map(decode_profile).collect()
    })
}

pub(crate) fn persist_profiles(profiles: &[QueryProfileSnapshot]) -> PwbResult<()> {
    hooks::with_admission_bypass(|| {
        if !query_profile_table_exists()? {
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
            .map_err(spi_error)?;
        }

        Ok(())
    })
}

pub(crate) fn delete_profiles() -> PwbResult<()> {
    hooks::with_admission_bypass(|| {
        if !query_profile_table_exists()? {
            return Ok(());
        }

        Spi::get_one::<bool>(
            "with deleted as (delete from pwb.query_profile returning true) select true",
        )
        .map_err(spi_error)?;
        Ok(())
    })
}

fn query_profile_table_exists() -> PwbResult<bool> {
    Spi::get_one::<bool>("select to_regclass('pwb.query_profile') is not null")
        .map(|exists| exists.unwrap_or(false))
        .map_err(spi_error)
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
fn spi_error(error: spi::Error) -> PwbError {
    PwbError::Internal {
        message: format!("SPI query profile operation failed: {error}"),
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

        assert!(error.to_string().contains("query_id"));
    }

    #[test]
    fn converts_snapshot_to_sql_profile() {
        let sql_profile = SqlQueryProfile::from_snapshot(QueryProfileSnapshot {
            scope_hash: Some(99),
            query_id: 42,
            profile: QueryWalProfile {
                calls: 2,
                ewma_wal_bytes: 1024,
                max_wal_bytes: 2048,
                last_seen_epoch_ms: 1234,
            },
        });

        assert_eq!(sql_profile.scope_hash, Some("99".to_string()));
        assert_eq!(sql_profile.query_id, "42");
        assert_eq!(sql_profile.calls, "2");
    }
}
