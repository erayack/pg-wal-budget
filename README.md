# pg_wal_budget

A Rust/pgrx PostgreSQL 17 extension that observes, predicts, and optionally enforces WAL-generation budgets by policy scope.

Hooks (executor + utility) and shared-memory state are installed from `_PG_init`. SQL functions live in the `pwb` schema. MVP status: observe / shadow / reject modes, durable policies, scope classification by tenant → role → database → `application_name`, and exact backend-local WAL telemetry where PostgreSQL exposes `pgWalUsage`. Unsupported targets fall back to approximate insert-LSN telemetry. No queue/wait mode, no cross-node coordination, no replica-lag throttling yet.

## Install

`pg_wal_budget` requires preloading; `CREATE EXTENSION` alone only installs SQL objects.

```conf
# postgresql.conf
shared_preload_libraries = 'pg_wal_budget'
```

Restart PostgreSQL, then:

```sql
create extension pg_wal_budget;
select pwb.preload_status();  -- expect: preloaded
```

For local pgrx development:

```sh
cargo pgrx run pg17 --postgresql-conf shared_preload_libraries=pg_wal_budget
```

## Quickstart

Create an observe-mode policy and inspect telemetry:

```sql
select pwb.create_policy(
  scope_kind => 'role',
  scope_value => current_user,
  wal_rate_bytes_per_sec => 1048576,
  wal_burst_bytes => 8388608,
  mode => 'observe',
  priority => 100
);

select * from pwb.counters();
select * from pwb.scope_stats();
select * from pwb.recent_decisions(100) order by timestamp_epoch_ms desc;
```

Promote a policy through the rollout stages (`observe` → `shadow` → `reject`) once predictions look stable:

```sql
select pwb.set_policy_mode(1, 'shadow');
select pwb.set_policy_mode(1, 'reject');
select pwb.disable_policy(1);
```

## Policies

- Modes: `off`, `observe`, `shadow`, `reject`.
- Scope kinds: `tenant`, `role`, `database`, `application`, `composite`.
- Matching: highest `priority` wins; ties resolved by lowest `policy_id`.
- Tenant scope is trusted backend-local state; set via `pwb.set_tenant(...)` / `pwb.clear_tenant()`. Restricted to superusers and members of `pwb_tenant_setter`.

## Configuration

Runtime GUCs (SIGHUP):

| Setting | Default | Purpose |
| --- | ---: | --- |
| `pwb.enabled` | `on` | Enable admission hooks and accounting. |
| `pwb.fail_open` | `on` | Allow on internal classification/prediction/accounting failure. |
| `pwb.default_write_wal_bytes` | `16kB` | Fallback prediction for writes. |
| `pwb.default_utility_wal_bytes` | `1MB` | Fallback prediction for utility / `COPY`. |
| `pwb.max_prediction_bytes` | `1GB` | Upper bound on predictions. |

Postmaster GUCs (restart required, sized into shared memory):

| Setting | Default | Purpose |
| --- | ---: | --- |
| `pwb.recent_decision_capacity` | `1024` | Recent-decision ring buffer. |
| `pwb.profile_cache_capacity` | `4096` | Query WAL profile cache. |

Emergency disable:

```sql
alter system set pwb.enabled = off;
select pg_reload_conf();
```

To fully unload hooks and shared memory, remove `pg_wal_budget` from `shared_preload_libraries` and restart.

## Observability

```sql
select * from pwb.counters();
select * from pwb.scope_stats();
select * from pwb.query_profiles();
select * from pwb.recent_decisions(100);

select pwb.reset_stats();      -- superuser
select pwb.reset_profiles();   -- superuser
```

Recent decisions expose query hashes, query IDs, and workload classifications; treat as operational telemetry and restrict access in production.

## Caveats

- Exact per-backend WAL measurements are used for query profile updates and budget refund/debt reconciliation.
- If the target PostgreSQL/pgrx binding does not expose backend-local WAL usage, the extension falls back to insert-LSN deltas. That fallback is approximate, can include WAL from other backend activity, and is not used to refund or charge enforcement buckets.
- Query profiles only update when an exact backend WAL measurement is available; fallback predictions stay important on unsupported targets.
- Shared-memory state is disposable and resets on PostgreSQL restart.
- Reject mode can produce false positives until predictions and fallback GUCs are tuned.
- Managed PostgreSQL providers often disallow custom native extensions and `shared_preload_libraries`.

## Build

```sh
cargo check --no-default-features --features 'pg17 pg_test'
cargo pgrx regress pg17 --resetdb --postgresql-conf shared_preload_libraries=pg_wal_budget
cargo fmt --all
```
