
create table pwb.policy (
  policy_id integer generated always as identity primary key,
  enabled boolean not null default true,
  mode text not null,
  scope_kind text not null,
  scope_value text,
  wal_rate_bytes_per_sec bigint not null,
  wal_burst_bytes bigint not null,
  priority integer not null default 100,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint policy_mode_check
    check (mode in ('off', 'observe', 'shadow', 'reject')),
  constraint policy_scope_kind_check
    check (scope_kind in ('database', 'role', 'application', 'tenant', 'composite')),
  constraint policy_rate_positive_check
    check (wal_rate_bytes_per_sec > 0),
  constraint policy_burst_check
    check (wal_burst_bytes >= wal_rate_bytes_per_sec)
);

create index policy_match_idx
  on pwb.policy (enabled, scope_kind, scope_value, priority desc, policy_id);

create function pwb.touch_policy_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create trigger policy_touch_updated_at
before update on pwb.policy
for each row
execute function pwb.touch_policy_updated_at();

create function pwb.create_policy(
  scope_kind text,
  scope_value text,
  wal_rate_bytes_per_sec bigint,
  wal_burst_bytes bigint,
  mode text default 'observe',
  priority integer default 100
) returns integer
language c
as 'MODULE_PATHNAME', 'pwb_create_policy_wrapper';

create function pwb.set_policy_mode(policy_id integer, mode text)
returns void
language c
as 'MODULE_PATHNAME', 'pwb_set_policy_mode_wrapper';

create function pwb.disable_policy(policy_id integer)
returns void
language c
as 'MODULE_PATHNAME', 'pwb_disable_policy_wrapper';

create function pwb.set_tenant(tenant text)
returns void
language c
as 'MODULE_PATHNAME', 'pwb_set_tenant_wrapper';

create function pwb.clear_tenant()
returns void
language c
as 'MODULE_PATHNAME', 'pwb_clear_tenant_wrapper';

create function pwb.policies()
returns setof pwb.policy
language sql
stable
security definer
set search_path = pwb, pg_temp
as $$
  select *
    from pwb.policy
   order by enabled desc, priority desc, policy_id asc;
$$;

create function pwb.version()
returns text
language c
immutable
parallel safe
as 'MODULE_PATHNAME', 'pg_wal_budget_version_wrapper';

create function pwb.preload_status()
returns text
language c
stable
parallel safe
as 'MODULE_PATHNAME', 'pg_wal_budget_preload_status_wrapper';

create function pwb.counters()
returns table (
  accepted_statements bigint,
  rejected_statements bigint,
  shadow_would_reject_count bigint,
  predicted_wal_bytes bigint,
  actual_wal_bytes bigint,
  absolute_prediction_error bigint,
  scope_debt_bytes bigint,
  missing_actual_wal_count bigint,
  internal_fail_open_count bigint,
  aborted_after_charge_count bigint
)
language c
stable
as 'MODULE_PATHNAME', 'pwb_counters_wrapper';

create function pwb.scope_stats()
returns table (
  policy_id integer,
  scope_hash bigint,
  available_bytes bigint,
  max_burst_bytes bigint,
  rate_bytes_per_sec bigint,
  debt_bytes bigint,
  last_refill_epoch_ms bigint
)
language c
stable
as 'MODULE_PATHNAME', 'pwb_scope_stats_wrapper';

create function pwb.query_profiles()
returns table (
  scope_hash bigint,
  query_id bigint,
  calls bigint,
  ewma_wal_bytes bigint,
  max_wal_bytes bigint,
  last_seen_epoch_ms bigint,
  is_global boolean
)
language c
stable
as 'MODULE_PATHNAME', 'pwb_query_profiles_wrapper';

create function pwb.recent_decisions(decision_limit integer default 100)
returns table (
  timestamp_epoch_ms bigint,
  decision_kind text,
  policy_id integer,
  scope_kind text,
  scope_hash bigint,
  query_id bigint,
  statement_class text,
  predicted_wal_bytes bigint,
  actual_wal_bytes bigint,
  available_before bigint,
  available_after bigint,
  reason_code text
)
language c
stable
as 'MODULE_PATHNAME', 'pwb_recent_decisions_wrapper';

create function pwb.reset_stats()
returns void
language c
as 'MODULE_PATHNAME', 'pwb_reset_stats_wrapper';

create function pwb.reset_profiles()
returns void
language c
as 'MODULE_PATHNAME', 'pwb_reset_profiles_wrapper';

create view pwb.active_policy_precedence as
select *
  from pwb.policy
 where enabled
 order by priority desc, policy_id asc;
