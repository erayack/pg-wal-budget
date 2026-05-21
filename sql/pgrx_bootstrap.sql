
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

create table pwb.query_profile (
  scope_hash numeric(20, 0),
  query_id numeric(20, 0) not null,
  calls numeric(20, 0) not null,
  ewma_wal_bytes numeric(20, 0) not null,
  max_wal_bytes numeric(20, 0) not null,
  last_seen_epoch_ms numeric(20, 0) not null,
  updated_at timestamptz not null default now(),
  constraint query_profile_key unique nulls not distinct (query_id, scope_hash),
  constraint query_profile_scope_hash_nonnegative_check
    check (scope_hash is null or scope_hash >= 0),
  constraint query_profile_calls_positive_check
    check (calls > 0),
  constraint query_profile_ewma_nonnegative_check
    check (ewma_wal_bytes >= 0),
  constraint query_profile_max_nonnegative_check
    check (max_wal_bytes >= 0),
  constraint query_profile_last_seen_nonnegative_check
    check (last_seen_epoch_ms >= 0)
);

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
security definer
as 'MODULE_PATHNAME', 'pwb_create_policy_wrapper';

create function pwb.set_policy_mode(policy_id integer, mode text)
returns void
language c
security definer
as 'MODULE_PATHNAME', 'pwb_set_policy_mode_wrapper';

create function pwb.disable_policy(policy_id integer)
returns void
language c
security definer
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

-- Durable profile maintenance uses SECURITY DEFINER for table access; the C wrappers still
-- enforce pwb_admin membership before mutating pwb.query_profile.
create function pwb.reset_profiles()
returns void
language c
security definer
as 'MODULE_PATHNAME', 'pwb_reset_profiles_wrapper';

create function pwb.flush_profiles()
returns void
language c
security definer
as 'MODULE_PATHNAME', 'pwb_flush_profiles_wrapper';

create view pwb.active_policy_precedence as
select *
  from pwb.policy
 where enabled
 order by priority desc, policy_id asc;

do $$
declare
  role_marker constant text = 'pg_wal_budget extension managed role';
begin
  if not exists (select 1 from pg_roles where rolname = 'pwb_admin') then
    create role pwb_admin;
    comment on role pwb_admin is 'pg_wal_budget extension managed role';
  elsif coalesce(shobj_description('pwb_admin'::regrole, 'pg_authid'), '') <> role_marker then
    raise exception 'role "pwb_admin" already exists and is not managed by pg_wal_budget';
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pwb_monitor') then
    create role pwb_monitor;
    comment on role pwb_monitor is 'pg_wal_budget extension managed role';
  elsif coalesce(shobj_description('pwb_monitor'::regrole, 'pg_authid'), '') <> role_marker then
    raise exception 'role "pwb_monitor" already exists and is not managed by pg_wal_budget';
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pwb_tenant_setter') then
    create role pwb_tenant_setter;
    comment on role pwb_tenant_setter is 'pg_wal_budget extension managed role';
  elsif coalesce(shobj_description('pwb_tenant_setter'::regrole, 'pg_authid'), '') <> role_marker then
    raise exception 'role "pwb_tenant_setter" already exists and is not managed by pg_wal_budget';
  end if;
end;
$$;

revoke all on schema pwb from public;
grant usage on schema pwb to public;
grant usage on schema pwb to pwb_admin, pwb_monitor, pwb_tenant_setter;

revoke all on table pwb.policy from public;
grant select on table pwb.policy to pwb_admin, pwb_monitor;

revoke all on table pwb.query_profile from public;
grant select on table pwb.query_profile to pwb_admin, pwb_monitor;

revoke all on pwb.active_policy_precedence from public;
grant select on pwb.active_policy_precedence to pwb_admin, pwb_monitor;

revoke all on function pwb.create_policy(text, text, bigint, bigint, text, integer) from public;
revoke all on function pwb.set_policy_mode(integer, text) from public;
revoke all on function pwb.disable_policy(integer) from public;
grant execute on function pwb.create_policy(text, text, bigint, bigint, text, integer) to pwb_admin;
grant execute on function pwb.set_policy_mode(integer, text) to pwb_admin;
grant execute on function pwb.disable_policy(integer) to pwb_admin;

revoke all on function pwb.set_tenant(text) from public;
revoke all on function pwb.clear_tenant() from public;
grant execute on function pwb.set_tenant(text) to pwb_admin, pwb_tenant_setter;
grant execute on function pwb.clear_tenant() to pwb_admin, pwb_tenant_setter;

revoke all on function pwb.policies() from public;
revoke all on function pwb.counters() from public;
revoke all on function pwb.scope_stats() from public;
revoke all on function pwb.query_profiles() from public;
revoke all on function pwb.recent_decisions(integer) from public;
grant execute on function pwb.policies() to pwb_admin, pwb_monitor;
grant execute on function pwb.counters() to pwb_admin, pwb_monitor;
grant execute on function pwb.scope_stats() to pwb_admin, pwb_monitor;
grant execute on function pwb.query_profiles() to pwb_admin, pwb_monitor;
grant execute on function pwb.recent_decisions(integer) to pwb_admin, pwb_monitor;

revoke all on function pwb.reset_stats() from public;
revoke all on function pwb.reset_profiles() from public;
revoke all on function pwb.flush_profiles() from public;
grant execute on function pwb.reset_stats() to pwb_admin;
grant execute on function pwb.reset_profiles() to pwb_admin;
grant execute on function pwb.flush_profiles() to pwb_admin;

grant execute on function pwb.version() to public;
grant execute on function pwb.preload_status() to public;
