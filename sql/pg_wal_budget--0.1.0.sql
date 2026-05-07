\echo Use "CREATE EXTENSION pg_wal_budget" to load this file. \quit

create schema if not exists pwb;

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

create view pwb.active_policy_precedence as
select *
  from pwb.policy
 where enabled
 order by priority desc, policy_id asc;
