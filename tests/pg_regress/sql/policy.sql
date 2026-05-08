create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;

select pwb.create_policy('role', null, 1000, 2000) as created_policy_id;
select pwb.create_policy(' DATABASE ', ' postgres ', 2000, 4000, ' SHADOW ', 250) as created_policy_id;

select
  policy_id,
  enabled,
  mode,
  scope_kind,
  scope_value,
  wal_rate_bytes_per_sec,
  wal_burst_bytes,
  priority
from pwb.policies()
order by policy_id;

select pwb.set_policy_mode(1, 'reject');
select policy_id, mode from pwb.policy where policy_id = 1;

select pwb.disable_policy(2);
select policy_id, enabled from pwb.policy where policy_id = 2;

select
  policy_id,
  enabled,
  priority
from pwb.active_policy_precedence;

select pwb.set_policy_mode(1, 'observe');

do $$
begin
  perform pwb.create_policy('role', null, 1000, 2000, 'invalid-mode');
  raise exception 'expected invalid mode to fail';
exception
  when invalid_parameter_value then
    raise notice 'invalid mode rejected';
end;
$$;

do $$
begin
  perform pwb.create_policy('invalid-scope', null, 1000, 2000);
  raise exception 'expected invalid scope to fail';
exception
  when invalid_parameter_value then
    raise notice 'invalid scope rejected';
end;
$$;

do $$
begin
  perform pwb.create_policy('role', null, 0, 2000);
  raise exception 'expected zero rate to fail';
exception
  when invalid_parameter_value then
    raise notice 'zero rate rejected';
end;
$$;

do $$
begin
  perform pwb.create_policy('role', null, 2000, 1000);
  raise exception 'expected burst below rate to fail';
exception
  when invalid_parameter_value then
    raise notice 'burst below rate rejected';
end;
$$;

do $$
begin
  perform pwb.set_policy_mode(999, 'observe');
  raise exception 'expected missing policy to fail';
exception
  when invalid_parameter_value then
    raise notice 'missing policy rejected';
end;
$$;
