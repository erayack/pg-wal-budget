create extension if not exists pg_wal_budget;

do $$
begin
  if not exists (select 1 from pg_roles where rolname = 'pwb_regress_no_tenant') then
    create role pwb_regress_no_tenant;
  end if;
  if not exists (select 1 from pg_roles where rolname = 'pwb_regress_admin') then
    create role pwb_regress_admin;
  end if;
end;
$$;

revoke pwb_tenant_setter from pwb_regress_no_tenant;
revoke pwb_admin from pwb_regress_admin;

set role pwb_regress_no_tenant;
\set VERBOSITY sqlstate
select pwb.set_tenant('tenant-a');
\set VERBOSITY default
reset role;

set role pwb_regress_no_tenant;
do $$
begin
  perform pwb.create_policy('role', current_user, 1000, 2000, 'observe', 10);
  raise exception 'expected create policy to fail';
exception
  when insufficient_privilege then
    raise notice 'create policy rejected';
end;
$$;
do $$
begin
  perform pwb.set_policy_mode(1, 'shadow');
  raise exception 'expected set policy mode to fail';
exception
  when insufficient_privilege then
    raise notice 'set policy mode rejected';
end;
$$;
do $$
begin
  perform pwb.update_policy(1, 1000, 2000);
  raise exception 'expected update policy to fail';
exception
  when insufficient_privilege then
    raise notice 'update policy rejected';
end;
$$;
do $$
begin
  perform pwb.disable_policy(1);
  raise exception 'expected disable policy to fail';
exception
  when insufficient_privilege then
    raise notice 'disable policy rejected';
end;
$$;
do $$
begin
  perform pwb.reset_profiles();
  raise exception 'expected reset profiles to fail';
exception
  when insufficient_privilege then
    raise notice 'reset profiles rejected';
end;
$$;
do $$
begin
  perform pwb.flush_profiles();
  raise exception 'expected flush profiles to fail';
exception
  when insufficient_privilege then
    raise notice 'flush profiles rejected';
end;
$$;
reset role;

grant pwb_tenant_setter to pwb_regress_no_tenant;

set role pwb_regress_no_tenant;
select pwb.set_tenant('tenant-a');
select pwb.clear_tenant();
reset role;

grant pwb_admin to pwb_regress_admin;

set role pwb_regress_admin;
select pwb.create_policy('role', current_user, 1000, 2000, 'observe', 10) is not null as admin_created_policy;
select pwb.set_policy_mode(1, 'shadow');
select pwb.update_policy(1, 2000, 4000, 20) is null as t;
select pwb.disable_policy(1);
select pwb.reset_stats();
select pwb.reset_profiles();
select pwb.set_tenant('tenant-admin');
select pwb.clear_tenant();
reset role;

revoke pwb_tenant_setter from pwb_regress_no_tenant;
revoke pwb_admin from pwb_regress_admin;
