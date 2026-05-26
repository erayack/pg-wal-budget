create extension if not exists pg_wal_budget;

set compute_query_id = on;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_fail_open_context_test (
  id integer generated always as identity,
  value text
);

select pwb.create_policy('tenant', null, 1073741824, 1073741824, 'reject', 100) as reject_policy_id;

do $$
declare
  tenant_index integer;
begin
  for tenant_index in 1..4096 loop
    perform pwb.set_tenant('fail-open-fill-' || tenant_index::text);
    execute format(
      'insert into pwb_fail_open_context_test (value) values (%L)',
      'fill-' || tenant_index::text
    );
  end loop;
end;
$$;

select pwb.set_tenant('fail-open-overflow');
select pwb.reset_stats();
select pwb.reset_profiles();
insert into pwb_fail_open_context_test (value) values ('fail-open-context');

select count(*) > 4096 as inserted_rows_recorded from pwb_fail_open_context_test;

select
  accepted_statements > 0 as accepted_recorded,
  internal_fail_open_count > 0 as fail_open_recorded,
  predicted_wal_bytes > 0 as prediction_recorded
from pwb.counters();

select
  exists (
    select 1
    from pwb.recent_decisions(20)
    where decision_kind = 'internal_error_fail_open'
      and reason_code = 'internal_error_fail_open'
      and policy_id = 1
      and scope_kind = 'tenant'
      and scope_hash <> 0
      and query_id is not null
      and statement_class = 'write'
      and predicted_wal_bytes > 0
      and actual_wal_bytes is null
      and available_before = 0
      and available_after = 0
  ) as fail_open_context_recorded;

select pwb.clear_tenant();
select pwb.set_policy_mode(1, 'observe');
