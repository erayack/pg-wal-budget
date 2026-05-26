create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_fail_open_context_test (
  id integer generated always as identity,
  value text
);

select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'reject', 100) as reject_policy_id;

insert into pwb_fail_open_context_test (value) values ('fail-open-context');

select count(*) as inserted_rows from pwb_fail_open_context_test;

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
      and scope_kind = 'role'
      and scope_hash <> 0
      and query_id is not null
      and statement_class = 'write'
      and predicted_wal_bytes > 0
      and actual_wal_bytes is null
      and available_before = 0
      and available_after = 0
  ) as fail_open_context_recorded;

select pwb.set_policy_mode(1, 'observe');
