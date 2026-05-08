create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1, 1, 'observe', 100) as observe_policy_id;

create temp table pwb_observe_test (id integer generated always as identity, value text);
insert into pwb_observe_test (value) values ('alpha'), ('beta'), ('gamma');

select count(*) as inserted_rows from pwb_observe_test;

select
  accepted_statements > 0 as accepted_recorded,
  rejected_statements = 0 as no_rejections,
  shadow_would_reject_count = 0 as no_shadow_rejections,
  predicted_wal_bytes > 0 as prediction_recorded
from pwb.counters();

select
  exists (
    select 1
    from pwb.recent_decisions(20)
    where policy_id = 1
      and decision_kind = 'allowed'
      and reason_code = 'observe_mode'
      and statement_class = 'write'
      and predicted_wal_bytes > 0
      and available_before = 0
      and available_after = 0
  ) as observe_decision_recorded;
