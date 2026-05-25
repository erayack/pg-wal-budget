create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_queue_allowed_test (id integer generated always as identity, value text);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'queue', 100) as queue_policy_id;

insert into pwb_queue_allowed_test (value) values ('alpha'), ('beta');

select count(*) as inserted_rows from pwb_queue_allowed_test;

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
      and reason_code = 'budget_available'
      and statement_class = 'write'
      and predicted_wal_bytes > 0
      and available_after < available_before
      and available_before - available_after = predicted_wal_bytes
  ) as queue_allowed_recorded;

select pwb.set_policy_mode(1, 'observe');

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_queue_blocked_test (id integer generated always as identity, value text);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1, 1, 'queue', 100) as queue_policy_id;

\set VERBOSITY sqlstate
insert into pwb_queue_blocked_test (value) values ('blocked');
\set VERBOSITY default

select count(*) as inserted_rows from pwb_queue_blocked_test;

select
  rejected_statements > 0 as rejection_recorded,
  shadow_would_reject_count = 0 as no_shadow_rejections,
  predicted_wal_bytes > 0 as prediction_recorded
from pwb.counters();

select
  exists (
    select 1
    from pwb.recent_decisions(20)
    where policy_id = 1
      and decision_kind = 'rejected'
      and reason_code = 'budget_exceeded'
      and statement_class = 'write'
      and predicted_wal_bytes > available_before
      and available_before = available_after
  ) as queue_rejects_impossible_statement;

select pwb.set_policy_mode(1, 'observe');
