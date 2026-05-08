create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_reject_blocked_test (id integer generated always as identity, value text);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1, 1, 'reject', 100) as reject_policy_id;

\set VERBOSITY sqlstate
insert into pwb_reject_blocked_test (value) values ('blocked');
\set VERBOSITY default

select count(*) as inserted_rows from pwb_reject_blocked_test;

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
  ) as reject_decision_recorded;

select pwb.set_policy_mode(1, 'observe');

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_reject_allowed_test (id integer generated always as identity, value text);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'reject', 100) as reject_policy_id;

insert into pwb_reject_allowed_test (value) values ('alpha'), ('beta');

select count(*) as inserted_rows from pwb_reject_allowed_test;

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
  ) as reject_allowed_recorded;

select
  exists (
    select 1
    from pwb.scope_stats()
    where policy_id = 1
      and available_bytes < max_burst_bytes
      and debt_bytes = 0
  ) as reject_bucket_charged;

select pwb.set_policy_mode(1, 'observe');

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_reject_readonly_test (id integer);
insert into pwb_reject_readonly_test values (1), (2);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1, 1, 'reject', 100) as reject_policy_id;

select count(*) as readonly_rows from pwb_reject_readonly_test;

select
  rejected_statements = 0 as no_rejections,
  shadow_would_reject_count = 0 as no_shadow_rejections
from pwb.counters();

select
  exists (
    select 1
    from pwb.recent_decisions(20)
    where policy_id = 1
      and decision_kind = 'allowed'
      and statement_class = 'read_only'
      and predicted_wal_bytes = 0
  ) as read_only_allowed_recorded;

select pwb.set_policy_mode(1, 'observe');
