create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_shadow_would_reject_test (id integer generated always as identity, value text);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1, 1, 'shadow', 100) as shadow_policy_id;

insert into pwb_shadow_would_reject_test (value) values ('alpha'), ('beta'), ('gamma');

select count(*) as inserted_rows from pwb_shadow_would_reject_test;

select
  accepted_statements > 0 as accepted_recorded,
  rejected_statements = 0 as no_rejections,
  shadow_would_reject_count > 0 as shadow_rejection_recorded,
  predicted_wal_bytes > 0 as prediction_recorded
from pwb.counters();

select
  exists (
    select 1
    from pwb.recent_decisions(20)
    where policy_id = 1
      and decision_kind = 'would_reject'
      and reason_code = 'budget_exceeded'
      and statement_class = 'write'
      and predicted_wal_bytes > available_before
      and available_before = 1
      and available_after = 1
  ) as shadow_would_reject_recorded;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_shadow_allowed_test (id integer generated always as identity, value text);
select pwb.reset_stats();

select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'shadow', 100) as shadow_policy_id;

insert into pwb_shadow_allowed_test (value) values ('delta'), ('epsilon');

select count(*) as inserted_rows from pwb_shadow_allowed_test;

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
      and reason_code = 'shadow_mode'
      and statement_class = 'write'
      and predicted_wal_bytes > 0
      and predicted_wal_bytes <= available_before
      and available_before = available_after
  ) as shadow_allowed_recorded;
