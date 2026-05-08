create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

select pwb.create_policy('role', current_user, 1, 1, 'observe', 100) as observe_policy_id;

create temp table pwb_utility_copy_test (id integer, value text);

copy pwb_utility_copy_test (id, value) from stdin;
1	alpha
2	beta
3	gamma
\.

select count(*) as copied_rows from pwb_utility_copy_test;

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
      and statement_class = 'copy'
      and predicted_wal_bytes > 0
      and available_before = 0
      and available_after = 0
  ) as copy_decision_recorded;

select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_utility_index_test (id integer, value text);
insert into pwb_utility_index_test
select i, 'value-' || i::text
from generate_series(1, 10) as i;

select pwb.reset_stats();
select pwb.reset_profiles();

create index pwb_utility_index_test_value_idx on pwb_utility_index_test (value);

select to_regclass('pg_temp.pwb_utility_index_test_value_idx') is not null as index_created;

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
      and statement_class = 'utility'
      and predicted_wal_bytes > 0
      and available_before = 0
      and available_after = 0
  ) as index_decision_recorded;
