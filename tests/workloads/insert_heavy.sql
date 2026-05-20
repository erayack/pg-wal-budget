\echo workload: insert_heavy
create extension if not exists pg_wal_budget;
truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();
select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'observe', 100);
create temp table pwb_cal_insert_heavy (id integer generated always as identity, value text);
insert into pwb_cal_insert_heavy (value)
select 'insert-heavy-' || i::text
from generate_series(1, 1000) as i;
select
  'insert_heavy' as workload_name,
  accepted_statements + rejected_statements as statements,
  predicted_wal_bytes,
  actual_wal_bytes,
  absolute_prediction_error,
  case when actual_wal_bytes = 0 then null else absolute_prediction_error::numeric / actual_wal_bytes end as error_ratio
from pwb.counters();
