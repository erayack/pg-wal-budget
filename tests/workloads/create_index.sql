\echo workload: create_index
create extension if not exists pg_wal_budget;
truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();
select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'observe', 100);
create temp table pwb_cal_create_index (id integer, value text);
insert into pwb_cal_create_index
select i, 'value-' || i::text
from generate_series(1, 5000) as i;
select pwb.reset_stats();
create index pwb_cal_create_index_value_idx on pwb_cal_create_index (value);
select
  'create_index' as workload_name,
  accepted_statements + rejected_statements as statements,
  predicted_wal_bytes,
  actual_wal_bytes,
  absolute_prediction_error,
  case when actual_wal_bytes = 0 then null else absolute_prediction_error::numeric / actual_wal_bytes end as error_ratio
from pwb.counters();
