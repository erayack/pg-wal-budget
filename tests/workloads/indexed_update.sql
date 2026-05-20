\echo workload: indexed_update
create extension if not exists pg_wal_budget;
truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();
select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'observe', 100);
create temp table pwb_cal_indexed_update (id integer primary key, indexed_value integer, filler text);
create index on pwb_cal_indexed_update (indexed_value);
insert into pwb_cal_indexed_update
select i, i, repeat('x', 80)
from generate_series(1, 1000) as i;
select pwb.reset_stats();
update pwb_cal_indexed_update set indexed_value = indexed_value + 1000;
select
  'indexed_update' as workload_name,
  accepted_statements + rejected_statements as statements,
  predicted_wal_bytes,
  actual_wal_bytes,
  absolute_prediction_error,
  case when actual_wal_bytes = 0 then null else absolute_prediction_error::numeric / actual_wal_bytes end as error_ratio
from pwb.counters();
