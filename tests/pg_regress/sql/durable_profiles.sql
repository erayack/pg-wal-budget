create extension if not exists pg_wal_budget;

set compute_query_id = on;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'observe', 100) as observe_policy_id;

create temp table pwb_durable_profile_test (
  id integer generated always as identity,
  value text
);

prepare pwb_durable_profile_insert(text) as
  insert into pwb_durable_profile_test (value) values ($1);

execute pwb_durable_profile_insert('durable-a');
execute pwb_durable_profile_insert('durable-b');
execute pwb_durable_profile_insert('durable-c');

select
  exists (
    select 1
    from pwb.query_profiles()
    where calls >= 1
      and ewma_wal_bytes > 0
      and max_wal_bytes >= ewma_wal_bytes
  ) as live_profile_observed;

select pwb.flush_profiles();

select
  count(*) >= 2 as durable_profiles_written,
  bool_or(scope_hash is null) as global_profile_written,
  bool_or(scope_hash is not null) as scoped_profile_written
from pwb.query_profile;

select pwb.reset_profiles();

select
  not exists (select 1 from pwb.query_profiles()) as live_profiles_cleared,
  not exists (select 1 from pwb.query_profile) as durable_profiles_cleared;

select pwb.set_policy_mode(1, 'observe');
truncate table pwb.policy restart identity cascade;
