create extension if not exists pg_wal_budget;

truncate table pwb.policy restart identity cascade;
select pwb.reset_stats();
select pwb.reset_profiles();

create temp table pwb_abort_after_charge_test (
  id integer primary key,
  value text
);

insert into pwb_abort_after_charge_test values (1, 'seed');

select pwb.reset_stats();
select pwb.create_policy('role', current_user, 1073741824, 1073741824, 'reject', 100) as reject_policy_id;

begin;
\set VERBOSITY sqlstate
insert into pwb_abort_after_charge_test values (1, 'duplicate');
\set VERBOSITY default
rollback;

insert into pwb_abort_after_charge_test values (2, 'after-rollback');

select count(*) as surviving_rows from pwb_abort_after_charge_test;

select
  accepted_statements > 0 as accepted_recorded,
  rejected_statements = 0 as no_budget_rejections
from pwb.counters();
