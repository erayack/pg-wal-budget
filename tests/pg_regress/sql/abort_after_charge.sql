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
select pwb.create_policy('role', current_user, 1, 16384, 'reject', 100) as reject_policy_id;

begin;
\set VERBOSITY sqlstate
insert into pwb_abort_after_charge_test values (1, 'duplicate');
\set VERBOSITY default
rollback;

select
  available_bytes = max_burst_bytes
  and available_bytes = 16384
  and debt_bytes = 0 as aborted_charge_refunded
from pwb.scope_stats();

insert into pwb_abort_after_charge_test values (2, 'after-rollback');

select count(*) as surviving_rows from pwb_abort_after_charge_test;

select
  accepted_statements > 0 as accepted_recorded,
  rejected_statements = 0 as no_budget_rejections,
  aborted_after_charge_count = 1 as aborted_charge_recorded
from pwb.counters();

select pwb.set_policy_mode(1, 'observe');
truncate table pwb.policy restart identity cascade;
