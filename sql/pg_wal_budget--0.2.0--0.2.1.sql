\echo Use "ALTER EXTENSION pg_wal_budget UPDATE TO '0.2.1'" to load this file. \quit

alter table pwb.policy
  drop constraint policy_mode_check;

alter table pwb.policy
  add constraint policy_mode_check
  check (mode in ('off', 'observe', 'shadow', 'reject', 'queue'));
