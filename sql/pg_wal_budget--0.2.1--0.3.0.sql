\echo Use "ALTER EXTENSION pg_wal_budget UPDATE TO '0.3.0'" to load this file. \quit

-- 0.3.0 contains Rust-side admission, policy-cache, catalog, and shared-memory
-- refactors with no durable SQL schema changes from 0.2.1.
