use std::error::Error;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use postgres::{Client, NoTls};

const DATABASE_URL_ENV: &str = "PGWALBUDGET_TEST_DATABASE_URL";
const DEFAULT_WRITE_PREDICTION_BYTES: i64 = 16 * 1024;
const MAX_DRAIN_ATTEMPTS: usize = 8;
const QUEUE_CANCEL_SQLSTATE: &str = "57014";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CounterSnapshot {
    accepted_statements: i64,
    rejected_statements: i64,
    actual_wal_bytes: i64,
    scope_debt_bytes: i64,
    missing_actual_wal_count: i64,
    aborted_after_charge_count: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BucketSnapshot {
    available_bytes: i64,
    debt_bytes: i64,
}

#[test]
fn queue_cancel_before_final_admission_leaves_no_charge_or_reconciliation()
-> Result<(), Box<dyn Error>> {
    let Some(database_url) = std::env::var(DATABASE_URL_ENV).ok() else {
        return Ok(());
    };

    let table_name = unique_table_name();
    let mut cleanup = CleanupGuard::new(database_url.clone(), table_name.clone());
    let mut control = Client::connect(&database_url, NoTls)?;
    ensure_extension_preloaded(&mut control)?;
    cleanup.enable();
    setup_extension_state(&mut control, &table_name)?;

    let policy_id = create_queue_policy(&mut control)?;
    drain_bucket_below_next_prediction(&mut control, &table_name, policy_id)?;

    let before_counters = counter_snapshot(&mut control)?;
    let before_bucket = bucket_snapshot(&mut control, policy_id)?;
    let before_allowed_decisions = allowed_budget_decision_count(&mut control, policy_id)?;
    let before_profiles = query_profile_count(&mut control)?;

    let mut worker = Client::connect(&database_url, NoTls)?;
    let worker_pid: i32 = worker.query_one("select pg_backend_pid()", &[])?.get(0);
    let cancel_token = worker.cancel_token();
    let queued_insert = queued_insert_sql(&table_name);

    let worker_handle = thread::spawn(move || worker.batch_execute(&queued_insert));

    wait_until_worker_active(&mut control, worker_pid)?;
    cancel_token.cancel_query(NoTls)?;

    let worker_result = worker_handle
        .join()
        .map_err(|_| std::io::Error::other("queued statement worker thread panicked"))?;
    assert!(
        matches!(
            worker_result.as_ref().err().and_then(|error| error.as_db_error()),
            Some(db_error) if db_error.code().code() == QUEUE_CANCEL_SQLSTATE
        ),
        "expected queued statement cancellation, got {worker_result:?}"
    );

    assert_eq!(queued_row_count(&mut control, &table_name)?, 0);
    assert_no_charge_or_reconciliation(
        &mut control,
        policy_id,
        before_counters,
        before_bucket,
        before_allowed_decisions,
        before_profiles,
    )?;

    cleanup_extension_state(&mut control, &table_name);
    cleanup.disable();
    Ok(())
}

struct CleanupGuard {
    database_url: String,
    table_name: String,
    enabled: bool,
}

impl CleanupGuard {
    const fn new(database_url: String, table_name: String) -> Self {
        Self {
            database_url,
            table_name,
            enabled: false,
        }
    }

    const fn enable(&mut self) {
        self.enabled = true;
    }

    const fn disable(&mut self) {
        self.enabled = false;
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if !self.enabled {
            return;
        }

        if let Ok(mut client) = Client::connect(&self.database_url, NoTls) {
            cleanup_extension_state(&mut client, &self.table_name);
        }
    }
}

fn unique_table_name() -> String {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("pwb_queue_cancel_{}_{}", std::process::id(), suffix)
}

fn ensure_extension_preloaded(client: &mut Client) -> Result<(), Box<dyn Error>> {
    client.batch_execute("create extension if not exists pg_wal_budget")?;
    let status: String = client
        .query_one("select pwb.preload_status()::text", &[])?
        .get(0);
    if status != "preloaded" {
        return Err(format!(
            "pg_wal_budget must be loaded through shared_preload_libraries for this test; preload_status() returned {status:?}"
        )
        .into());
    }
    Ok(())
}

fn setup_extension_state(client: &mut Client, table_name: &str) -> Result<(), postgres::Error> {
    client.batch_execute(&format!(
        "
        truncate table pwb.policy restart identity cascade;
        select pwb.reset_stats();
        select pwb.reset_profiles();
        drop table if exists public.{table_name};
        create table public.{table_name} (
            id integer generated always as identity,
            value text
        );
        select pwb.reset_stats();
        select pwb.reset_profiles();
        "
    ))
}

fn cleanup_extension_state(client: &mut Client, table_name: &str) {
    let _ = client.batch_execute(&format!(
        "
        select pwb.set_policy_mode(1, 'observe');
        truncate table pwb.policy restart identity cascade;
        drop table if exists public.{table_name};
        "
    ));
}

fn create_queue_policy(client: &mut Client) -> Result<i32, postgres::Error> {
    let row = client.query_one(
        "
        select pwb.create_policy(
            'role',
            current_user,
            $1,
            $2,
            'queue',
            100
        )
        ",
        &[
            &DEFAULT_WRITE_PREDICTION_BYTES,
            &(DEFAULT_WRITE_PREDICTION_BYTES * 2),
        ],
    )?;
    Ok(row.get(0))
}

fn drain_bucket_below_next_prediction(
    client: &mut Client,
    table_name: &str,
    policy_id: i32,
) -> Result<(), postgres::Error> {
    for attempt in 0..MAX_DRAIN_ATTEMPTS {
        client.batch_execute(&format!(
            "
            insert into public.{table_name} (value)
            values ('drain-{attempt}')
            "
        ))?;

        let bucket = bucket_snapshot(client, policy_id)?;
        if bucket.available_bytes < DEFAULT_WRITE_PREDICTION_BYTES {
            set_queue_refill_rate_to_one_byte_per_sec(client, policy_id)?;
            return Ok(());
        }
    }

    let bucket = bucket_snapshot(client, policy_id)?;
    assert!(
        bucket.available_bytes < DEFAULT_WRITE_PREDICTION_BYTES,
        "test setup failed to drain queue bucket below the next fallback prediction after {MAX_DRAIN_ATTEMPTS} attempts: {bucket:?}"
    );
    set_queue_refill_rate_to_one_byte_per_sec(client, policy_id)?;
    Ok(())
}

fn set_queue_refill_rate_to_one_byte_per_sec(
    client: &mut Client,
    policy_id: i32,
) -> Result<(), postgres::Error> {
    client.batch_execute(&format!(
        "select pwb.update_policy({policy_id}, 1, {burst})",
        burst = DEFAULT_WRITE_PREDICTION_BYTES * 2,
    ))
}

fn queued_insert_sql(table_name: &str) -> String {
    format!(
        "
        with queued(value) as (select 'queued-cancel')
        insert into public.{table_name} (value)
        select value from queued
        "
    )
}

fn wait_until_worker_active(client: &mut Client, worker_pid: i32) -> Result<(), postgres::Error> {
    let started_at = Instant::now();
    let query_pattern = "%queued-cancel%";

    while started_at.elapsed() < Duration::from_secs(10) {
        let active = client.query_opt(
            "
            select true
            from pg_stat_activity
            where pid = $1
              and state = 'active'
              and query like $2
            ",
            &[&worker_pid, &query_pattern],
        )?;

        if active.is_some() {
            return Ok(());
        }

        thread::sleep(Duration::from_millis(25));
    }

    panic!("queued worker backend did not become active before the test timeout");
}

fn counter_snapshot(client: &mut Client) -> Result<CounterSnapshot, postgres::Error> {
    let row = client.query_one(
        "
        select
            accepted_statements,
            rejected_statements,
            actual_wal_bytes,
            scope_debt_bytes,
            missing_actual_wal_count,
            aborted_after_charge_count
        from pwb.counters()
        ",
        &[],
    )?;

    Ok(CounterSnapshot {
        accepted_statements: row.get(0),
        rejected_statements: row.get(1),
        actual_wal_bytes: row.get(2),
        scope_debt_bytes: row.get(3),
        missing_actual_wal_count: row.get(4),
        aborted_after_charge_count: row.get(5),
    })
}

fn bucket_snapshot(client: &mut Client, policy_id: i32) -> Result<BucketSnapshot, postgres::Error> {
    let row = client.query_one(
        "
        select available_bytes, debt_bytes
        from pwb.scope_stats()
        where policy_id = $1
        ",
        &[&policy_id],
    )?;

    Ok(BucketSnapshot {
        available_bytes: row.get(0),
        debt_bytes: row.get(1),
    })
}

fn allowed_budget_decision_count(
    client: &mut Client,
    policy_id: i32,
) -> Result<i64, postgres::Error> {
    let row = client.query_one(
        "
        select count(*)::bigint
        from pwb.recent_decisions(100)
        where policy_id = $1
          and decision_kind = 'allowed'
          and reason_code = 'budget_available'
        ",
        &[&policy_id],
    )?;
    Ok(row.get(0))
}

fn query_profile_count(client: &mut Client) -> Result<i64, postgres::Error> {
    let row = client.query_one("select count(*)::bigint from pwb.query_profiles()", &[])?;
    Ok(row.get(0))
}

fn queued_row_count(client: &mut Client, table_name: &str) -> Result<i64, postgres::Error> {
    let row = client.query_one(
        &format!(
            "
            select count(*)::bigint
            from public.{table_name}
            where value = 'queued-cancel'
            "
        ),
        &[],
    )?;
    Ok(row.get(0))
}

fn assert_no_charge_or_reconciliation(
    client: &mut Client,
    policy_id: i32,
    before_counters: CounterSnapshot,
    before_bucket: BucketSnapshot,
    before_allowed_decisions: i64,
    before_profiles: i64,
) -> Result<(), postgres::Error> {
    let after_counters = counter_snapshot(client)?;
    assert_eq!(after_counters, before_counters);

    let after_bucket = bucket_snapshot(client, policy_id)?;
    assert!(
        after_bucket.available_bytes >= before_bucket.available_bytes,
        "queue cancellation left a lower bucket balance: before={before_bucket:?} after={after_bucket:?}"
    );
    assert_eq!(after_bucket.debt_bytes, before_bucket.debt_bytes);

    assert_eq!(
        allowed_budget_decision_count(client, policy_id)?,
        before_allowed_decisions
    );
    assert_eq!(query_profile_count(client)?, before_profiles);
    Ok(())
}
