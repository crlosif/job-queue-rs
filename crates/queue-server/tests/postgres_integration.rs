use anyhow::Context;
use queue_core::{EnqueueRequest, QueueStore};
use serde_json::json;
use serial_test::serial;
use sqlx::{PgPool, postgres::PgPoolOptions};

use queue_server::store::postgres::PostgresStore;

// Make queue-server importable from tests by adding a tiny lib target (see below).

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("../../migrations");

async fn setup() -> anyhow::Result<PostgresStore> {
    let database_url =
        std::env::var("DATABASE_URL").context("DATABASE_URL must be set for integration tests")?;

    let pool: PgPool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    MIGRATOR.run(&pool).await?;

    // Clean slate per test run
    sqlx::query("TRUNCATE TABLE jobs").execute(&pool).await?;

    Ok(PostgresStore::new(pool))
}

#[tokio::test]
#[serial]
async fn enqueue_and_lease_and_ack() -> anyhow::Result<()> {
    let store = setup().await?;

    let id = store
        .enqueue(EnqueueRequest {
            queue: "default".to_string(),
            payload: json!({"hello":"world"}),
            max_attempts: Some(3),
            run_at: None,
            priority: None,
        })
        .await?;

    let jobs = store.lease("default", 10, 5_000).await?;
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0].id, id);

    store.ack(id).await?;
    Ok(())
}

#[tokio::test]
#[serial]
async fn fail_retries_then_dead() -> anyhow::Result<()> {
    let store = setup().await?;

    let id = store
        .enqueue(EnqueueRequest {
            queue: "default".to_string(),
            payload: json!({"task":"x"}),
            max_attempts: Some(2),
            run_at: None,
            priority: None,
        })
        .await?;

    // 1st lease -> fail -> should retry (run_at = now + 100ms)
    let jobs = store.lease("default", 1, 1_000).await?;
    assert_eq!(jobs.len(), 1);
    store.fail(id, "boom", 100).await?;

    // wait for run_at to be in the past so job is leaseable again
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // 2nd lease -> fail -> should become dead
    let jobs = store.lease("default", 1, 1_000).await?;
    assert_eq!(jobs.len(), 1);
    store.fail(id, "boom2", 100).await?;

    // After dead, it should NOT be leaseable
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let jobs = store.lease("default", 10, 1_000).await?;
    assert_eq!(jobs.len(), 0);

    Ok(())
}

#[tokio::test]
#[serial]
async fn lease_expiration_allows_reclaim() -> anyhow::Result<()> {
    let store = setup().await?;

    let _id = store
        .enqueue(EnqueueRequest {
            queue: "default".to_string(),
            payload: json!({"k":"v"}),
            max_attempts: Some(3),
            run_at: None,
            priority: None,
        })
        .await?;

    // lease for 100ms
    let jobs1 = store.lease("default", 10, 100).await?;
    assert_eq!(jobs1.len(), 1);

    // immediately leasing again should return none
    let jobs2 = store.lease("default", 10, 100).await?;
    assert_eq!(jobs2.len(), 0);

    // wait for lease to expire
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    // should be reclaimable
    let jobs3 = store.lease("default", 10, 100).await?;
    assert_eq!(jobs3.len(), 1);

    Ok(())
}
