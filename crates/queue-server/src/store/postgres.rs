use chrono::{DateTime, Utc};
use queue_core::{EnqueueRequest, JobId, QueueError, QueueStore};
use serde_json::Value;
use sqlx::{PgPool, Row};
use uuid::Uuid;

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait::async_trait]
impl QueueStore for PostgresStore {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<JobId, QueueError> {
        let id: Uuid = Uuid::new_v4();
        let max_attempts: i32 = req.max_attempts.unwrap_or(5);

        // Note: run_at is optional; COALESCE($5, now()) sets it.
        let row = sqlx::query(
            r#"
            INSERT INTO jobs (id, queue, payload, max_attempts, run_at)
            VALUES ($1, $2, $3, $4, COALESCE($5, now()))
            RETURNING id
            "#,
        )
        .bind(id)
        .bind(req.queue)
        .bind(req.payload)
        .bind(max_attempts)
        .bind(req.run_at) // Option<DateTime<Utc>>
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        let id: Uuid = row
            .try_get("id")
            .map_err(|e| QueueError::Database(e.to_string()))?;

        Ok(id)
    }

    async fn lease(&self, _queue: &str, _limit: i64, _lease_ms: i64) -> Result<Vec<queue_core::Job>, QueueError> {
        Err(QueueError::Internal("lease not implemented".into()))
    }
}
