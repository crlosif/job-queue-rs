use chrono::{DateTime, Utc};
use queue_core::{EnqueueRequest, Job, JobId, JobState, QueueError, QueueStore};
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

fn parse_state(s: &str) -> Result<JobState, QueueError> {
    match s {
        "queued" => Ok(JobState::Queued),
        "leased" => Ok(JobState::Leased),
        "succeeded" => Ok(JobState::Succeeded),
        "failed" => Ok(JobState::Failed),
        "dead" => Ok(JobState::Dead),
        other => Err(QueueError::Internal(format!("unknown job state: {other}"))),
    }
}

fn row_to_job(row: &sqlx::postgres::PgRow) -> Result<Job, QueueError> {
    let state_str: String = row
        .try_get("state")
        .map_err(|e| QueueError::Database(e.to_string()))?;

    Ok(Job {
        id: row
            .try_get::<Uuid, _>("id")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        queue: row
            .try_get::<String, _>("queue")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        payload: row
            .try_get::<Value, _>("payload")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        state: parse_state(&state_str)?,
        attempts: row
            .try_get::<i32, _>("attempts")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        max_attempts: row
            .try_get::<i32, _>("max_attempts")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        run_at: row
            .try_get::<DateTime<Utc>, _>("run_at")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        leased_until: row
            .try_get::<Option<DateTime<Utc>>, _>("leased_until")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(|e| QueueError::Database(e.to_string()))?,
    })
}

#[async_trait::async_trait]
impl QueueStore for PostgresStore {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<JobId, QueueError> {
        let id: Uuid = Uuid::new_v4();
        let max_attempts: i32 = req.max_attempts.unwrap_or(5);

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
        .bind(req.run_at)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        let id: Uuid = row
            .try_get("id")
            .map_err(|e| QueueError::Database(e.to_string()))?;
        Ok(id)
    }

    async fn lease(&self, queue: &str, limit: i64, lease_ms: i64) -> Result<Vec<Job>, QueueError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| QueueError::Database(e.to_string()))?;

        // Eligible:
        // - queued && run_at <= now()
        // - leased but expired (leased_until <= now())
        //
        // SKIP LOCKED prevents workers blocking each other.
        let rows = sqlx::query(
            r#"
            WITH cte AS (
              SELECT id
              FROM jobs
              WHERE queue = $1
                AND run_at <= now()
                AND (
                  state = 'queued'::job_state
                  OR (state = 'leased'::job_state AND leased_until <= now())
                )
              ORDER BY run_at ASC, created_at ASC
              FOR UPDATE SKIP LOCKED
              LIMIT $2
            )
            UPDATE jobs j
            SET state = 'leased'::job_state,
                leased_until = now() + ($3::int * interval '1 millisecond'),
                updated_at = now()
            FROM cte
            WHERE j.id = cte.id
            RETURNING
              j.id,
              j.queue,
              j.payload,
              j.state::text as state,
              j.attempts,
              j.max_attempts,
              j.run_at,
              j.leased_until,
              j.created_at,
              j.updated_at
            "#,
        )
        .bind(queue)
        .bind(limit)
        .bind(lease_ms as i32)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        tx.commit()
            .await
            .map_err(|e| QueueError::Database(e.to_string()))?;

        let mut jobs = Vec::with_capacity(rows.len());
        for row in rows {
            jobs.push(row_to_job(&row)?);
        }
        Ok(jobs)
    }

    async fn ack(&self, job_id: JobId) -> Result<(), QueueError> {
        let affected = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'succeeded'::job_state,
                leased_until = NULL,
                updated_at = now()
            WHERE id = $1 AND state = 'leased'::job_state
            "#,
        )
        .bind(job_id)
        .execute(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?
        .rows_affected();

        if affected == 0 {
            return Err(QueueError::InvalidState);
        }
        Ok(())
    }

    async fn fail(&self, job_id: JobId, reason: &str, retry_ms: i64) -> Result<(), QueueError> {
        let affected = sqlx::query(
            r#"
            UPDATE jobs
            SET attempts = attempts + 1,
                last_error = $2,
                leased_until = NULL,
                state = CASE
                  WHEN (attempts + 1) >= max_attempts THEN 'dead'::job_state
                  ELSE 'queued'::job_state
                END,
                run_at = CASE
                  WHEN (attempts + 1) >= max_attempts THEN run_at
                  ELSE now() + ($3::int * interval '1 millisecond')
                END,
                updated_at = now()
            WHERE id = $1 AND state = 'leased'::job_state
            "#,
        )
        .bind(job_id)
        .bind(reason)
        .bind(retry_ms as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?
        .rows_affected();

        if affected == 0 {
            return Err(QueueError::InvalidState);
        }
        Ok(())
    }
}
