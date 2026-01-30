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

    /// Enqueue with idempotency key: return existing job_id if key seen within window, else insert and record.
    async fn enqueue_with_idempotency(
        &self,
        idempotency_key: &str,
        req: &EnqueueRequest,
        max_attempts: i32,
        priority: i32,
    ) -> Result<JobId, QueueError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| QueueError::Database(e.to_string()))?;

        // Return existing job_id if key was used within the window (24h).
        let existing = sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT job_id FROM enqueue_idempotency
            WHERE idempotency_key = $1
              AND created_at > now() - (($2::text || ' hours')::interval)
            "#,
        )
        .bind(idempotency_key)
        .bind(IDEMPOTENCY_WINDOW_HOURS)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        if let Some(job_id) = existing {
            tx.commit()
                .await
                .map_err(|e| QueueError::Database(e.to_string()))?;
            return Ok(job_id);
        }

        let id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO jobs (id, queue, payload, max_attempts, run_at, priority)
            VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6)
            "#,
        )
        .bind(id)
        .bind(&req.queue)
        .bind(&req.payload)
        .bind(max_attempts)
        .bind(req.run_at)
        .bind(priority)
        .execute(&mut *tx)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        let insert_idempotency = sqlx::query(
            r#"
            INSERT INTO enqueue_idempotency (idempotency_key, job_id, created_at)
            VALUES ($1, $2, now())
            "#,
        )
        .bind(idempotency_key)
        .bind(id)
        .execute(&mut *tx)
        .await;

        match insert_idempotency {
            Ok(_) => {
                tx.commit()
                    .await
                    .map_err(|e| QueueError::Database(e.to_string()))?;
                Ok(id)
            }
            Err(e) => {
                let _ = tx.rollback().await;
                // Unique violation: another request won the race; return existing job_id.
                if is_unique_violation(&e) {
                    let row = sqlx::query_scalar::<_, Uuid>(
                        r#"SELECT job_id FROM enqueue_idempotency WHERE idempotency_key = $1"#,
                    )
                    .bind(idempotency_key)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| QueueError::Database(e.to_string()))?;
                    return Ok(row);
                }
                Err(QueueError::Database(e.to_string()))
            }
        }
    }
}

fn is_unique_violation(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(d) if d.code().as_deref() == Some("23505"))
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
        priority: row
            .try_get::<i32, _>("priority")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        created_at: row
            .try_get::<DateTime<Utc>, _>("created_at")
            .map_err(|e| QueueError::Database(e.to_string()))?,
        updated_at: row
            .try_get::<DateTime<Utc>, _>("updated_at")
            .map_err(|e| QueueError::Database(e.to_string()))?,
    })
}

const IDEMPOTENCY_WINDOW_HOURS: i64 = 24;

#[async_trait::async_trait]
impl QueueStore for PostgresStore {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<JobId, QueueError> {
        let max_attempts: i32 = req.max_attempts.unwrap_or(5);
        let priority: i32 = req.priority.unwrap_or(0);

        if let Some(ref key) = req.idempotency_key {
            return self.enqueue_with_idempotency(key, &req, max_attempts, priority).await;
        }

        let id: Uuid = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            INSERT INTO jobs (id, queue, payload, max_attempts, run_at, priority)
            VALUES ($1, $2, $3, $4, COALESCE($5, now()), $6)
            RETURNING id
            "#,
        )
        .bind(id)
        .bind(req.queue)
        .bind(req.payload)
        .bind(max_attempts)
        .bind(req.run_at)
        .bind(priority)
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
        // Order: higher priority first, then run_at, then created_at.
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
              ORDER BY priority DESC, run_at ASC, created_at ASC
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
              j.priority,
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

    async fn heartbeat(&self, job_id: JobId, extend_ms: i64) -> Result<(), QueueError> {
        let affected = sqlx::query(
            r#"
            UPDATE jobs
            SET leased_until = now() + ($2::int * interval '1 millisecond'),
                updated_at = now()
            WHERE id = $1
              AND state = 'leased'
            "#,
        )
        .bind(job_id)
        .bind(extend_ms as i32)
        .execute(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?
        .rows_affected();

        if affected == 0 {
            return Err(QueueError::InvalidState);
        }
        Ok(())
    }

    async fn queue_depth(&self, queue: &str) -> Result<i64, QueueError> {
        let row = sqlx::query(
            r#"
            SELECT COUNT(*)::bigint AS cnt
            FROM jobs
            WHERE queue = $1
              AND state = 'queued'
              AND run_at <= now()
            "#,
        )
        .bind(queue)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        let cnt: i64 = row
            .try_get("cnt")
            .map_err(|e| QueueError::Database(e.to_string()))?;
        Ok(cnt)
    }

    async fn list_dead(&self, queue: &str, limit: i64) -> Result<Vec<Job>, QueueError> {
        let rows = sqlx::query(
            r#"
            SELECT
              id, queue, payload, state::text as state,
              attempts, max_attempts, run_at, leased_until,
              priority, created_at, updated_at
            FROM jobs
            WHERE queue = $1
              AND state = 'dead'
            ORDER BY updated_at DESC
            LIMIT $2
            "#,
        )
        .bind(queue)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| QueueError::Database(e.to_string()))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(row_to_job(&row)?);
        }
        Ok(out)
    }

    async fn requeue_dead(&self, job_id: JobId) -> Result<(), QueueError> {
        let affected = sqlx::query(
            r#"
            UPDATE jobs
            SET state = 'queued',
                attempts = 0,
                last_error = NULL,
                leased_until = NULL,
                run_at = now(),
                updated_at = now()
            WHERE id = $1
              AND state = 'dead'
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
}
