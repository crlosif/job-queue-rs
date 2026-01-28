use queue_core::{QueueError, QueueStore};
use sqlx::PgPool;

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

// Implementations added in later commits
#[async_trait::async_trait]
impl QueueStore for PostgresStore {
    async fn enqueue(&self, _req: queue_core::EnqueueRequest) -> Result<queue_core::JobId, QueueError> {
        Err(QueueError::Internal("enqueue not implemented".into()))
    }

    async fn lease(&self, _queue: &str, _limit: i64, _lease_ms: i64) -> Result<Vec<queue_core::Job>, QueueError> {
        Err(QueueError::Internal("lease not implemented".into()))
    }
}
