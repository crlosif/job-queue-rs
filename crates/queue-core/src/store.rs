use crate::{EnqueueRequest, Job, JobId, QueueError};
use async_trait::async_trait;

#[async_trait]
pub trait QueueStore: Send + Sync {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<JobId, QueueError>;

    /// Lease up to `limit` jobs from `queue` for `lease_ms`.
    async fn lease(&self, queue: &str, limit: i64, lease_ms: i64) -> Result<Vec<Job>, QueueError>;

    /// Mark a leased job as succeeded.
    async fn ack(&self, job_id: JobId) -> Result<(), QueueError>;

    /// Mark a leased job as failed; retries or dead-letters based on attempts/max_attempts.
    /// retry_ms: how long to wait before retry (ignored if job becomes dead).
    async fn fail(&self, job_id: JobId, reason: &str, retry_ms: i64) -> Result<(), QueueError>;

    /// Extend lease for a currently leased job.
    async fn heartbeat(&self, job_id: JobId, extend_ms: i64) -> Result<(), QueueError>;

    async fn queue_depth(&self, queue: &str) -> Result<i64, QueueError>;

    async fn list_dead(&self, queue: &str, limit: i64) -> Result<Vec<Job>, QueueError>;

    async fn requeue_dead(&self, job_id: JobId) -> Result<(), QueueError>;
}
