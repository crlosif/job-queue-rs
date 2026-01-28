use async_trait::async_trait;
use crate::{EnqueueRequest, Job, JobId, QueueError};

#[async_trait]
pub trait QueueStore: Send + Sync {
    async fn enqueue(&self, req: EnqueueRequest) -> Result<JobId, QueueError>;

    /// Lease up to `limit` jobs from `queue` for `lease_ms`.
    async fn lease(&self, queue: &str, limit: i64, lease_ms: i64) -> Result<Vec<Job>, QueueError>;
}
