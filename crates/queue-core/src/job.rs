use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type JobId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
    Queued,
    Leased,
    Succeeded,
    Failed,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct Job {
    #[schema(value_type = String, example = "550e8400-e29b-41d4-a716-446655440000")]
    pub id: JobId,
    pub queue: String,
    pub payload: serde_json::Value,

    pub state: JobState,

    pub attempts: i32,
    pub max_attempts: i32,

    pub run_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,

    pub priority: i32,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EnqueueRequest {
    pub queue: String,
    pub payload: serde_json::Value,
    pub max_attempts: Option<i32>,
    pub run_at: Option<DateTime<Utc>>,
    pub priority: Option<i32>,
    /// If set, duplicate enqueues with the same key within 24h return the same job_id (dedupe).
    pub idempotency_key: Option<String>,
}
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EnqueueResponse {
    #[schema(value_type = String, example = "550e8400-e29b-41d4-a716-446655440000")]
    pub job_id: JobId,
}
