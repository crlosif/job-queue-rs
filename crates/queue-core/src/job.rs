use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub type JobId = Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum JobState {
    Queued,
    Leased,
    Succeeded,
    Failed,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: JobId,
    pub queue: String,
    pub payload: serde_json::Value,

    pub state: JobState,

    pub attempts: i32,
    pub max_attempts: i32,

    pub run_at: DateTime<Utc>,
    pub leased_until: Option<DateTime<Utc>>,

    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnqueueRequest {
    pub queue: String,
    pub payload: serde_json::Value,
    pub max_attempts: Option<i32>,
    pub run_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnqueueResponse {
    pub job_id: JobId,
}
