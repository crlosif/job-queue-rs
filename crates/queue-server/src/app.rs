use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use queue_core::{EnqueueRequest, EnqueueResponse, Job, JobId, QueueError, QueueStore};

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn QueueStore>,
}

#[derive(Debug, serde::Deserialize)]
pub struct LeaseRequest {
    pub queue: String,
    pub limit: i64,
    pub lease_ms: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct LeaseResponse {
    pub jobs: Vec<Job>,
}

#[derive(Debug, serde::Deserialize)]
pub struct FailRequest {
    pub reason: String,
    pub retry_ms: Option<i64>,
}

pub fn build_app(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/jobs", post(enqueue_job))
        .route("/v1/lease", post(lease_jobs))
        .route("/v1/jobs/:id/ack", post(ack_job))
        .route("/v1/jobs/:id/fail", post(fail_job))
        .with_state(state)
}

async fn enqueue_job(
    State(state): State<AppState>,
    Json(req): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, (StatusCode, String)> {
    let job_id = state.store.enqueue(req).await.map_err(map_err)?;

    Ok(Json(EnqueueResponse { job_id }))
}

async fn lease_jobs(
    State(state): State<AppState>,
    Json(req): Json<LeaseRequest>,
) -> Result<Json<LeaseResponse>, (StatusCode, String)> {
    let jobs = state
        .store
        .lease(&req.queue, req.limit, req.lease_ms)
        .await
        .map_err(map_err)?;

    Ok(Json(LeaseResponse { jobs }))
}

async fn ack_job(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.store.ack(id).await.map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn fail_job(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Json(req): Json<FailRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    let retry_ms = req.retry_ms.unwrap_or(5_000);
    state
        .store
        .fail(id, &req.reason, retry_ms)
        .await
        .map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}

fn map_err(e: QueueError) -> (StatusCode, String) {
    match e {
        QueueError::NotFound => (StatusCode::NOT_FOUND, e.to_string()),
        QueueError::InvalidState => (StatusCode::CONFLICT, e.to_string()),
        QueueError::Database(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        QueueError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
