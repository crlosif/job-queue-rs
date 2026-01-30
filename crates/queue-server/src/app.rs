use crate::auth::{self, AdminAuth};
use crate::metrics;
use axum::middleware;
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

#[derive(Debug, serde::Deserialize)]
pub struct HeartbeatRequest {
    pub extend_ms: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct DepthResponse {
    pub queue: String,
    pub depth: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct DeadJobsResponse {
    pub queue: String,
    pub jobs: Vec<Job>,
}

async fn metrics_handler() -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        metrics::gather(),
    )
}

pub fn build_app(state: AppState, admin_auth: AdminAuth) -> Router {
    let public = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler))
        .route("/v1/jobs", post(enqueue_job))
        .route("/v1/lease", post(lease_jobs))
        .route("/v1/jobs/:id/ack", post(ack_job))
        .route("/v1/jobs/:id/fail", post(fail_job))
        .route("/v1/jobs/:id/heartbeat", post(heartbeat_job));

    let admin_state = (state.clone(), admin_auth.clone());
    let admin = Router::new()
        .route("/v1/admin/queues/:queue/depth", get(admin_depth))
        .route("/v1/admin/queues/:queue/dead", get(admin_dead))
        .route("/v1/admin/jobs/:id/requeue", post(admin_requeue))
        .with_state(admin_state.clone())
        .route_layer(middleware::from_fn_with_state(
            admin_state,
            admin_auth_middleware,
        ));

    Router::new().merge(public.with_state(state)).merge(admin)
}

async fn admin_auth_middleware(
    State((_state, auth)): State<(AppState, AdminAuth)>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    if auth::check_admin_auth(&auth, &req) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn enqueue_job(
    State(state): State<AppState>,
    Json(req): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, (StatusCode, String)> {
    let job_id = state.store.enqueue(req).await.map_err(map_err)?;

    metrics::JOBS_ENQUEUED.inc();
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

    metrics::JOBS_LEASED.inc_by(jobs.len() as u64);
    metrics::INFLIGHT_LEASES.set(jobs.len() as i64);

    Ok(Json(LeaseResponse { jobs }))
}

async fn ack_job(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.store.ack(id).await.map_err(map_err)?;
    metrics::JOBS_ACKED.inc();
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
    metrics::JOBS_FAILED.inc();
    Ok(StatusCode::NO_CONTENT)
}

async fn heartbeat_job(
    State(state): State<AppState>,
    Path(id): Path<JobId>,
    Json(req): Json<HeartbeatRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    state
        .store
        .heartbeat(id, req.extend_ms)
        .await
        .map_err(map_err)?;
    Ok(StatusCode::NO_CONTENT)
}

use axum::extract::Query;

#[derive(Debug, serde::Deserialize)]
pub struct DeadQuery {
    pub limit: Option<i64>,
}

async fn admin_depth(
    State((state, _auth)): State<(AppState, AdminAuth)>,
    Path(queue): Path<String>,
) -> Result<Json<DepthResponse>, (StatusCode, String)> {
    let depth = state.store.queue_depth(&queue).await.map_err(map_err)?;
    Ok(Json(DepthResponse { queue, depth }))
}

async fn admin_dead(
    State((state, _auth)): State<(AppState, AdminAuth)>,
    Path(queue): Path<String>,
    Query(q): Query<DeadQuery>,
) -> Result<Json<DeadJobsResponse>, (StatusCode, String)> {
    let limit = q.limit.unwrap_or(50).clamp(1, 500);
    let jobs = state
        .store
        .list_dead(&queue, limit)
        .await
        .map_err(map_err)?;
    Ok(Json(DeadJobsResponse { queue, jobs }))
}

async fn admin_requeue(
    State((state, _auth)): State<(AppState, AdminAuth)>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.store.requeue_dead(id).await.map_err(map_err)?;
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
