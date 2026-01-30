use crate::auth::{self, AdminAuth, ApiAuth};
use crate::metrics;
use crate::rate_limit::{self, RateLimitState};
use axum::middleware;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use queue_core::{EnqueueRequest, EnqueueResponse, Job, JobId, QueueError, QueueStore};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<dyn QueueStore>,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct LeaseRequest {
    pub queue: String,
    pub limit: i64,
    pub lease_ms: i64,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct LeaseResponse {
    pub jobs: Vec<Job>,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct FailRequest {
    pub reason: String,
    pub retry_ms: Option<i64>,
}

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
pub struct HeartbeatRequest {
    pub extend_ms: i64,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct DepthResponse {
    pub queue: String,
    pub depth: i64,
}

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
pub struct DeadJobsResponse {
    pub queue: String,
    pub jobs: Vec<Job>,
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Job Queue API",
        version = "0.1.0",
        description = "REST API for enqueueing, leasing, and managing jobs."
    ),
    paths(
        enqueue_job,
        lease_jobs,
        ack_job,
        fail_job,
        heartbeat_job,
        admin_depth,
        admin_dead,
        admin_requeue
    ),
    components(schemas(
        queue_core::EnqueueRequest,
        queue_core::EnqueueResponse,
        queue_core::Job,
        queue_core::JobState,
        LeaseRequest,
        LeaseResponse,
        FailRequest,
        HeartbeatRequest,
        DepthResponse,
        DeadJobsResponse,
        DeadQuery
    ))
)]
struct ApiDoc;

async fn metrics_handler() -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        metrics::gather(),
    )
}

/// State for public API routes: store + optional API auth + optional rate limiting.
type PublicApiState = (AppState, ApiAuth, RateLimitState);

pub fn build_app(
    state: AppState,
    admin_auth: AdminAuth,
    api_auth: ApiAuth,
    rate_limit_state: RateLimitState,
) -> Router {
    let unauthenticated = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/metrics", get(metrics_handler));

    let public_api_state: PublicApiState = (state.clone(), api_auth.clone(), rate_limit_state);
    let public_api = Router::new()
        .route("/v1/jobs", post(enqueue_job))
        .route("/v1/lease", post(lease_jobs))
        .route("/v1/jobs/{id}/ack", post(ack_job))
        .route("/v1/jobs/{id}/fail", post(fail_job))
        .route("/v1/jobs/{id}/heartbeat", post(heartbeat_job))
        .route_layer(middleware::from_fn_with_state(
            public_api_state.clone(),
            rate_limit_middleware,
        ))
        .route_layer(middleware::from_fn_with_state(
            public_api_state.clone(),
            api_auth_middleware,
        ))
        .with_state(public_api_state);

    let admin_state = (state.clone(), admin_auth.clone());
    let admin = Router::new()
        .route("/v1/admin/queues/{queue}/depth", get(admin_depth))
        .route("/v1/admin/queues/{queue}/dead", get(admin_dead))
        .route("/v1/admin/jobs/{id}/requeue", post(admin_requeue))
        .with_state(admin_state.clone())
        .route_layer(middleware::from_fn_with_state(
            admin_state,
            admin_auth_middleware,
        ));

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(unauthenticated)
        .merge(public_api)
        .merge(admin)
}

async fn rate_limit_middleware(
    State((_state, _auth, rate_limit)): State<PublicApiState>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, axum::response::Response> {
    let key = rate_limit::client_key(req.headers());
    rate_limit
        .check(&key)
        .await
        .map_err(|code| (code, "rate limit exceeded").into_response())?;
    Ok(next.run(req).await)
}

async fn api_auth_middleware(
    State((_state, auth, _rate_limit)): State<PublicApiState>,
    req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next,
) -> Result<axum::response::Response, StatusCode> {
    if auth::check_api_auth(&auth, &req) {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
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

/// Enqueue a new job.
#[utoipa::path(
    post,
    path = "/v1/jobs",
    request_body = EnqueueRequest,
    responses(
        (status = 200, description = "Job enqueued", body = EnqueueResponse),
        (status = 500, description = "Internal error")
    )
)]
async fn enqueue_job(
    State((state, _auth, _rate_limit)): State<PublicApiState>,
    Json(req): Json<EnqueueRequest>,
) -> Result<Json<EnqueueResponse>, (StatusCode, String)> {
    let job_id = state.store.enqueue(req).await.map_err(map_err)?;

    metrics::JOBS_ENQUEUED.inc();
    Ok(Json(EnqueueResponse { job_id }))
}

/// Lease jobs from a queue. Requires API auth if API_TOKEN is set. Rate limited when RATE_LIMIT_PER_MINUTE is set.
#[utoipa::path(
    post,
    path = "/v1/lease",
    request_body = LeaseRequest,
    responses(
        (status = 200, description = "Leased jobs", body = LeaseResponse),
        (status = 401, description = "Unauthorized (API_TOKEN required)"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 500, description = "Internal error")
    )
)]
async fn lease_jobs(
    State((state, _auth, _rate_limit)): State<PublicApiState>,
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

/// Acknowledge (complete) a leased job. Requires API auth if API_TOKEN is set. Rate limited when RATE_LIMIT_PER_MINUTE is set.
#[utoipa::path(
    post,
    path = "/v1/jobs/{id}/ack",
    params(("id" = uuid::Uuid, Path, description = "Job ID")),
    responses(
        (status = 204, description = "Job acknowledged"),
        (status = 401, description = "Unauthorized (API_TOKEN required)"),
        (status = 404, description = "Job not found"),
        (status = 409, description = "Invalid state"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 500, description = "Internal error")
    )
)]
async fn ack_job(
    State((state, _auth, _rate_limit)): State<PublicApiState>,
    Path(id): Path<JobId>,
) -> Result<StatusCode, (StatusCode, String)> {
    state.store.ack(id).await.map_err(map_err)?;
    metrics::JOBS_ACKED.inc();
    Ok(StatusCode::NO_CONTENT)
}

/// Mark a leased job as failed (retry or dead). Requires API auth if API_TOKEN is set. Rate limited when RATE_LIMIT_PER_MINUTE is set.
#[utoipa::path(
    post,
    path = "/v1/jobs/{id}/fail",
    params(("id" = uuid::Uuid, Path, description = "Job ID")),
    request_body = FailRequest,
    responses(
        (status = 204, description = "Job failed"),
        (status = 401, description = "Unauthorized (API_TOKEN required)"),
        (status = 404, description = "Job not found"),
        (status = 409, description = "Invalid state"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 500, description = "Internal error")
    )
)]
async fn fail_job(
    State((state, _auth, _rate_limit)): State<PublicApiState>,
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

/// Extend lease for a job. Requires API auth if API_TOKEN is set. Rate limited when RATE_LIMIT_PER_MINUTE is set.
#[utoipa::path(
    post,
    path = "/v1/jobs/{id}/heartbeat",
    params(("id" = uuid::Uuid, Path, description = "Job ID")),
    request_body = HeartbeatRequest,
    responses(
        (status = 204, description = "Lease extended"),
        (status = 401, description = "Unauthorized (API_TOKEN required)"),
        (status = 404, description = "Job not found"),
        (status = 409, description = "Invalid state"),
        (status = 429, description = "Rate limit exceeded"),
        (status = 500, description = "Internal error")
    )
)]
async fn heartbeat_job(
    State((state, _auth, _rate_limit)): State<PublicApiState>,
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

#[derive(Debug, serde::Deserialize, utoipa::IntoParams, utoipa::ToSchema)]
pub struct DeadQuery {
    /// Max number of dead jobs to return (1–500).
    pub limit: Option<i64>,
}

/// Get queue depth (admin). Requires Authorization.
#[utoipa::path(
    get,
    path = "/v1/admin/queues/{queue}/depth",
    params(("queue" = String, Path, description = "Queue name")),
    responses(
        (status = 200, description = "Queue depth", body = DepthResponse),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal error")
    ),
    security(("admin_token" = []))
)]
async fn admin_depth(
    State((state, _auth)): State<(AppState, AdminAuth)>,
    Path(queue): Path<String>,
) -> Result<Json<DepthResponse>, (StatusCode, String)> {
    let depth = state.store.queue_depth(&queue).await.map_err(map_err)?;
    Ok(Json(DepthResponse { queue, depth }))
}

/// List dead jobs for a queue (admin). Requires Authorization.
#[utoipa::path(
    get,
    path = "/v1/admin/queues/{queue}/dead",
    params(
        ("queue" = String, Path, description = "Queue name"),
        DeadQuery
    ),
    responses(
        (status = 200, description = "Dead jobs", body = DeadJobsResponse),
        (status = 401, description = "Unauthorized"),
        (status = 500, description = "Internal error")
    ),
    security(("admin_token" = []))
)]
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

/// Requeue a dead job (admin). Requires Authorization.
#[utoipa::path(
    post,
    path = "/v1/admin/jobs/{id}/requeue",
    params(("id" = uuid::Uuid, Path, description = "Job ID")),
    responses(
        (status = 204, description = "Job requeued"),
        (status = 401, description = "Unauthorized"),
        (status = 404, description = "Job not found"),
        (status = 500, description = "Internal error")
    ),
    security(("admin_token" = []))
)]
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
