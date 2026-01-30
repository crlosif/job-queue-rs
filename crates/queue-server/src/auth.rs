use axum::http::Request;

#[derive(Clone)]
pub struct AdminAuth {
    pub token: Option<String>,
}

/// Optional API auth for public endpoints (enqueue, lease, ack, fail, heartbeat).
/// When token is set, requests must include `Authorization: Bearer <token>`.
#[derive(Clone)]
pub struct ApiAuth {
    pub token: Option<String>,
}

/// Returns true if the request is authorized (no token configured, or valid Bearer token).
pub fn check_admin_auth(auth: &AdminAuth, req: &Request<axum::body::Body>) -> bool {
    let Some(expected) = &auth.token else {
        return true;
    };

    req.headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|t| t == expected)
        .unwrap_or(false)
}

/// Returns true if the request is authorized for the public API (no token configured, or valid Bearer token).
pub fn check_api_auth(auth: &ApiAuth, req: &Request<axum::body::Body>) -> bool {
    let Some(expected) = &auth.token else {
        return true;
    };

    req.headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .map(|t| t == expected)
        .unwrap_or(false)
}
