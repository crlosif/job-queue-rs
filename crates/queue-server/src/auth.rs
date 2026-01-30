use axum::http::Request;

#[derive(Clone)]
pub struct AdminAuth {
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
