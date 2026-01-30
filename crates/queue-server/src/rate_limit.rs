//! Simple in-memory rate limiting: fixed 1-minute window per client key (IP or header).

use axum::http::{HeaderMap, StatusCode};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Per-client state: (request count in current window, window start).
type Window = (u32, Instant);

/// Shared state for rate limiting: key -> (count, window_start).
#[derive(Clone)]
pub struct RateLimitState {
    /// Max requests per minute per key. If None, rate limiting is disabled.
    limit_per_minute: Option<u32>,
    /// key -> (count, window_start). Window = 1 minute.
    inner: Arc<Mutex<HashMap<String, Window>>>,
}

impl RateLimitState {
    pub fn new(limit_per_minute: Option<u32>) -> Self {
        Self {
            limit_per_minute,
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns Ok(()) if the request is allowed, Err(429) if rate limited.
    pub async fn check(&self, key: &str) -> Result<(), StatusCode> {
        let Some(limit) = self.limit_per_minute else {
            return Ok(());
        };

        let mut guard = self.inner.lock().await;
        let now = Instant::now();
        let window_duration = Duration::from_secs(60);

        let (count, window_start) = guard.get(key).copied().unwrap_or((0, now));

        let (new_count, new_start) = if now.duration_since(window_start) >= window_duration {
            // New window
            (1u32, now)
        } else {
            (count.saturating_add(1), window_start)
        };

        guard.insert(key.to_string(), (new_count, new_start));

        if new_count > limit {
            return Err(StatusCode::TOO_MANY_REQUESTS);
        }
        Ok(())
    }
}

/// Extracts client key from request: X-Real-IP, then first X-Forwarded-For, else "unknown".
pub fn client_key(headers: &HeaderMap) -> String {
    if let Some(v) = headers.get("X-Real-IP").and_then(|v| v.to_str().ok()) {
        return v.trim().to_string();
    }
    if let Some(v) = headers.get("X-Forwarded-For").and_then(|v| v.to_str().ok())
        && let Some(first) = v.split(',').next()
    {
        return first.trim().to_string();
    }
    "unknown".to_string()
}
