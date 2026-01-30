use std::net::SocketAddr;

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_url: String,
    pub admin_token: Option<String>,
    /// Optional Bearer token required for public API (enqueue, lease, ack, fail, heartbeat). Env: API_TOKEN.
    pub api_token: Option<String>,
    /// Max requests per minute per client (IP). No limit if unset. Env: RATE_LIMIT_PER_MINUTE.
    pub rate_limit_per_minute: Option<u32>,
}

impl Config {
    pub fn from_env() -> Self {
        let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
        let bind: SocketAddr = bind.parse().expect("BIND_ADDR must be a valid SocketAddr");

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set (postgres://...)");

        let admin_token = std::env::var("ADMIN_TOKEN").ok();
        let api_token = std::env::var("API_TOKEN").ok();
        let rate_limit_per_minute = std::env::var("RATE_LIMIT_PER_MINUTE")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n: &u32| n > 0);

        Self {
            bind,
            database_url,
            admin_token,
            api_token,
            rate_limit_per_minute,
        }
    }
}
