use std::net::SocketAddr;

#[derive(Clone)]
pub struct Config {
    pub bind: SocketAddr,
    pub database_url: String,
    pub admin_token: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        let bind = std::env::var("BIND_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
        let bind: SocketAddr = bind.parse().expect("BIND_ADDR must be a valid SocketAddr");

        let database_url =
            std::env::var("DATABASE_URL").expect("DATABASE_URL must be set (postgres://...)");

        let admin_token = std::env::var("ADMIN_TOKEN").ok();

        Self {
            bind,
            database_url,
            admin_token,
        }
    }
}
