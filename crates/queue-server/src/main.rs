mod app;
mod auth;
mod config;
mod metrics;
mod rate_limit;
mod store;

use std::sync::Arc;

use app::AppState;
use config::Config;
use sqlx::postgres::PgPoolOptions;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use store::postgres::PostgresStore;

#[tokio::main]
async fn main() {
    metrics::init_metrics();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "queue_server=info,tower_http=info,sqlx=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env();

    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.database_url)
        .await
        .expect("failed to connect to Postgres");

    let store = Arc::new(PostgresStore::new(pool));

    let state = AppState { store };

    let admin_auth = auth::AdminAuth {
        token: cfg.admin_token.clone(),
    };
    let api_auth = auth::ApiAuth {
        token: cfg.api_token.clone(),
    };
    let rate_limit_state = rate_limit::RateLimitState::new(cfg.rate_limit_per_minute);

    let app = app::build_app(state, admin_auth, api_auth, rate_limit_state)
        .layer(TraceLayer::new_for_http());

    tracing::info!("listening on {}", cfg.bind);

    let listener = tokio::net::TcpListener::bind(cfg.bind)
        .await
        .expect("failed to bind");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server failed");
    async fn shutdown_signal() {
        // Wait for Ctrl+C
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("shutdown signal received");
    }
}
