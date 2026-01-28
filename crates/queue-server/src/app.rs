mod app;
mod config;
mod store;

use axum::Router;
use config::Config;
use sqlx::postgres::PgPoolOptions;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            "queue_server=info,tower_http=info".into()
        }))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env();

    // DB pool (used later in commit 9+)
    let _pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&cfg.database_url)
        .await
        .expect("failed to connect to Postgres");

    let app: Router = app::build_app().layer(TraceLayer::new_for_http());

    tracing::info!("listening on {}", cfg.bind);

    let listener = tokio::net::TcpListener::bind(cfg.bind)
        .await
        .expect("failed to bind");
    axum::serve(listener, app).await.expect("server failed");
}
