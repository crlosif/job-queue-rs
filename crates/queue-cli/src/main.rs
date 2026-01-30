use anyhow::Context;
use clap::{Parser, Subcommand};
use queue_core::{EnqueueRequest, EnqueueResponse};
use serde_json::Value;

#[derive(Parser)]
#[command(name = "queue-cli", version, about = "CLI for job-queue-rs")]
struct Cli {
    #[arg(long, default_value = "http://localhost:8080")]
    server_url: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Enqueue a job
    Enqueue {
        #[arg(long, default_value = "default")]
        queue: String,

        /// JSON payload string, e.g. '{"hello":"world"}'
        #[arg(long)]
        json: String,

        #[arg(long)]
        max_attempts: Option<i32>,

        /// Priority (higher = processed first; default 0)
        #[arg(long)]
        priority: Option<i32>,

        /// Idempotency key: same key within 24h returns the same job_id (dedupe)
        #[arg(long)]
        idempotency_key: Option<String>,
    },

    /// Ping server health endpoint
    Ping,

    /// Run a worker (same as queue-worker binary, but convenient)
    Worker {
        #[arg(long, default_value = "default")]
        queue: String,

        #[arg(long, default_value_t = 10)]
        concurrency: usize,

        #[arg(long, default_value_t = 30_000)]
        lease_ms: i64,

        #[arg(long, default_value_t = 500)]
        poll_interval_ms: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let base = cli.server_url.trim_end_matches('/').to_string();

    match cli.command {
        Commands::Ping => {
            let url = format!("{}/healthz", base);
            let r = reqwest::get(url).await?;
            let text = r.text().await.unwrap_or_default();
            println!("{}", text);
        }

        Commands::Enqueue {
            queue,
            json,
            max_attempts,
            priority,
            idempotency_key,
        } => {
            let payload: Value = serde_json::from_str(&json).context("invalid JSON payload")?;

            let req = EnqueueRequest {
                queue,
                payload,
                max_attempts,
                run_at: None,
                priority,
                idempotency_key,
            };

            let url = format!("{}/v1/jobs", base);
            let client = reqwest::Client::new();
            let r = client.post(url).json(&req).send().await?;

            if !r.status().is_success() {
                let status = r.status();
                let body = r.text().await.unwrap_or_default();
                anyhow::bail!("enqueue failed: {} {}", status, body);
            }

            let body = r.json::<EnqueueResponse>().await?;
            println!("{}", body.job_id);
        }

        Commands::Worker {
            queue,
            concurrency,
            lease_ms,
            poll_interval_ms,
        } => {
            let heartbeat_interval_ms = (lease_ms as u64 / 2).max(1000);
            let cfg = queue_worker::WorkerConfig {
                server_url: base,
                queue,
                concurrency,
                lease_ms,
                poll_interval_ms,
                heartbeat_interval_ms,
            };
            queue_worker::run_worker(cfg).await?;
        }
    }

    Ok(())
}
