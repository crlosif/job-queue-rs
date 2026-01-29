use std::{sync::Arc, time::Duration};

use queue_core::{Job, JobId};
use reqwest::Client;
use tokio::sync::Semaphore;

#[derive(Clone)]
pub struct WorkerConfig {
    pub server_url: String,    // e.g. http://localhost:8080
    pub queue: String,         // e.g. default
    pub concurrency: usize,    // e.g. 10
    pub lease_ms: i64,         // e.g. 30000
    pub poll_interval_ms: u64, // e.g. 500
}

impl WorkerConfig {
    pub fn from_env() -> Self {
        let server_url =
            std::env::var("QUEUE_URL").unwrap_or_else(|_| "http://localhost:8080".to_string());
        let queue = std::env::var("QUEUE_NAME").unwrap_or_else(|_| "default".to_string());
        let concurrency = std::env::var("CONCURRENCY")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(10);
        let lease_ms = std::env::var("LEASE_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30_000);
        let poll_interval_ms = std::env::var("POLL_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(500);

        Self {
            server_url,
            queue,
            concurrency,
            lease_ms,
            poll_interval_ms,
        }
    }
}

#[derive(serde::Serialize)]
struct LeaseRequest {
    queue: String,
    limit: i64,
    lease_ms: i64,
}

#[derive(serde::Deserialize)]
struct LeaseResponse {
    jobs: Vec<Job>,
}

#[derive(serde::Serialize)]
struct FailRequest {
    reason: String,
    retry_ms: Option<i64>,
}

pub async fn run_worker(cfg: WorkerConfig) -> anyhow::Result<()> {
    let client = Client::new();
    let sem = Arc::new(Semaphore::new(cfg.concurrency));

    tracing::info!(
        queue=%cfg.queue,
        concurrency=cfg.concurrency,
        lease_ms=cfg.lease_ms,
        poll_interval_ms=cfg.poll_interval_ms,
        server_url=%cfg.server_url,
        "worker started"
    );

    loop {
        let lease_req = LeaseRequest {
            queue: cfg.queue.clone(),
            limit: cfg.concurrency as i64, // simple: one batch ~= concurrency
            lease_ms: cfg.lease_ms,
        };

        let lease_url = format!("{}/v1/lease", cfg.server_url.trim_end_matches('/'));
        let resp = client.post(&lease_url).json(&lease_req).send().await;

        let jobs = match resp {
            Ok(r) if r.status().is_success() => match r.json::<LeaseResponse>().await {
                Ok(body) => body.jobs,
                Err(e) => {
                    tracing::warn!(error=%e, "failed to parse lease response");
                    tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
                    continue;
                }
            },
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                tracing::warn!(%status, body=%text, "lease request failed");
                tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
                continue;
            }
            Err(e) => {
                tracing::warn!(error=%e, "lease request error");
                tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
                continue;
            }
        };

        if jobs.is_empty() {
            tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
            continue;
        }

        for job in jobs {
            let permit = sem.clone().acquire_owned().await?;
            let client = client.clone();
            let server_url = cfg.server_url.clone();

            tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = process_one(&client, &server_url, job).await {
                    tracing::warn!(error=%e, "job processing task failed");
                }
            });
        }
    }
}

async fn process_one(client: &Client, server_url: &str, job: Job) -> anyhow::Result<()> {
    let job_id = job.id;

    // Example handler: just log payload
    tracing::info!(job_id=%job_id, queue=%job.queue, attempts=job.attempts, payload=%job.payload, "processing job");

    // If you want to simulate failures:
    // if job.payload.get("fail").and_then(|v| v.as_bool()) == Some(true) { ... }

    // Success => ack
    ack(client, server_url, job_id).await?;
    Ok(())
}

async fn ack(client: &Client, server_url: &str, job_id: JobId) -> anyhow::Result<()> {
    let url = format!(
        "{}/v1/jobs/{}/ack",
        server_url.trim_end_matches('/'),
        job_id
    );
    let r = client.post(url).send().await?;
    if !r.status().is_success() {
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        anyhow::bail!("ack failed: {} {}", status, text);
    }
    Ok(())
}

#[allow(dead_code)]
async fn fail(
    client: &Client,
    server_url: &str,
    job_id: JobId,
    reason: &str,
    retry_ms: i64,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/v1/jobs/{}/fail",
        server_url.trim_end_matches('/'),
        job_id
    );
    let body = FailRequest {
        reason: reason.to_string(),
        retry_ms: Some(retry_ms),
    };
    let r = client.post(url).json(&body).send().await?;
    if !r.status().is_success() {
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        anyhow::bail!("fail failed: {} {}", status, text);
    }
    Ok(())
}
