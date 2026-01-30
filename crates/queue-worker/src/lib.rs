use futures::future::join_all;
use std::{sync::Arc, time::Duration};
use tokio::task::JoinHandle;

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
    pub heartbeat_interval_ms: u64,
    /// If set, sent as Authorization: Bearer <token> on all API requests.
    pub api_token: Option<String>,
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

        let heartbeat_interval_ms = std::env::var("HEARTBEAT_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or((lease_ms as u64 / 2).max(1000));

        let api_token = std::env::var("API_TOKEN").ok();

        Self {
            server_url,
            queue,
            concurrency,
            lease_ms,
            poll_interval_ms,
            heartbeat_interval_ms,
            api_token,
        }
    }
}

/// Add optional Bearer token to a request builder.
fn auth_header(
    req: reqwest::RequestBuilder,
    api_token: Option<&String>,
) -> reqwest::RequestBuilder {
    match api_token {
        Some(t) => req.header("Authorization", format!("Bearer {}", t)),
        None => req,
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

    let mut handles: Vec<JoinHandle<()>> = Vec::new();

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                tracing::info!("worker shutdown signal received, draining tasks...");
                break;
            }
            _ = tokio::time::sleep(Duration::from_millis(cfg.poll_interval_ms)) => {
                // Instead of sleeping at end, we tick here and attempt lease each tick.
            }
        }

        // Try lease each tick
        let lease_req = LeaseRequest {
            queue: cfg.queue.clone(),
            limit: cfg.concurrency as i64,
            lease_ms: cfg.lease_ms,
        };

        let lease_url = format!("{}/v1/lease", cfg.server_url.trim_end_matches('/'));
        let resp = auth_header(client.post(&lease_url), cfg.api_token.as_ref())
            .json(&lease_req)
            .send()
            .await;

        let jobs = match resp {
            Ok(r) if r.status().is_success() => match r.json::<LeaseResponse>().await {
                Ok(body) => body.jobs,
                Err(e) => {
                    tracing::warn!(error=%e, "failed to parse lease response");
                    continue;
                }
            },
            Ok(r) => {
                let status = r.status();
                let text = r.text().await.unwrap_or_default();
                tracing::warn!(%status, body=%text, "lease request failed");
                continue;
            }
            Err(e) => {
                tracing::warn!(error=%e, "lease request error");
                continue;
            }
        };

        for job in jobs {
            let permit = sem.clone().acquire_owned().await?;
            let client = client.clone();
            let server_url = cfg.server_url.clone();
            let api_token = cfg.api_token.clone();

            let h = tokio::spawn(async move {
                let _permit = permit;
                if let Err(e) = process_one(&client, &server_url, api_token.as_ref(), job).await {
                    tracing::warn!(error=%e, "job processing task failed");
                }
            });
            handles.push(h);
        }
    }

    // Drain spawned tasks
    join_all(handles).await;
    tracing::info!("worker stopped");
    Ok(())
}

async fn process_one(
    client: &Client,
    server_url: &str,
    api_token: Option<&String>,
    job: Job,
) -> anyhow::Result<()> {
    let job_id = job.id;

    let span = tracing::info_span!(
        "job",
        job_id = %job_id,
        queue = %job.queue,
        attempts = job.attempts
    );
    let _enter = span.enter();

    let (stop_tx, mut stop_rx) = tokio::sync::watch::channel(false);
    let hb_client = client.clone();
    let hb_server = server_url.to_string();
    let hb_token = api_token.cloned();

    // heartbeat task
    let extend_ms = 60_000i64; // simple default extension per heartbeat tick
    let hb_handle = tokio::spawn(async move {
        loop {
            if *stop_rx.borrow() {
                break;
            }
            // wait
            tokio::select! {
                _ = stop_rx.changed() => {},
                _ = tokio::time::sleep(std::time::Duration::from_millis(5_000)) => {}
            }
            if *stop_rx.borrow() {
                break;
            }

            if let Err(e) =
                heartbeat(&hb_client, &hb_server, hb_token.as_ref(), job_id, extend_ms).await
            {
                tracing::warn!(error=%e, "heartbeat failed");
            } else {
                tracing::debug!("heartbeat ok");
            }
        }
    });

    // ---- actual handler (example)
    tracing::info!(payload=%job.payload, "processing job");

    // Optional failure simulation
    if job.payload.get("fail").and_then(|v| v.as_bool()) == Some(true) {
        tracing::warn!("simulated failure requested by payload");
        stop_tx.send(true).ok();
        let _ = hb_handle.await;
        fail(
            client,
            server_url,
            api_token,
            job_id,
            "simulated failure",
            2_000,
        )
        .await?;
        return Ok(());
    }

    // success
    stop_tx.send(true).ok();
    let _ = hb_handle.await;
    ack(client, server_url, api_token, job_id).await?;
    Ok(())
}

async fn ack(
    client: &Client,
    server_url: &str,
    api_token: Option<&String>,
    job_id: JobId,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/v1/jobs/{}/ack",
        server_url.trim_end_matches('/'),
        job_id
    );
    let r = auth_header(client.post(url), api_token).send().await?;
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
    api_token: Option<&String>,
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
    let r = auth_header(client.post(url), api_token)
        .json(&body)
        .send()
        .await?;
    if !r.status().is_success() {
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        anyhow::bail!("fail failed: {} {}", status, text);
    }
    Ok(())
}

async fn heartbeat(
    client: &Client,
    server_url: &str,
    api_token: Option<&String>,
    job_id: JobId,
    extend_ms: i64,
) -> anyhow::Result<()> {
    let url = format!(
        "{}/v1/jobs/{}/heartbeat",
        server_url.trim_end_matches('/'),
        job_id
    );
    let body = serde_json::json!({ "extend_ms": extend_ms });
    let r = auth_header(client.post(url), api_token)
        .json(&body)
        .send()
        .await?;
    if !r.status().is_success() {
        let status = r.status();
        let text = r.text().await.unwrap_or_default();
        anyhow::bail!("heartbeat failed: {} {}", status, text);
    }
    Ok(())
}
