# job-queue-rs

A minimal job queue and worker system in Rust, backed by PostgreSQL and row-level leasing (`FOR UPDATE SKIP LOCKED`).

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Quick start](#quick-start)
- [HTTP API](#http-api)
- [Development](#development)
- [Roadmap](#roadmap)
- [License](#license)

## Features

- **Enqueue** jobs with a JSON payload and optional `run_at` / `max_attempts`
- **Lease** jobs safely across multiple workers (PostgreSQL + `SKIP LOCKED`)
- **Ack** or **Fail** with retry and dead-lettering when `max_attempts` is exceeded
- Worker polling loop with configurable concurrency
- HTTP API (Axum) and CLI (enqueue, ping, worker)

## Requirements

- [Rust](https://www.rust-lang.org/) (stable)
- [Docker](https://www.docker.com/) (for PostgreSQL)

## Quick start

### 1. Start PostgreSQL

```bash
docker compose up -d
```

### 2. Run migrations

```bash
export DATABASE_URL=postgres://queue:queue@localhost:5432/queue
cargo install sqlx-cli --no-default-features --features postgres
sqlx migrate run
```

### 3. Start the server

```bash
export DATABASE_URL=postgres://queue:queue@localhost:5432/queue
export BIND_ADDR=0.0.0.0:8080
cargo run -p queue-server
```

### 4. Enqueue a job

```bash
cargo run -p queue-cli -- enqueue --queue default --json '{"hello":"world"}'
```

### 5. Run a worker

```bash
cargo run -p queue-cli -- worker --queue default --concurrency 5
```

## HTTP API

### Health

| Method | Path      | Response   |
|--------|-----------|------------|
| `GET`  | `/healthz` | `200 OK`   |

### Enqueue

| Method | Path       |
|--------|------------|
| `POST` | `/v1/jobs` |

**Request body:**

```json
{
  "queue": "default",
  "payload": { "hello": "world" },
  "max_attempts": 5,
  "run_at": null
}
```

**Response:** `200 OK`

```json
{ "job_id": "uuid" }
```

### Lease

| Method | Path        |
|--------|-------------|
| `POST` | `/v1/lease` |

**Request body:**

```json
{
  "queue": "default",
  "limit": 10,
  "lease_ms": 30000
}
```

**Response:** `200 OK`

```json
{ "jobs": [ ... ] }
```

### Ack

| Method | Path                 | Response        |
|--------|----------------------|-----------------|
| `POST` | `/v1/jobs/:id/ack`   | `204 No Content`|

### Fail

| Method | Path                  |
|--------|-----------------------|
| `POST` | `/v1/jobs/:id/fail`   |

**Request body:**

```json
{
  "reason": "something broke",
  "retry_ms": 5000
}
```

**Response:** `204 No Content`

## Development

Format, lint, and test:

```bash
cargo fmt
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Integration tests for `queue-server` require a running PostgreSQL and `DATABASE_URL`; they run serially to avoid conflicts.

## Roadmap

- Cron / scheduled jobs
- Heartbeat (extend lease for long-running jobs)
- DLQ inspection endpoints
- Metrics and admin UI
