-- Idempotency keys for enqueue: same key within 24h returns existing job_id.
CREATE TABLE enqueue_idempotency (
  idempotency_key TEXT PRIMARY KEY,
  job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX enqueue_idempotency_created_at_idx
  ON enqueue_idempotency (created_at);
