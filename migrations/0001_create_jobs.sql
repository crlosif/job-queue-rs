CREATE TYPE job_state AS ENUM (
  'queued',
  'leased',
  'succeeded',
  'failed',
  'dead'
);

CREATE TABLE jobs (
  id UUID PRIMARY KEY,
  queue TEXT NOT NULL,
  payload JSONB NOT NULL,

  state job_state NOT NULL DEFAULT 'queued',

  attempts INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,

  run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  leased_until TIMESTAMPTZ NULL,
  last_error TEXT NULL,

  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX jobs_dequeue_idx
  ON jobs (queue, state, run_at)
  WHERE state IN ('queued', 'leased');

CREATE INDEX jobs_lease_idx
  ON jobs (queue, leased_until)
  WHERE state = 'leased';
