ALTER TABLE jobs
ADD COLUMN priority INT NOT NULL DEFAULT 0;

-- Better dequeue order: higher priority first, then time
CREATE INDEX jobs_dequeue_priority_idx
  ON jobs (queue, state, priority DESC, run_at, created_at)
  WHERE state IN ('queued','leased');
