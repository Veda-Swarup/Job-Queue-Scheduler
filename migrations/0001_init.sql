CREATE TABLE jobs (
    id BIGSERIAL PRIMARY KEY,
    queue_name TEXT NOT NULL DEFAULT 'default',
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    locked_by TEXT,
    locked_at TIMESTAMPTZ,
    last_heartbeat TIMESTAMPTZ,
    lock_version INT NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_jobs_claimable ON jobs (queue_name, priority DESC, run_at)
WHERE status = 'pending';
