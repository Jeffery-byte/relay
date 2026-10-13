CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    numbers JSONB NOT NULL CHECK (jsonb_typeof(numbers) = 'array'),
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'completed', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    result JSONB,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (attempts <= max_attempts),
    CHECK ((status = 'completed') = (result IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS job_attempts (
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt INTEGER NOT NULL,
    error TEXT,
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, attempt)
);

-- Written in the same transaction as job creation or a failed attempt.
CREATE TABLE IF NOT EXISTS outbox (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(available_at)
    WHERE published_at IS NULL;
