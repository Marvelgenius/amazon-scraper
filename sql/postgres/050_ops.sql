CREATE TABLE IF NOT EXISTS ops.job_run (
    job_run_id BIGSERIAL PRIMARY KEY,
    job_name TEXT NOT NULL,
    job_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL,
    rows_written BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ops_job_run_name_started
    ON ops.job_run (job_name, started_at DESC);

CREATE TABLE IF NOT EXISTS ops.api_request_log (
    request_id TEXT PRIMARY KEY,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    request_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    marketplace_country TEXT,
    response_status_code INTEGER,
    retry_count INTEGER NOT NULL DEFAULT 0,
    rows_written INTEGER NOT NULL DEFAULT 0,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS ops.rate_limit_event (
    rate_limit_event_id BIGSERIAL PRIMARY KEY,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    request_id TEXT,
    status_code INTEGER,
    retry_after_seconds INTEGER,
    remaining_quota NUMERIC(18, 4),
    event_message TEXT,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.pagination_checkpoint (
    checkpoint_id BIGSERIAL PRIMARY KEY,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    account_name TEXT NOT NULL,
    marketplace_country TEXT,
    cursor_value TEXT,
    page_number INTEGER,
    request_fingerprint TEXT NOT NULL,
    last_success_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checkpoint_payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (source_system, endpoint_name, account_name, request_fingerprint)
);

CREATE TABLE IF NOT EXISTS ops.ingest_dead_letter (
    dead_letter_id BIGSERIAL PRIMARY KEY,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    request_id TEXT,
    payload_json JSONB NOT NULL,
    error_message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ops.schema_drift_event (
    schema_drift_event_id BIGSERIAL PRIMARY KEY,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    drift_type TEXT NOT NULL,
    details_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    detected_at TIMESTAMPTZ NOT NULL
);
