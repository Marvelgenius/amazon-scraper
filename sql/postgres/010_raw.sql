CREATE TABLE IF NOT EXISTS raw.api_ingest_event (
    raw_event_id BIGSERIAL,
    ingest_date DATE NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    request_id TEXT NOT NULL,
    request_fingerprint TEXT NOT NULL,
    marketplace_country TEXT,
    account_name TEXT NOT NULL DEFAULT 'default',
    source_record_id TEXT,
    source_updated_at TIMESTAMPTZ,
    business_id TEXT,
    idempotency_key TEXT NOT NULL,
    request_params_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_body_jsonb JSONB NOT NULL,
    s3_bucket TEXT,
    s3_key TEXT,
    s3_etag TEXT,
    s3_version_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (raw_event_id, ingest_date),
    UNIQUE (ingest_date, idempotency_key)
) PARTITION BY RANGE (ingest_date);

CREATE TABLE IF NOT EXISTS raw.api_ingest_event_default
PARTITION OF raw.api_ingest_event DEFAULT;

CREATE INDEX IF NOT EXISTS idx_raw_api_ingest_event_request_id
    ON raw.api_ingest_event (request_id);

CREATE INDEX IF NOT EXISTS idx_raw_api_ingest_event_endpoint
    ON raw.api_ingest_event (source_system, endpoint_name, fetched_at DESC);

CREATE INDEX IF NOT EXISTS idx_raw_api_ingest_event_business
    ON raw.api_ingest_event (business_id, fetched_at DESC);
