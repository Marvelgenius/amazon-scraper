CREATE TABLE IF NOT EXISTS staging.stg_product_snapshot (
    raw_event_id BIGINT PRIMARY KEY,
    ingest_date DATE NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    endpoint_name TEXT NOT NULL,
    marketplace_country TEXT,
    account_name TEXT NOT NULL,
    asin TEXT NOT NULL,
    product_title TEXT,
    brand TEXT,
    currency TEXT,
    price NUMERIC(18, 4),
    star_rating NUMERIC(8, 4),
    num_ratings INTEGER,
    num_reviews INTEGER,
    sales_volume_raw TEXT,
    category_id TEXT,
    category_name TEXT,
    segment_name TEXT,
    segment_keyword TEXT,
    is_best_seller BOOLEAN NOT NULL DEFAULT FALSE,
    is_prime BOOLEAN NOT NULL DEFAULT FALSE,
    product_url TEXT,
    image_url TEXT,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_product_snapshot_asin_date
    ON staging.stg_product_snapshot (asin, ingest_date DESC);

CREATE INDEX IF NOT EXISTS idx_stg_product_snapshot_segment
    ON staging.stg_product_snapshot (segment_name, ingest_date DESC);

CREATE TABLE IF NOT EXISTS staging.stg_review_event (
    raw_event_id BIGINT PRIMARY KEY,
    ingest_date DATE NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    marketplace_country TEXT,
    asin TEXT NOT NULL,
    review_id TEXT,
    review_title TEXT,
    review_comment TEXT,
    review_star_rating NUMERIC(8, 4),
    review_date_text TEXT,
    is_verified_purchase BOOLEAN NOT NULL DEFAULT FALSE,
    helpful_count INTEGER,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_review_event_asin_date
    ON staging.stg_review_event (asin, ingest_date DESC);

CREATE TABLE IF NOT EXISTS staging.stg_offer_snapshot (
    raw_event_id BIGINT PRIMARY KEY,
    ingest_date DATE NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    source_system TEXT NOT NULL,
    marketplace_country TEXT,
    asin TEXT NOT NULL,
    offer_price NUMERIC(18, 4),
    original_price NUMERIC(18, 4),
    discount_pct NUMERIC(8, 4),
    product_condition TEXT,
    is_prime BOOLEAN NOT NULL DEFAULT FALSE,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_stg_offer_snapshot_asin_date
    ON staging.stg_offer_snapshot (asin, ingest_date DESC);
