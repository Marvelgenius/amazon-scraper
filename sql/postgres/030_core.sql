CREATE TABLE IF NOT EXISTS core.dim_platform (
    platform_id BIGSERIAL PRIMARY KEY,
    platform_code TEXT NOT NULL UNIQUE,
    platform_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.dim_account (
    account_id BIGSERIAL PRIMARY KEY,
    platform_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    UNIQUE (platform_code, account_name)
);

CREATE TABLE IF NOT EXISTS core.dim_marketplace (
    marketplace_id BIGSERIAL PRIMARY KEY,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT NOT NULL,
    marketplace_name TEXT,
    UNIQUE (platform_code, marketplace_code)
);

CREATE TABLE IF NOT EXISTS core.dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    platform_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    asin TEXT NOT NULL,
    product_title TEXT,
    brand TEXT,
    category_id TEXT,
    category_name TEXT,
    first_seen_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ,
    UNIQUE (platform_code, account_name, asin)
);

CREATE TABLE IF NOT EXISTS core.bridge_product_identifier (
    bridge_id BIGSERIAL PRIMARY KEY,
    platform_code TEXT NOT NULL,
    identifier_type TEXT NOT NULL,
    identifier_value TEXT NOT NULL,
    asin TEXT NOT NULL,
    UNIQUE (platform_code, identifier_type, identifier_value)
);

CREATE TABLE IF NOT EXISTS core.fact_inventory_snapshot (
    inventory_snapshot_id BIGSERIAL,
    snapshot_date DATE NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL,
    platform_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    marketplace_code TEXT,
    asin TEXT NOT NULL,
    category_id TEXT,
    category_name TEXT,
    bsr_rank INTEGER,
    segment_name TEXT,
    segment_keyword TEXT,
    inventory_status TEXT,
    is_prime BOOLEAN NOT NULL DEFAULT FALSE,
    is_best_seller BOOLEAN NOT NULL DEFAULT FALSE,
    raw_event_id BIGINT,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (inventory_snapshot_id, snapshot_date),
    UNIQUE (snapshot_date, idempotency_key)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS core.fact_inventory_snapshot_default
PARTITION OF core.fact_inventory_snapshot DEFAULT;

CREATE INDEX IF NOT EXISTS idx_core_fact_inventory_snapshot_asin
    ON core.fact_inventory_snapshot (asin, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS core.fact_price_snapshot (
    price_snapshot_id BIGSERIAL,
    snapshot_date DATE NOT NULL,
    snapshot_at TIMESTAMPTZ NOT NULL,
    platform_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    marketplace_code TEXT,
    asin TEXT NOT NULL,
    category_id TEXT,
    category_name TEXT,
    current_price NUMERIC(18, 4),
    original_price NUMERIC(18, 4),
    currency TEXT,
    discount_pct NUMERIC(8, 4),
    raw_event_id BIGINT,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (price_snapshot_id, snapshot_date),
    UNIQUE (snapshot_date, idempotency_key)
) PARTITION BY RANGE (snapshot_date);

CREATE TABLE IF NOT EXISTS core.fact_price_snapshot_default
PARTITION OF core.fact_price_snapshot DEFAULT;

CREATE INDEX IF NOT EXISTS idx_core_fact_price_snapshot_asin
    ON core.fact_price_snapshot (asin, snapshot_date DESC);

CREATE TABLE IF NOT EXISTS core.fact_review (
    review_fact_id BIGSERIAL,
    review_date DATE NOT NULL,
    reviewed_at TIMESTAMPTZ NOT NULL,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT,
    asin TEXT NOT NULL,
    review_id TEXT NOT NULL,
    review_title TEXT,
    review_comment TEXT,
    review_star_rating NUMERIC(8, 4),
    helpful_count INTEGER,
    is_verified_purchase BOOLEAN NOT NULL DEFAULT FALSE,
    raw_event_id BIGINT,
    request_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (review_fact_id, review_date),
    UNIQUE (review_date, idempotency_key)
) PARTITION BY RANGE (review_date);

CREATE TABLE IF NOT EXISTS core.fact_review_default
PARTITION OF core.fact_review DEFAULT;

CREATE INDEX IF NOT EXISTS idx_core_fact_review_asin
    ON core.fact_review (asin, review_date DESC);

CREATE TABLE IF NOT EXISTS core.fact_order_event (
    order_event_id BIGSERIAL,
    event_date DATE NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    platform_code TEXT NOT NULL,
    account_name TEXT NOT NULL,
    marketplace_code TEXT,
    asin TEXT,
    order_id TEXT,
    order_status TEXT,
    quantity INTEGER,
    gross_amount NUMERIC(18, 4),
    currency TEXT,
    source_record_id TEXT,
    source_updated_at TIMESTAMPTZ,
    request_id TEXT,
    idempotency_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (order_event_id, event_date),
    UNIQUE (event_date, idempotency_key)
) PARTITION BY RANGE (event_date);

CREATE TABLE IF NOT EXISTS core.fact_order_event_default
PARTITION OF core.fact_order_event DEFAULT;
