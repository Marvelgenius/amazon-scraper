CREATE TABLE IF NOT EXISTS mart.mart_product_daily_metrics (
    observed_date DATE NOT NULL,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT NOT NULL,
    asin TEXT NOT NULL,
    brand TEXT,
    category_id TEXT,
    category_name TEXT,
    segment_name TEXT,
    product_title TEXT,
    current_price NUMERIC(18, 4),
    original_price NUMERIC(18, 4),
    discount_pct NUMERIC(8, 4),
    bsr_rank INTEGER,
    avg_star_rating NUMERIC(8, 4),
    review_count INTEGER,
    is_best_seller BOOLEAN NOT NULL DEFAULT FALSE,
    is_prime BOOLEAN NOT NULL DEFAULT FALSE,
    latest_snapshot_at TIMESTAMPTZ,
    PRIMARY KEY (observed_date, platform_code, marketplace_code, asin)
);

CREATE INDEX IF NOT EXISTS idx_mart_product_daily_metrics_category
    ON mart.mart_product_daily_metrics (category_id, observed_date DESC);

CREATE INDEX IF NOT EXISTS idx_mart_product_daily_metrics_segment
    ON mart.mart_product_daily_metrics (segment_name, observed_date DESC);

CREATE TABLE IF NOT EXISTS mart.mart_brand_market_share (
    observed_date DATE NOT NULL,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT NOT NULL,
    category_id TEXT NOT NULL,
    category_name TEXT,
    brand TEXT NOT NULL,
    asin_count INTEGER NOT NULL,
    avg_price NUMERIC(18, 4),
    avg_star_rating NUMERIC(8, 4),
    review_count BIGINT,
    brand_share_pct NUMERIC(8, 4),
    PRIMARY KEY (observed_date, platform_code, marketplace_code, category_id, brand)
);

CREATE TABLE IF NOT EXISTS mart.mart_segment_market_share (
    observed_date DATE NOT NULL,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT NOT NULL,
    segment_name TEXT NOT NULL,
    brand TEXT NOT NULL,
    asin_count INTEGER NOT NULL,
    avg_price NUMERIC(18, 4),
    avg_star_rating NUMERIC(8, 4),
    review_count BIGINT,
    brand_share_pct NUMERIC(8, 4),
    PRIMARY KEY (observed_date, platform_code, marketplace_code, segment_name, brand)
);
