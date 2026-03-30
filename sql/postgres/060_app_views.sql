CREATE SCHEMA IF NOT EXISTS gurysk_app;

DROP VIEW IF EXISTS gurysk_app.v_trend_alert;
DROP VIEW IF EXISTS gurysk_app.v_segment_estimation_stats;
DROP VIEW IF EXISTS gurysk_app.v_segment_market_share;
DROP VIEW IF EXISTS gurysk_app.v_brand_market_share;
DROP VIEW IF EXISTS gurysk_app.v_daily_sales_estimate;
DROP VIEW IF EXISTS gurysk_app.v_segment_product_daily_metrics;
DROP VIEW IF EXISTS gurysk_app.v_product_daily_metrics;

ALTER TABLE mart.mart_brand_market_share
    ADD COLUMN IF NOT EXISTS total_estimated_daily_sales NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS total_num_ratings BIGINT,
    ADD COLUMN IF NOT EXISTS total_sales_volume BIGINT,
    ADD COLUMN IF NOT EXISTS rating_share_pct NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS asin_share_pct NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ;

ALTER TABLE staging.stg_product_snapshot
    ADD COLUMN IF NOT EXISTS bsr_rank INTEGER;

ALTER TABLE staging.stg_product_snapshot
    ADD COLUMN IF NOT EXISTS segment_root_keyword TEXT,
    ADD COLUMN IF NOT EXISTS search_query TEXT,
    ADD COLUMN IF NOT EXISTS query_source TEXT,
    ADD COLUMN IF NOT EXISTS sample_round INTEGER,
    ADD COLUMN IF NOT EXISTS search_page INTEGER,
    ADD COLUMN IF NOT EXISTS result_rank INTEGER,
    ADD COLUMN IF NOT EXISTS target_brand TEXT;

ALTER TABLE core.fact_inventory_snapshot
    ADD COLUMN IF NOT EXISTS bsr_rank INTEGER;

ALTER TABLE mart.mart_product_daily_metrics
    ADD COLUMN IF NOT EXISTS bsr_rank INTEGER;

ALTER TABLE mart.mart_segment_market_share
    ADD COLUMN IF NOT EXISTS total_estimated_daily_sales NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS total_num_ratings BIGINT,
    ADD COLUMN IF NOT EXISTS total_sales_volume BIGINT,
    ADD COLUMN IF NOT EXISTS rating_share_pct NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS asin_share_pct NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS eb_adjusted_share_pct NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS sample_asin_count INTEGER,
    ADD COLUMN IF NOT EXISTS estimated_market_asin_count INTEGER,
    ADD COLUMN IF NOT EXISTS bootstrap_mean_sales NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS bootstrap_lower_bound NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS bootstrap_upper_bound NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS coverage_ratio NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS stability_score NUMERIC(8, 4),
    ADD COLUMN IF NOT EXISTS last_observed_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS mart.mart_daily_sales_estimate (
    estimate_date DATE NOT NULL,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT NOT NULL,
    asin TEXT NOT NULL,
    brand TEXT,
    category_id TEXT,
    category_name TEXT,
    segment_name TEXT,
    product_title TEXT,
    sales_volume_raw TEXT,
    sales_volume_num INTEGER,
    bsr_rank INTEGER,
    num_ratings INTEGER,
    num_ratings_delta INTEGER,
    price NUMERIC(18, 4),
    star_rating NUMERIC(8, 4),
    estimated_daily_sales INTEGER,
    estimate_lower_bound INTEGER,
    estimate_upper_bound INTEGER,
    estimation_method TEXT,
    confidence_score NUMERIC(8, 4),
    PRIMARY KEY (estimate_date, platform_code, marketplace_code, asin)
);

CREATE INDEX IF NOT EXISTS idx_mart_daily_sales_estimate_category
    ON mart.mart_daily_sales_estimate (category_id, estimate_date DESC);

CREATE INDEX IF NOT EXISTS idx_mart_daily_sales_estimate_segment
    ON mart.mart_daily_sales_estimate (segment_name, estimate_date DESC);

CREATE TABLE IF NOT EXISTS mart.mart_segment_estimation_stats (
    observed_date DATE NOT NULL,
    platform_code TEXT NOT NULL,
    marketplace_code TEXT NOT NULL,
    segment_name TEXT NOT NULL,
    sample_asin_count INTEGER NOT NULL,
    estimated_market_asin_count INTEGER NOT NULL,
    sample_sales_sum NUMERIC(18, 4),
    bootstrap_mean_sales NUMERIC(18, 4),
    bootstrap_lower_bound NUMERIC(18, 4),
    bootstrap_upper_bound NUMERIC(18, 4),
    coverage_ratio NUMERIC(8, 4),
    stability_score NUMERIC(8, 4),
    eb_prior_strength NUMERIC(18, 4),
    last_observed_at TIMESTAMPTZ,
    PRIMARY KEY (observed_date, platform_code, marketplace_code, segment_name)
);

CREATE TABLE IF NOT EXISTS ops.trend_alert (
    alert_date DATE NOT NULL,
    marketplace_code TEXT NOT NULL,
    dimension_type TEXT NOT NULL,
    dimension_value TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    current_value NUMERIC(18, 4),
    baseline_value NUMERIC(18, 4),
    change_pct NUMERIC(18, 4),
    z_score NUMERIC(18, 4),
    alert_level TEXT NOT NULL,
    alert_message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (alert_date, marketplace_code, dimension_type, dimension_value, metric_name)
);

CREATE OR REPLACE VIEW gurysk_app.v_product_daily_metrics AS
SELECT
    m.observed_date,
    m.marketplace_code AS marketplace,
    m.asin,
    m.brand,
    m.category_id,
    m.category_name,
    m.segment_name,
    m.product_title,
    m.current_price AS price,
    m.original_price,
    m.discount_pct,
    m.bsr_rank,
    m.avg_star_rating AS star_rating,
    m.review_count AS num_ratings,
    m.review_count AS num_reviews,
    COALESCE(e.sales_volume_num, NULL) AS sales_volume_num,
    m.is_best_seller,
    m.is_prime,
    m.latest_snapshot_at
FROM mart.mart_product_daily_metrics m
LEFT JOIN mart.mart_daily_sales_estimate e
  ON m.observed_date = e.estimate_date
 AND m.platform_code = e.platform_code
 AND m.marketplace_code = e.marketplace_code
 AND m.asin = e.asin;

CREATE OR REPLACE VIEW gurysk_app.v_segment_product_daily_metrics AS
SELECT
    m.observed_date,
    m.marketplace_code AS marketplace,
    m.asin,
    m.brand,
    m.segment_name,
    NULL::text AS segment_keyword,
    m.product_title,
    m.current_price AS price,
    m.original_price,
    m.discount_pct,
    m.avg_star_rating AS star_rating,
    m.review_count AS num_ratings,
    m.review_count AS num_reviews,
    COALESCE(e.sales_volume_num, NULL) AS sales_volume_num,
    m.latest_snapshot_at
FROM mart.mart_product_daily_metrics m
LEFT JOIN mart.mart_daily_sales_estimate e
  ON m.observed_date = e.estimate_date
 AND m.platform_code = e.platform_code
 AND m.marketplace_code = e.marketplace_code
 AND m.asin = e.asin
WHERE m.segment_name IS NOT NULL
  AND m.segment_name <> '';

CREATE OR REPLACE VIEW gurysk_app.v_daily_sales_estimate AS
SELECT
    estimate_date,
    marketplace_code AS marketplace,
    asin,
    brand,
    category_id,
    category_name,
    segment_name,
    product_title,
    sales_volume_raw,
    sales_volume_num,
    bsr_rank,
    num_ratings,
    num_ratings_delta,
    price,
    star_rating,
    estimated_daily_sales,
    estimate_lower_bound,
    estimate_upper_bound,
    estimation_method,
    confidence_score
FROM mart.mart_daily_sales_estimate;

CREATE OR REPLACE VIEW gurysk_app.v_brand_market_share AS
SELECT
    observed_date,
    marketplace_code AS marketplace,
    category_id,
    category_name,
    brand,
    asin_count AS distinct_asins,
    total_estimated_daily_sales,
    total_num_ratings,
    total_sales_volume,
    avg_price,
    avg_star_rating,
    brand_share_pct AS sales_share_pct,
    rating_share_pct,
    asin_share_pct,
    last_observed_at
FROM mart.mart_brand_market_share;

CREATE OR REPLACE VIEW gurysk_app.v_segment_market_share AS
SELECT
    observed_date,
    marketplace_code AS marketplace,
    segment_name,
    brand,
    asin_count AS distinct_asins,
    total_estimated_daily_sales,
    total_num_ratings,
    total_sales_volume,
    avg_price,
    avg_star_rating,
    brand_share_pct AS sales_share_pct,
    rating_share_pct,
    asin_share_pct,
    eb_adjusted_share_pct,
    sample_asin_count,
    estimated_market_asin_count,
    bootstrap_mean_sales,
    bootstrap_lower_bound,
    bootstrap_upper_bound,
    coverage_ratio,
    stability_score,
    last_observed_at
FROM mart.mart_segment_market_share;

CREATE OR REPLACE VIEW gurysk_app.v_segment_estimation_stats AS
SELECT
    observed_date,
    marketplace_code AS marketplace,
    segment_name,
    sample_asin_count,
    estimated_market_asin_count,
    sample_sales_sum,
    bootstrap_mean_sales,
    bootstrap_lower_bound,
    bootstrap_upper_bound,
    coverage_ratio,
    stability_score,
    eb_prior_strength,
    last_observed_at
FROM mart.mart_segment_estimation_stats;

CREATE OR REPLACE VIEW gurysk_app.v_trend_alert AS
SELECT
    alert_date,
    marketplace_code AS marketplace,
    dimension_type,
    dimension_value,
    metric_name,
    current_value,
    baseline_value,
    change_pct,
    z_score,
    alert_level,
    alert_message,
    created_at
FROM ops.trend_alert;
