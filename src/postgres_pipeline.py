from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

from psycopg2.extras import Json

from .aws_storage import (
    build_idempotency_key,
    build_request_fingerprint,
    build_request_id,
    build_s3_key,
    put_json_to_s3,
)
from .debug_runtime import debug_log
from .postgres_database import bulk_insert_raw_events, execute_sql_file

SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "postgres"
SQL_FILES = [
    "001_init_schemas.sql",
    "010_raw.sql",
    "020_staging.sql",
    "030_core.sql",
    "040_mart.sql",
    "050_ops.sql",
    "060_app_views.sql",
]


def ensure_postgres_data_platform(connection) -> None:
    for filename in SQL_FILES:
        execute_sql_file(connection, SQL_DIR / filename)


def _normalize_raw_row(row: Dict[str, Any]) -> Dict[str, Any]:
    fetched_at = row["record_create_timestamp"]
    request_params = row.get("request_metadata") or {}
    source_system = row.get("source_system", "amazon")
    endpoint_name = row.get("endpoint_name") or row.get("source_endpoint")
    source_record_id = row.get("source_record_id") or row.get("asin")
    request_id = row.get("request_id") or build_request_id()
    ingest_date = row.get("ingest_date") or fetched_at.date()
    s3_key = row.get("s3_key") or build_s3_key(
        source_system=source_system,
        endpoint_name=endpoint_name,
        ingest_date=ingest_date,
        request_id=request_id,
        source_record_id=source_record_id,
        business_domain=request_params.get("business_domain"),
        dataset_name=request_params.get("dataset_name"),
    )
    s3_result = put_json_to_s3(row["api_payload"], s3_key)
    normalized = {
        "source_system": source_system,
        "endpoint_name": endpoint_name,
        "request_id": request_id,
        "request_fingerprint": row.get("request_fingerprint")
        or build_request_fingerprint(
            source_system=source_system,
            endpoint_name=endpoint_name,
            marketplace_country=row.get("marketplace_country", ""),
            request_params=request_params,
        ),
        "ingest_date": ingest_date,
        "fetched_at": fetched_at,
        "marketplace_country": row.get("marketplace_country", ""),
        "account_name": str(request_params.get("account", "default")),
        "source_record_id": source_record_id,
        "source_updated_at": row.get("source_updated_at"),
        "business_id": row.get("business_id") or row.get("asin") or source_record_id,
        "idempotency_key": row.get("idempotency_key") or build_idempotency_key(row),
        "request_params_json": request_params,
        "response_body_jsonb": row["api_payload"],
        "s3_bucket": s3_result["bucket"],
        "s3_key": s3_result["key"],
        "s3_etag": s3_result["etag"],
        "s3_version_id": s3_result["version_id"],
    }
    return normalized


def _upsert_api_request_logs(connection, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    request_rows: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        request_id = row["request_id"]
        bucket = request_rows.setdefault(
            request_id,
            {
                "request_id": request_id,
                "source_system": row["source_system"],
                "endpoint_name": row["endpoint_name"],
                "request_fingerprint": row["request_fingerprint"],
                "request_params_json": row["request_params_json"],
                "marketplace_country": row["marketplace_country"],
                "response_status_code": 200,
                "retry_count": 0,
                "rows_written": 0,
                "started_at": row["fetched_at"],
                "completed_at": row["fetched_at"],
                "status": "success",
            },
        )
        bucket["rows_written"] += 1
        if row["fetched_at"] < bucket["started_at"]:
            bucket["started_at"] = row["fetched_at"]
        if row["fetched_at"] > bucket["completed_at"]:
            bucket["completed_at"] = row["fetched_at"]

    sql = """
        INSERT INTO ops.api_request_log (
            request_id,
            source_system,
            endpoint_name,
            request_fingerprint,
            request_params_json,
            marketplace_country,
            response_status_code,
            retry_count,
            rows_written,
            started_at,
            completed_at,
            status
        )
        VALUES (
            %(request_id)s,
            %(source_system)s,
            %(endpoint_name)s,
            %(request_fingerprint)s,
            %(request_params_json)s,
            %(marketplace_country)s,
            %(response_status_code)s,
            %(retry_count)s,
            %(rows_written)s,
            %(started_at)s,
            %(completed_at)s,
            %(status)s
        )
        ON CONFLICT (request_id) DO UPDATE SET
            rows_written = EXCLUDED.rows_written,
            completed_at = EXCLUDED.completed_at,
            status = EXCLUDED.status
    """
    payloads = []
    for request in request_rows.values():
        request_copy = dict(request)
        request_copy["request_params_json"] = Json(request_copy["request_params_json"])
        payloads.append(request_copy)

    with connection.cursor() as cursor:
        cursor.executemany(sql, payloads)
    connection.commit()
    return len(payloads)


def _write_dead_letters(connection, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
        INSERT INTO ops.ingest_dead_letter (
            source_system,
            endpoint_name,
            request_id,
            payload_json,
            error_message
        ) VALUES (%s, %s, %s, %s, %s)
    """
    values = [
        (
            row["source_system"],
            row["endpoint_name"],
            row.get("request_id"),
            Json(row.get("payload_json") or {}),
            row["error_message"],
        )
        for row in rows
    ]
    with connection.cursor() as cursor:
        cursor.executemany(sql, values)
    connection.commit()
    return len(values)


def store_raw_api_events(connection, rows: Iterable[Dict[str, Any]]) -> int:
    ensure_postgres_data_platform(connection)
    normalized_rows: List[Dict[str, Any]] = []
    dead_letters: List[Dict[str, Any]] = []
    for row in rows:
        try:
            normalized_rows.append(_normalize_raw_row(row))
        except Exception as exc:
            dead_letters.append(
                {
                    "source_system": row.get("source_system", "amazon"),
                    "endpoint_name": row.get("endpoint_name") or row.get("source_endpoint", "unknown"),
                    "request_id": row.get("request_id"),
                    "payload_json": row.get("api_payload"),
                    "error_message": str(exc),
                }
            )
    inserted = bulk_insert_raw_events(connection, normalized_rows)
    _upsert_api_request_logs(connection, normalized_rows)
    _write_dead_letters(connection, dead_letters)
    # region agent log
    debug_log(
        hypothesis_id="H4",
        location="src/postgres_pipeline.py:store_raw_api_events",
        message="Raw event storage summary",
        data={
            "normalized_rows": len(normalized_rows),
            "dead_letters": len(dead_letters),
            "inserted": inserted,
            "sample_endpoint": normalized_rows[0]["endpoint_name"] if normalized_rows else None,
            "sample_marketplace": normalized_rows[0]["marketplace_country"] if normalized_rows else None,
            "sample_payload_keys": (
                sorted(normalized_rows[0]["response_body_jsonb"].keys())[:12]
                if normalized_rows and isinstance(normalized_rows[0].get("response_body_jsonb"), dict)
                else []
            ),
        },
    )
    # endregion
    return inserted


def start_job_run(connection, job_name: str, job_params: Optional[Dict[str, Any]] = None) -> int:
    ensure_postgres_data_platform(connection)
    sql = """
        INSERT INTO ops.job_run (
            job_name,
            job_params_json,
            status,
            started_at
        )
        VALUES (%s, %s, 'running', NOW())
        RETURNING job_run_id
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (job_name, Json(job_params or {})))
        job_run_id = cursor.fetchone()[0]
    connection.commit()
    return int(job_run_id)


def finish_job_run(
    connection,
    job_run_id: int,
    status: str,
    rows_written: int = 0,
    error_message: Optional[str] = None,
) -> None:
    sql = """
        UPDATE ops.job_run
        SET status = %s,
            rows_written = %s,
            error_message = %s,
            finished_at = NOW()
        WHERE job_run_id = %s
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (status, rows_written, error_message, job_run_id))
    connection.commit()


@contextmanager
def logged_job(connection, job_name: str, job_params: Optional[Dict[str, Any]] = None):
    job_run_id = start_job_run(connection, job_name=job_name, job_params=job_params)
    try:
        yield job_run_id
        finish_job_run(connection, job_run_id, status="success")
    except Exception as exc:
        finish_job_run(connection, job_run_id, status="failed", error_message=str(exc))
        raise


def _execute_insert(connection, sql: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(sql)
        rowcount = cursor.rowcount
    connection.commit()
    return max(rowcount, 0)


def build_staging_product_snapshot(connection) -> int:
    sql = """
        INSERT INTO staging.stg_product_snapshot (
            raw_event_id,
            ingest_date,
            fetched_at,
            source_system,
            endpoint_name,
            marketplace_country,
            account_name,
            asin,
            product_title,
            brand,
            currency,
            price,
            star_rating,
            num_ratings,
            num_reviews,
            sales_volume_raw,
            category_id,
            category_name,
            segment_name,
            segment_keyword,
            is_best_seller,
            is_prime,
            product_url,
            image_url,
            request_id,
            idempotency_key
        )
        SELECT
            raw_event_id,
            ingest_date,
            fetched_at,
            source_system,
            endpoint_name,
            marketplace_country,
            account_name,
            COALESCE(source_record_id, business_id),
            NULLIF(response_body_jsonb ->> 'product_title', ''),
            NULLIF(
                COALESCE(
                    response_body_jsonb -> 'product_information' ->> 'Brand',
                    response_body_jsonb -> 'product_information' ->> 'brand',
                    response_body_jsonb ->> 'brand',
                    NULLIF(
                        REGEXP_REPLACE(
                            split_part(
                                COALESCE(
                                    response_body_jsonb ->> 'product_byline',
                                    response_body_jsonb ->> 'product_title',
                                    ''
                                ),
                                ' ',
                                1
                            ),
                            '(^[[:punct:]]+|[[:punct:]]+$)',
                            '',
                            'g'
                        ),
                        ''
                    )
                ),
                ''
            ),
            NULLIF(response_body_jsonb ->> 'currency', ''),
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_price', response_body_jsonb ->> 'price', ''), '[^0-9.]', '', 'g'), '')::numeric,
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_star_rating', response_body_jsonb ->> 'rating', ''), '[^0-9.]', '', 'g'), '')::numeric,
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_num_ratings', response_body_jsonb ->> 'reviews', ''), '[^0-9]', '', 'g'), '')::integer,
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_num_reviews', ''), '[^0-9]', '', 'g'), '')::integer,
            NULLIF(response_body_jsonb ->> 'sales_volume', ''),
            COALESCE(
                response_body_jsonb #>> '{category_path,-1,id}',
                response_body_jsonb -> 'category' ->> 'id',
                NULLIF(request_params_json ->> 'category_id', ''),
                NULLIF(split_part(NULLIF(request_params_json ->> 'search_query', ''), ':', 2), '')
            ),
            COALESCE(
                response_body_jsonb #>> '{category_path,-1,name}',
                response_body_jsonb -> 'category' ->> 'name',
                request_params_json ->> 'category_name'
            ),
            request_params_json ->> 'segment_name',
            request_params_json ->> 'segment_keyword',
            COALESCE((response_body_jsonb ->> 'is_best_seller')::boolean, false),
            COALESCE((response_body_jsonb ->> 'is_prime')::boolean, false),
            NULLIF(COALESCE(response_body_jsonb ->> 'product_url', response_body_jsonb ->> 'link'), ''),
            NULLIF(COALESCE(response_body_jsonb ->> 'product_photo', response_body_jsonb ->> 'featured_image'), ''),
            request_id,
            idempotency_key
        FROM raw.api_ingest_event
        WHERE endpoint_name IN (
            'product_search',
            'segment_search',
            'product_details',
            'products_by_category',
            'best_sellers'
        )
        ON CONFLICT (raw_event_id) DO UPDATE SET
            fetched_at = EXCLUDED.fetched_at,
            price = EXCLUDED.price,
            star_rating = EXCLUDED.star_rating,
            num_ratings = EXCLUDED.num_ratings,
            num_reviews = EXCLUDED.num_reviews,
            sales_volume_raw = EXCLUDED.sales_volume_raw
    """
    return _execute_insert(connection, sql)


def build_staging_review_event(connection) -> int:
    sql = """
        INSERT INTO staging.stg_review_event (
            raw_event_id,
            ingest_date,
            fetched_at,
            source_system,
            marketplace_country,
            asin,
            review_id,
            review_title,
            review_comment,
            review_star_rating,
            review_date_text,
            is_verified_purchase,
            helpful_count,
            request_id,
            idempotency_key
        )
        SELECT
            raw_event_id,
            ingest_date,
            fetched_at,
            source_system,
            marketplace_country,
            COALESCE(source_record_id, business_id),
            NULLIF(response_body_jsonb ->> 'review_id', ''),
            NULLIF(response_body_jsonb ->> 'review_title', ''),
            NULLIF(response_body_jsonb ->> 'review_comment', ''),
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'review_star_rating', ''), '[^0-9.]', '', 'g'), '')::numeric,
            NULLIF(response_body_jsonb ->> 'review_date', ''),
            COALESCE((response_body_jsonb ->> 'is_verified_purchase')::boolean, false),
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'helpful_count', ''), '[^0-9]', '', 'g'), '')::integer,
            request_id,
            idempotency_key
        FROM raw.api_ingest_event
        WHERE endpoint_name = 'top_product_reviews'
        ON CONFLICT (raw_event_id) DO NOTHING
    """
    return _execute_insert(connection, sql)


def build_staging_offer_snapshot(connection) -> int:
    sql = """
        INSERT INTO staging.stg_offer_snapshot (
            raw_event_id,
            ingest_date,
            fetched_at,
            source_system,
            marketplace_country,
            asin,
            offer_price,
            original_price,
            discount_pct,
            product_condition,
            is_prime,
            request_id,
            idempotency_key
        )
        SELECT
            raw_event_id,
            ingest_date,
            fetched_at,
            source_system,
            marketplace_country,
            COALESCE(source_record_id, business_id),
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_price', response_body_jsonb ->> 'price', ''), '[^0-9.]', '', 'g'), '')::numeric,
            NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_original_price', response_body_jsonb ->> 'list_price', ''), '[^0-9.]', '', 'g'), '')::numeric,
            CASE
                WHEN NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_original_price', response_body_jsonb ->> 'list_price', ''), '[^0-9.]', '', 'g'), '')::numeric > 0
                THEN ROUND(
                    (1 - (
                        NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_price', response_body_jsonb ->> 'price', ''), '[^0-9.]', '', 'g'), '')::numeric
                        /
                        NULLIF(REGEXP_REPLACE(COALESCE(response_body_jsonb ->> 'product_original_price', response_body_jsonb ->> 'list_price', ''), '[^0-9.]', '', 'g'), '')::numeric
                    )) * 100,
                    2
                )
                ELSE NULL
            END,
            NULLIF(response_body_jsonb ->> 'product_condition', ''),
            COALESCE((response_body_jsonb ->> 'is_prime')::boolean, false),
            request_id,
            idempotency_key
        FROM raw.api_ingest_event
        WHERE endpoint_name = 'product_offers'
        ON CONFLICT (raw_event_id) DO NOTHING
    """
    return _execute_insert(connection, sql)


def build_core_dimensions(connection) -> int:
    rowcount = 0
    statements = [
        """
        INSERT INTO core.dim_platform (platform_code, platform_name)
        SELECT DISTINCT source_system, INITCAP(source_system)
        FROM raw.api_ingest_event
        ON CONFLICT (platform_code) DO NOTHING
        """,
        """
        INSERT INTO core.dim_account (platform_code, account_name)
        SELECT DISTINCT source_system, account_name
        FROM raw.api_ingest_event
        ON CONFLICT (platform_code, account_name) DO NOTHING
        """,
        """
        INSERT INTO core.dim_marketplace (platform_code, marketplace_code, marketplace_name)
        SELECT DISTINCT source_system, marketplace_country, marketplace_country
        FROM raw.api_ingest_event
        WHERE marketplace_country IS NOT NULL AND marketplace_country <> ''
        ON CONFLICT (platform_code, marketplace_code) DO NOTHING
        """,
        """
        INSERT INTO core.dim_product (
            platform_code,
            account_name,
            asin,
            product_title,
            brand,
            category_id,
            category_name,
            first_seen_at,
            last_seen_at
        )
        SELECT
            source_system,
            account_name,
            asin,
            MAX(product_title),
            MAX(brand),
            MAX(category_id),
            MAX(category_name),
            MIN(fetched_at),
            MAX(fetched_at)
        FROM staging.stg_product_snapshot
        GROUP BY source_system, account_name, asin
        ON CONFLICT (platform_code, account_name, asin) DO UPDATE SET
            product_title = EXCLUDED.product_title,
            brand = EXCLUDED.brand,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            last_seen_at = EXCLUDED.last_seen_at
        """,
        """
        INSERT INTO core.bridge_product_identifier (platform_code, identifier_type, identifier_value, asin)
        SELECT DISTINCT source_system, 'asin', asin, asin
        FROM staging.stg_product_snapshot
        WHERE asin IS NOT NULL
        ON CONFLICT (platform_code, identifier_type, identifier_value) DO NOTHING
        """,
    ]
    for statement in statements:
        rowcount += _execute_insert(connection, statement)
    return rowcount


def build_core_inventory_snapshot(connection) -> int:
    sql = """
        INSERT INTO core.fact_inventory_snapshot (
            snapshot_date,
            snapshot_at,
            platform_code,
            account_name,
            marketplace_code,
            asin,
            category_id,
            category_name,
            segment_name,
            segment_keyword,
            inventory_status,
            is_prime,
            is_best_seller,
            raw_event_id,
            request_id,
            idempotency_key
        )
        SELECT
            ingest_date,
            fetched_at,
            source_system,
            account_name,
            marketplace_country,
            asin,
            category_id,
            category_name,
            segment_name,
            segment_keyword,
            'available',
            is_prime,
            is_best_seller,
            raw_event_id,
            request_id,
            idempotency_key
        FROM staging.stg_product_snapshot
        ON CONFLICT (snapshot_date, idempotency_key) DO UPDATE SET
            snapshot_at = EXCLUDED.snapshot_at,
            inventory_status = EXCLUDED.inventory_status,
            is_prime = EXCLUDED.is_prime,
            is_best_seller = EXCLUDED.is_best_seller
    """
    return _execute_insert(connection, sql)


def build_core_price_snapshot(connection) -> int:
    sql = """
        INSERT INTO core.fact_price_snapshot (
            snapshot_date,
            snapshot_at,
            platform_code,
            account_name,
            marketplace_code,
            asin,
            category_id,
            category_name,
            current_price,
            original_price,
            currency,
            discount_pct,
            raw_event_id,
            request_id,
            idempotency_key
        )
        SELECT
            p.ingest_date,
            p.fetched_at,
            p.source_system,
            p.account_name,
            p.marketplace_country,
            p.asin,
            p.category_id,
            p.category_name,
            p.price,
            COALESCE(o.original_price, p.price),
            p.currency,
            o.discount_pct,
            p.raw_event_id,
            p.request_id,
            p.idempotency_key
        FROM staging.stg_product_snapshot p
        LEFT JOIN staging.stg_offer_snapshot o
          ON p.raw_event_id = o.raw_event_id
        ON CONFLICT (snapshot_date, idempotency_key) DO UPDATE SET
            current_price = EXCLUDED.current_price,
            original_price = EXCLUDED.original_price,
            currency = EXCLUDED.currency,
            discount_pct = EXCLUDED.discount_pct
    """
    return _execute_insert(connection, sql)


def build_core_review_fact(connection) -> int:
    sql = """
        INSERT INTO core.fact_review (
            review_date,
            reviewed_at,
            platform_code,
            marketplace_code,
            asin,
            review_id,
            review_title,
            review_comment,
            review_star_rating,
            helpful_count,
            is_verified_purchase,
            raw_event_id,
            request_id,
            idempotency_key
        )
        SELECT
            ingest_date,
            fetched_at,
            source_system,
            marketplace_country,
            asin,
            COALESCE(review_id, idempotency_key),
            review_title,
            review_comment,
            review_star_rating,
            helpful_count,
            is_verified_purchase,
            raw_event_id,
            request_id,
            idempotency_key
        FROM staging.stg_review_event
        ON CONFLICT (review_date, idempotency_key) DO UPDATE SET
            review_title = EXCLUDED.review_title,
            review_comment = EXCLUDED.review_comment,
            review_star_rating = EXCLUDED.review_star_rating,
            helpful_count = EXCLUDED.helpful_count
    """
    return _execute_insert(connection, sql)


def build_mart_product_daily_metrics(connection) -> int:
    sql = """
        INSERT INTO mart.mart_product_daily_metrics (
            observed_date,
            platform_code,
            marketplace_code,
            asin,
            brand,
            category_id,
            category_name,
            segment_name,
            product_title,
            current_price,
            original_price,
            discount_pct,
            avg_star_rating,
            review_count,
            is_best_seller,
            is_prime,
            latest_snapshot_at
        )
        SELECT
            i.snapshot_date,
            i.platform_code,
            i.marketplace_code,
            i.asin,
            d.brand,
            i.category_id,
            i.category_name,
            i.segment_name,
            d.product_title,
            MAX(p.current_price),
            MAX(p.original_price),
            MAX(p.discount_pct),
            AVG(r.review_star_rating),
            COUNT(DISTINCT r.review_id),
            BOOL_OR(i.is_best_seller),
            BOOL_OR(i.is_prime),
            MAX(i.snapshot_at)
        FROM core.fact_inventory_snapshot i
        LEFT JOIN core.fact_price_snapshot p
          ON i.snapshot_date = p.snapshot_date
         AND i.idempotency_key = p.idempotency_key
        LEFT JOIN core.fact_review r
          ON i.snapshot_date = r.review_date
         AND i.asin = r.asin
         AND i.marketplace_code = r.marketplace_code
        LEFT JOIN core.dim_product d
          ON i.platform_code = d.platform_code
         AND i.account_name = d.account_name
         AND i.asin = d.asin
        GROUP BY
            i.snapshot_date,
            i.platform_code,
            i.marketplace_code,
            i.asin,
            d.brand,
            i.category_id,
            i.category_name,
            i.segment_name,
            d.product_title
        ON CONFLICT (observed_date, platform_code, marketplace_code, asin) DO UPDATE SET
            brand = EXCLUDED.brand,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            segment_name = EXCLUDED.segment_name,
            product_title = EXCLUDED.product_title,
            current_price = EXCLUDED.current_price,
            original_price = EXCLUDED.original_price,
            discount_pct = EXCLUDED.discount_pct,
            avg_star_rating = EXCLUDED.avg_star_rating,
            review_count = EXCLUDED.review_count,
            is_best_seller = EXCLUDED.is_best_seller,
            is_prime = EXCLUDED.is_prime,
            latest_snapshot_at = EXCLUDED.latest_snapshot_at
    """
    return _execute_insert(connection, sql)


def build_mart_brand_market_share(connection) -> int:
    sql = """
        INSERT INTO mart.mart_brand_market_share (
            observed_date,
            platform_code,
            marketplace_code,
            category_id,
            category_name,
            brand,
            asin_count,
            avg_price,
            avg_star_rating,
            review_count,
            brand_share_pct
        )
        WITH brand_agg AS (
            SELECT
                observed_date,
                platform_code,
                marketplace_code,
                category_id,
                category_name,
                COALESCE(brand, 'Unknown') AS brand,
                COUNT(DISTINCT asin) AS asin_count,
                AVG(current_price) AS avg_price,
                AVG(avg_star_rating) AS avg_star_rating,
                SUM(review_count) AS review_count
            FROM mart.mart_product_daily_metrics
            GROUP BY observed_date, platform_code, marketplace_code, category_id, category_name, COALESCE(brand, 'Unknown')
        ),
        totals AS (
            SELECT
                observed_date,
                platform_code,
                marketplace_code,
                category_id,
                SUM(asin_count) AS total_asins
            FROM brand_agg
            GROUP BY observed_date, platform_code, marketplace_code, category_id
        )
        SELECT
            b.observed_date,
            b.platform_code,
            b.marketplace_code,
            b.category_id,
            b.category_name,
            b.brand,
            b.asin_count,
            b.avg_price,
            b.avg_star_rating,
            b.review_count,
            CASE WHEN t.total_asins > 0 THEN ROUND((b.asin_count::numeric / t.total_asins::numeric) * 100, 2) ELSE 0 END
        FROM brand_agg b
        JOIN totals t
          ON b.observed_date = t.observed_date
         AND b.platform_code = t.platform_code
         AND b.marketplace_code = t.marketplace_code
         AND b.category_id = t.category_id
        ON CONFLICT (observed_date, platform_code, marketplace_code, category_id, brand) DO UPDATE SET
            asin_count = EXCLUDED.asin_count,
            avg_price = EXCLUDED.avg_price,
            avg_star_rating = EXCLUDED.avg_star_rating,
            review_count = EXCLUDED.review_count,
            brand_share_pct = EXCLUDED.brand_share_pct
    """
    return _execute_insert(connection, sql)


def build_mart_segment_market_share(connection) -> int:
    sql = """
        INSERT INTO mart.mart_segment_market_share (
            observed_date,
            platform_code,
            marketplace_code,
            segment_name,
            brand,
            asin_count,
            avg_price,
            avg_star_rating,
            review_count,
            brand_share_pct
        )
        WITH segment_agg AS (
            SELECT
                observed_date,
                platform_code,
                marketplace_code,
                segment_name,
                COALESCE(brand, 'Unknown') AS brand,
                COUNT(DISTINCT asin) AS asin_count,
                AVG(current_price) AS avg_price,
                AVG(avg_star_rating) AS avg_star_rating,
                SUM(review_count) AS review_count
            FROM mart.mart_product_daily_metrics
            WHERE segment_name IS NOT NULL AND segment_name <> ''
            GROUP BY observed_date, platform_code, marketplace_code, segment_name, COALESCE(brand, 'Unknown')
        ),
        totals AS (
            SELECT
                observed_date,
                platform_code,
                marketplace_code,
                segment_name,
                SUM(asin_count) AS total_asins
            FROM segment_agg
            GROUP BY observed_date, platform_code, marketplace_code, segment_name
        )
        SELECT
            s.observed_date,
            s.platform_code,
            s.marketplace_code,
            s.segment_name,
            s.brand,
            s.asin_count,
            s.avg_price,
            s.avg_star_rating,
            s.review_count,
            CASE WHEN t.total_asins > 0 THEN ROUND((s.asin_count::numeric / t.total_asins::numeric) * 100, 2) ELSE 0 END
        FROM segment_agg s
        JOIN totals t
          ON s.observed_date = t.observed_date
         AND s.platform_code = t.platform_code
         AND s.marketplace_code = t.marketplace_code
         AND s.segment_name = t.segment_name
        ON CONFLICT (observed_date, platform_code, marketplace_code, segment_name, brand) DO UPDATE SET
            asin_count = EXCLUDED.asin_count,
            avg_price = EXCLUDED.avg_price,
            avg_star_rating = EXCLUDED.avg_star_rating,
            review_count = EXCLUDED.review_count,
            brand_share_pct = EXCLUDED.brand_share_pct
    """
    return _execute_insert(connection, sql)


def run_postgres_pipeline(connection) -> Dict[str, int]:
    ensure_postgres_data_platform(connection)
    staging_products = build_staging_product_snapshot(connection)
    sample_staging_row: Optional[Sequence[Any]] = None
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT raw_event_id, asin, product_title, brand, currency, price, category_id, category_name
            FROM staging.stg_product_snapshot
            ORDER BY raw_event_id DESC
            LIMIT 1
            """
        )
        sample_staging_row = cursor.fetchone()
    # region agent log
    debug_log(
        hypothesis_id="H5",
        location="src/postgres_pipeline.py:run_postgres_pipeline",
        message="Staging product snapshot sample",
        data={
            "staging_products": staging_products,
            "latest_raw_event_id": sample_staging_row[0] if sample_staging_row else None,
            "latest_asin": sample_staging_row[1] if sample_staging_row else None,
            "latest_title": sample_staging_row[2] if sample_staging_row else None,
            "latest_brand": sample_staging_row[3] if sample_staging_row else None,
            "latest_currency": sample_staging_row[4] if sample_staging_row else None,
            "latest_price": str(sample_staging_row[5]) if sample_staging_row and sample_staging_row[5] is not None else None,
            "latest_category_id": sample_staging_row[6] if sample_staging_row else None,
            "latest_category_name": sample_staging_row[7] if sample_staging_row else None,
        },
    )
    # endregion
    result = {
        "staging_products": staging_products,
        "staging_reviews": build_staging_review_event(connection),
        "staging_offers": build_staging_offer_snapshot(connection),
        "core_dimensions": build_core_dimensions(connection),
        "core_inventory": build_core_inventory_snapshot(connection),
        "core_prices": build_core_price_snapshot(connection),
        "core_reviews": build_core_review_fact(connection),
        "mart_product_daily_metrics": build_mart_product_daily_metrics(connection),
        "mart_brand_market_share": build_mart_brand_market_share(connection),
        "mart_segment_market_share": build_mart_segment_market_share(connection),
    }
    # region agent log
    debug_log(
        hypothesis_id="H5",
        location="src/postgres_pipeline.py:run_postgres_pipeline",
        message="Postgres pipeline step counts",
        data=result,
    )
    # endregion
    return result


def log_schema_drift_event(
    connection,
    source_system: str,
    endpoint_name: str,
    drift_type: str,
    details: Dict[str, Any],
) -> None:
    sql = """
        INSERT INTO ops.schema_drift_event (
            source_system,
            endpoint_name,
            drift_type,
            details_json,
            detected_at
        ) VALUES (%s, %s, %s, %s, NOW())
    """
    with connection.cursor() as cursor:
        cursor.execute(sql, (source_system, endpoint_name, drift_type, Json(details)))
    connection.commit()
