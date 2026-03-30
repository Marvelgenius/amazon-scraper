from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from psycopg2.extras import Json
from psycopg2.extras import execute_values

from .aws_storage import (
    build_idempotency_key,
    build_request_fingerprint,
    build_request_id,
    build_s3_key,
    put_json_to_s3,
)
from .debug_runtime import debug_log
from .etl import _bootstrap_scaled_total_sales, _compute_jaccard_similarity, parse_sales_volume
from .postgres_database import bulk_insert_raw_events, execute_sql_file
from .sales_estimator import build_calibrated_category_params, estimate_daily_sales_batch

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


def _read_dataframe(connection, sql: str, params: Optional[Sequence[Any]] = None) -> pd.DataFrame:
    return pd.read_sql(sql, connection, params=params)


def _normalize_frame_for_upsert(df: pd.DataFrame, columns: Sequence[str]) -> List[tuple]:
    rows: List[tuple] = []
    for _, row in df.iterrows():
        normalized: List[Any] = []
        for column in columns:
            value = row.get(column)
            if value is None or value is pd.NaT:
                normalized.append(None)
                continue
            try:
                if pd.isna(value):
                    normalized.append(None)
                    continue
            except Exception:
                pass
            if isinstance(value, pd.Timestamp):
                normalized.append(value.to_pydatetime())
            else:
                normalized.append(value)
        rows.append(tuple(normalized))
    return rows


def _upsert_dataframe(
    connection,
    table: str,
    columns: Sequence[str],
    conflict_columns: Sequence[str],
    df: pd.DataFrame,
) -> int:
    if df.empty:
        return 0

    insert_columns = ", ".join(columns)
    conflict_clause = ", ".join(conflict_columns)
    update_columns = [c for c in columns if c not in conflict_columns]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_columns)
    sql = (
        f"INSERT INTO {table} ({insert_columns}) VALUES %s "
        f"ON CONFLICT ({conflict_clause}) DO UPDATE SET {update_clause}"
    )
    values = _normalize_frame_for_upsert(df, columns)
    with connection.cursor() as cursor:
        execute_values(cursor, sql, values, page_size=200)
    connection.commit()
    return len(values)


def _load_metric_signal_frame(connection) -> pd.DataFrame:
    sql = """
        SELECT
            m.observed_date,
            m.platform_code,
            m.marketplace_code AS marketplace,
            m.asin,
            m.brand,
            m.category_id,
            m.category_name,
            m.segment_name,
            m.product_title,
            m.current_price AS price,
            m.bsr_rank,
            m.avg_star_rating AS star_rating,
            m.review_count AS num_ratings,
            m.is_best_seller,
            m.is_prime,
            m.latest_snapshot_at,
            sv.sales_volume_raw
        FROM mart.mart_product_daily_metrics m
        LEFT JOIN (
            SELECT
                ingest_date AS observed_date,
                source_system AS platform_code,
                marketplace_country AS marketplace_code,
                asin,
                MAX(sales_volume_raw) AS sales_volume_raw
            FROM staging.stg_product_snapshot
            GROUP BY ingest_date, source_system, marketplace_country, asin
        ) sv
          ON m.observed_date = sv.observed_date
         AND m.platform_code = sv.platform_code
         AND m.marketplace_code = sv.marketplace_code
         AND m.asin = sv.asin
    """
    df = _read_dataframe(connection, sql)
    if df.empty:
        return df

    df = df.sort_values(["platform_code", "marketplace", "asin", "observed_date"])
    df["observed_date"] = pd.to_datetime(df["observed_date"])
    df["sales_volume_num"] = df["sales_volume_raw"].apply(parse_sales_volume)
    df["num_ratings_delta"] = (
        df.groupby(["platform_code", "marketplace", "asin"])["num_ratings"].diff()
    )
    history_sql = """
        SELECT
            estimate_date,
            platform_code,
            marketplace_code AS marketplace,
            asin,
            estimated_daily_sales
        FROM mart.mart_daily_sales_estimate
    """
    history_df = _read_dataframe(connection, history_sql)
    if not history_df.empty:
        merged_groups: List[pd.DataFrame] = []
        history_df = history_df.sort_values(["platform_code", "marketplace", "asin", "estimate_date"])
        history_df["estimate_date"] = pd.to_datetime(history_df["estimate_date"])
        for key, group in df.groupby(["platform_code", "marketplace", "asin"], dropna=False):
            platform_code, marketplace, asin = key
            group = group.sort_values("observed_date").copy()
            hist_group = history_df[
                (history_df["platform_code"] == platform_code)
                & (history_df["marketplace"] == marketplace)
                & (history_df["asin"] == asin)
            ][["estimate_date", "estimated_daily_sales"]].sort_values("estimate_date")
            if hist_group.empty:
                group["prior_daily_sales"] = None
            else:
                merged = pd.merge_asof(
                    group,
                    hist_group,
                    left_on="observed_date",
                    right_on="estimate_date",
                    direction="backward",
                    allow_exact_matches=False,
                )
                group["prior_daily_sales"] = merged["estimated_daily_sales"]
            merged_groups.append(group)
        df = pd.concat(merged_groups, ignore_index=True)
    else:
        df["prior_daily_sales"] = None
    return df


def _load_segment_sampling_frame(connection) -> pd.DataFrame:
    sql = """
        SELECT
            ingest_date AS observed_date,
            source_system AS platform_code,
            marketplace_country AS marketplace_code,
            segment_name,
            asin,
            COUNT(*) AS appearance_count,
            COUNT(DISTINCT COALESCE(search_query, segment_keyword, '')) AS query_count,
            COUNT(DISTINCT COALESCE(sample_round, 1)) AS sample_round_count,
            MIN(
                CASE
                    WHEN result_rank IS NOT NULL
                    THEN ((COALESCE(search_page, 1) - 1) * 20) + result_rank
                    ELSE NULL
                END
            ) AS best_rank
        FROM staging.stg_product_snapshot
        WHERE segment_name IS NOT NULL
          AND segment_name <> ''
        GROUP BY ingest_date, source_system, marketplace_country, segment_name, asin
    """
    return _read_dataframe(connection, sql)


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
            bsr_rank,
            category_id,
            category_name,
            segment_name,
            segment_keyword,
            segment_root_keyword,
            search_query,
            query_source,
            sample_round,
            search_page,
            result_rank,
            target_brand,
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
            NULLIF(
                REPLACE(
                    SUBSTRING(
                        COALESCE(
                            response_body_jsonb -> 'product_information' ->> 'Best Sellers Rank',
                            response_body_jsonb -> 'product_information' ->> 'Best Seller Rank',
                            response_body_jsonb -> 'product_information' ->> 'best sellers rank',
                            response_body_jsonb -> 'product_details' ->> 'Best Sellers Rank',
                            response_body_jsonb ->> 'best_sellers_rank',
                            response_body_jsonb ->> 'bsr_rank',
                            response_body_jsonb ->> 'bsr',
                            ''
                        ) FROM '#([0-9,]+)'
                    ),
                    ',',
                    ''
                ),
                ''
            )::integer,
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
            request_params_json ->> 'segment_root_keyword',
            request_params_json ->> 'search_query',
            request_params_json ->> 'query_source',
            NULLIF(request_params_json ->> 'sample_round', '')::integer,
            NULLIF(request_params_json ->> 'page', '')::integer,
            NULLIF(request_params_json ->> 'result_rank', '')::integer,
            request_params_json ->> 'target_brand',
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
            sales_volume_raw = EXCLUDED.sales_volume_raw,
            bsr_rank = COALESCE(EXCLUDED.bsr_rank, staging.stg_product_snapshot.bsr_rank),
            category_id = COALESCE(EXCLUDED.category_id, staging.stg_product_snapshot.category_id),
            category_name = COALESCE(EXCLUDED.category_name, staging.stg_product_snapshot.category_name),
            segment_name = COALESCE(EXCLUDED.segment_name, staging.stg_product_snapshot.segment_name),
            segment_keyword = COALESCE(EXCLUDED.segment_keyword, staging.stg_product_snapshot.segment_keyword),
            segment_root_keyword = COALESCE(EXCLUDED.segment_root_keyword, staging.stg_product_snapshot.segment_root_keyword),
            search_query = COALESCE(EXCLUDED.search_query, staging.stg_product_snapshot.search_query),
            query_source = COALESCE(EXCLUDED.query_source, staging.stg_product_snapshot.query_source),
            sample_round = COALESCE(EXCLUDED.sample_round, staging.stg_product_snapshot.sample_round),
            search_page = COALESCE(EXCLUDED.search_page, staging.stg_product_snapshot.search_page),
            result_rank = COALESCE(EXCLUDED.result_rank, staging.stg_product_snapshot.result_rank),
            target_brand = COALESCE(EXCLUDED.target_brand, staging.stg_product_snapshot.target_brand)
    """
    return _execute_insert(connection, sql)


def backfill_staging_product_enrichment(connection) -> int:
    category_sql = """
        WITH latest_category AS (
            SELECT DISTINCT ON (marketplace_country, asin)
                marketplace_country,
                asin,
                category_id,
                category_name
            FROM staging.stg_product_snapshot
            WHERE (category_id IS NOT NULL AND category_id <> '')
               OR (category_name IS NOT NULL AND category_name <> '')
            ORDER BY marketplace_country, asin, fetched_at DESC, raw_event_id DESC
        )
        UPDATE staging.stg_product_snapshot s
        SET category_id = COALESCE(s.category_id, c.category_id),
            category_name = COALESCE(s.category_name, c.category_name)
        FROM latest_category c
        WHERE s.marketplace_country = c.marketplace_country
          AND s.asin = c.asin
          AND (
              s.category_id IS NULL OR s.category_id = ''
              OR s.category_name IS NULL OR s.category_name = ''
          )
    """
    bsr_sql = """
        WITH latest_bsr AS (
            SELECT DISTINCT ON (marketplace_country, asin)
                marketplace_country,
                asin,
                bsr_rank
            FROM staging.stg_product_snapshot
            WHERE bsr_rank IS NOT NULL
            ORDER BY marketplace_country, asin, fetched_at DESC, raw_event_id DESC
        )
        UPDATE staging.stg_product_snapshot s
        SET bsr_rank = COALESCE(s.bsr_rank, b.bsr_rank)
        FROM latest_bsr b
        WHERE s.marketplace_country = b.marketplace_country
          AND s.asin = b.asin
          AND s.bsr_rank IS NULL
    """
    updated = 0
    with connection.cursor() as cursor:
        cursor.execute(category_sql)
        updated += max(cursor.rowcount, 0)
        cursor.execute(bsr_sql)
        updated += max(cursor.rowcount, 0)
    connection.commit()
    return updated


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
        WITH latest_product AS (
            SELECT DISTINCT ON (source_system, account_name, asin)
                source_system,
                account_name,
                asin,
                product_title,
                brand,
                category_id,
                category_name
            FROM staging.stg_product_snapshot
            ORDER BY
                source_system,
                account_name,
                asin,
                CASE
                    WHEN COALESCE(category_name, '') <> '' OR COALESCE(category_id, '') <> '' THEN 0
                    ELSE 1
                END,
                fetched_at DESC,
                raw_event_id DESC
        ),
        first_last AS (
            SELECT
                source_system,
                account_name,
                asin,
                MIN(fetched_at) AS first_seen_at,
                MAX(fetched_at) AS last_seen_at
            FROM staging.stg_product_snapshot
            GROUP BY source_system, account_name, asin
        )
        SELECT
            l.source_system,
            l.account_name,
            l.asin,
            l.product_title,
            l.brand,
            l.category_id,
            l.category_name,
            f.first_seen_at,
            f.last_seen_at
        FROM latest_product l
        JOIN first_last f
          ON l.source_system = f.source_system
         AND l.account_name = f.account_name
         AND l.asin = f.asin
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
            bsr_rank,
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
            bsr_rank,
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
            category_id = COALESCE(EXCLUDED.category_id, core.fact_inventory_snapshot.category_id),
            category_name = COALESCE(EXCLUDED.category_name, core.fact_inventory_snapshot.category_name),
            bsr_rank = COALESCE(EXCLUDED.bsr_rank, core.fact_inventory_snapshot.bsr_rank),
            segment_name = COALESCE(EXCLUDED.segment_name, core.fact_inventory_snapshot.segment_name),
            segment_keyword = COALESCE(EXCLUDED.segment_keyword, core.fact_inventory_snapshot.segment_keyword),
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
            bsr_rank,
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
            MAX(d.brand) AS brand,
            COALESCE(MAX(NULLIF(i.category_id, '')), MAX(NULLIF(d.category_id, ''))) AS category_id,
            COALESCE(MAX(NULLIF(i.category_name, '')), MAX(NULLIF(d.category_name, ''))) AS category_name,
            MAX(i.segment_name) AS segment_name,
            MAX(d.product_title) AS product_title,
            MAX(p.current_price),
            MAX(p.original_price),
            MAX(p.discount_pct),
            MAX(i.bsr_rank),
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
            i.asin
        ON CONFLICT (observed_date, platform_code, marketplace_code, asin) DO UPDATE SET
            brand = EXCLUDED.brand,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            segment_name = EXCLUDED.segment_name,
            product_title = EXCLUDED.product_title,
            current_price = EXCLUDED.current_price,
            original_price = EXCLUDED.original_price,
            discount_pct = EXCLUDED.discount_pct,
            bsr_rank = COALESCE(EXCLUDED.bsr_rank, mart.mart_product_daily_metrics.bsr_rank),
            avg_star_rating = EXCLUDED.avg_star_rating,
            review_count = EXCLUDED.review_count,
            is_best_seller = EXCLUDED.is_best_seller,
            is_prime = EXCLUDED.is_prime,
            latest_snapshot_at = EXCLUDED.latest_snapshot_at
    """
    return _execute_insert(connection, sql)


def build_mart_daily_sales_estimate(connection) -> int:
    df = _load_metric_signal_frame(connection)
    if df.empty:
        return 0

    global_params = build_calibrated_category_params(df.copy(), category_hint=None)
    marketplace_params = {}
    for marketplace, marketplace_group in df.groupby(df["marketplace"].fillna("__unknown__"), dropna=False):
        marketplace_params[marketplace] = build_calibrated_category_params(
            marketplace_group.copy(),
            category_hint=None,
            fallback=global_params,
        )

    df["calibration_bucket"] = (
        df["category_id"].fillna("").astype(str).str.strip()
    )
    missing_bucket = df["calibration_bucket"] == ""
    df.loc[missing_bucket, "calibration_bucket"] = (
        df.loc[missing_bucket, "category_name"].fillna("__unknown__").astype(str)
    )

    estimated_frames: List[pd.DataFrame] = []
    for (marketplace, bucket), group in df.groupby(
        [df["marketplace"].fillna("__unknown__"), df["calibration_bucket"].fillna("__unknown__")],
        dropna=False,
    ):
        category_hint = group["category_name"].dropna()
        hint = None
        if not category_hint.empty:
            hint = str(category_hint.iloc[0])
        elif bucket != "__unknown__":
            hint = str(bucket)
        params = build_calibrated_category_params(
            group.copy(),
            category_hint=hint,
            fallback=marketplace_params.get(marketplace, global_params),
        )
        estimated_frames.append(
            estimate_daily_sales_batch(
                group.copy(),
                category_hint=hint,
                params=params,
            )
        )

    result = pd.concat(estimated_frames, ignore_index=True)
    result = result[
        [
            "observed_date",
            "platform_code",
            "marketplace",
            "asin",
            "brand",
            "category_id",
            "category_name",
            "segment_name",
            "product_title",
            "sales_volume_raw",
            "sales_volume_num",
            "bsr_rank",
            "num_ratings",
            "num_ratings_delta",
            "price",
            "star_rating",
            "estimated_daily_sales",
            "estimate_lower_bound",
            "estimate_upper_bound",
            "estimation_method",
            "confidence_score",
        ]
    ].rename(
        columns={
            "observed_date": "estimate_date",
            "marketplace": "marketplace_code",
        }
    )

    return _upsert_dataframe(
        connection,
        table="mart.mart_daily_sales_estimate",
        columns=[
            "estimate_date",
            "platform_code",
            "marketplace_code",
            "asin",
            "brand",
            "category_id",
            "category_name",
            "segment_name",
            "product_title",
            "sales_volume_raw",
            "sales_volume_num",
            "bsr_rank",
            "num_ratings",
            "num_ratings_delta",
            "price",
            "star_rating",
            "estimated_daily_sales",
            "estimate_lower_bound",
            "estimate_upper_bound",
            "estimation_method",
            "confidence_score",
        ],
        conflict_columns=["estimate_date", "platform_code", "marketplace_code", "asin"],
        df=result,
    )


def build_mart_brand_market_share(connection) -> int:
    sql = """
        SELECT
            m.observed_date,
            m.platform_code,
            m.marketplace_code,
            m.category_id,
            m.category_name,
            COALESCE(m.brand, 'Unknown') AS brand,
            m.asin,
            m.current_price AS price,
            m.avg_star_rating AS star_rating,
            COALESCE(e.sales_volume_num, 0) AS sales_volume_num,
            COALESCE(m.review_count, 0) AS num_ratings,
            COALESCE(e.estimated_daily_sales, 0) AS estimated_daily_sales
        FROM mart.mart_product_daily_metrics m
        LEFT JOIN mart.mart_daily_sales_estimate e
          ON m.observed_date = e.estimate_date
         AND m.platform_code = e.platform_code
         AND m.marketplace_code = e.marketplace_code
         AND m.asin = e.asin
        WHERE m.category_id IS NOT NULL
          AND m.category_id <> ''
    """
    df = _read_dataframe(connection, sql)
    if df.empty:
        return 0

    brand_agg = (
        df.groupby(
            ["observed_date", "platform_code", "marketplace_code", "category_id", "category_name", "brand"],
            dropna=False,
        )
        .agg(
            asin_count=("asin", "nunique"),
            total_estimated_daily_sales=("estimated_daily_sales", "sum"),
            total_num_ratings=("num_ratings", "sum"),
            total_sales_volume=("sales_volume_num", "sum"),
            avg_price=("price", "mean"),
            avg_star_rating=("star_rating", "mean"),
        )
        .reset_index()
    )
    totals = (
        brand_agg.groupby(
            ["observed_date", "platform_code", "marketplace_code", "category_id"],
            dropna=False,
        )
        .agg(
            market_sales=("total_estimated_daily_sales", "sum"),
            market_ratings=("total_num_ratings", "sum"),
            market_asins=("asin_count", "sum"),
        )
        .reset_index()
    )
    result = brand_agg.merge(
        totals,
        on=["observed_date", "platform_code", "marketplace_code", "category_id"],
        how="left",
    )
    result["brand_share_pct"] = result.apply(
        lambda r: round(float(r["total_estimated_daily_sales"]) * 100.0 / float(r["market_sales"]), 2)
        if r["market_sales"] else 0.0,
        axis=1,
    )
    result["rating_share_pct"] = result.apply(
        lambda r: round(float(r["total_num_ratings"]) * 100.0 / float(r["market_ratings"]), 2)
        if r["market_ratings"] else 0.0,
        axis=1,
    )
    result["asin_share_pct"] = result.apply(
        lambda r: round(float(r["asin_count"]) * 100.0 / float(r["market_asins"]), 2)
        if r["market_asins"] else 0.0,
        axis=1,
    )
    result["review_count"] = result["total_num_ratings"]
    result["last_observed_at"] = datetime.utcnow()

    return _upsert_dataframe(
        connection,
        table="mart.mart_brand_market_share",
        columns=[
            "observed_date",
            "platform_code",
            "marketplace_code",
            "category_id",
            "category_name",
            "brand",
            "asin_count",
            "avg_price",
            "avg_star_rating",
            "review_count",
            "brand_share_pct",
            "total_estimated_daily_sales",
            "total_num_ratings",
            "total_sales_volume",
            "rating_share_pct",
            "asin_share_pct",
            "last_observed_at",
        ],
        conflict_columns=["observed_date", "platform_code", "marketplace_code", "category_id", "brand"],
        df=result,
    )


def build_mart_segment_market_share(connection) -> int:
    sql = """
        SELECT
            m.observed_date,
            m.platform_code,
            m.marketplace_code,
            m.segment_name,
            m.asin,
            COALESCE(m.brand, 'Unknown') AS brand,
            m.current_price AS price,
            m.avg_star_rating AS star_rating,
            COALESCE(m.review_count, 0) AS num_ratings,
            COALESCE(e.sales_volume_num, 0) AS sales_volume_num,
            COALESCE(e.estimated_daily_sales, 0) AS estimated_daily_sales
        FROM mart.mart_product_daily_metrics m
        LEFT JOIN mart.mart_daily_sales_estimate e
          ON m.observed_date = e.estimate_date
         AND m.platform_code = e.platform_code
         AND m.marketplace_code = e.marketplace_code
         AND m.asin = e.asin
        WHERE m.segment_name IS NOT NULL
          AND m.segment_name <> ''
    """
    df = _read_dataframe(connection, sql)
    if df.empty:
        return 0

    sampling_df = _load_segment_sampling_frame(connection)
    if not sampling_df.empty:
        df = df.merge(
            sampling_df,
            on=["observed_date", "platform_code", "marketplace_code", "segment_name", "asin"],
            how="left",
        )
    for column, default_value in {
        "appearance_count": 1,
        "query_count": 1,
        "sample_round_count": 1,
        "best_rank": 1,
    }.items():
        if column not in df.columns:
            df[column] = default_value
        df[column] = df[column].fillna(default_value).astype(float)
    df["sampling_weight"] = (1.0 / df["appearance_count"]).clip(lower=0.2, upper=1.0)
    df["weighted_estimated_daily_sales"] = df["estimated_daily_sales"].astype(float) * df["sampling_weight"]

    history_counts = (
        df.groupby(["observed_date", "platform_code", "marketplace_code", "segment_name"], dropna=False)
        .agg(asin_count=("asin", "nunique"))
        .reset_index()
        .sort_values("observed_date")
    )

    share_rows: List[Dict[str, Any]] = []
    stats_rows: List[Dict[str, Any]] = []

    for key, group in df.groupby(["observed_date", "platform_code", "marketplace_code", "segment_name"], dropna=False):
        observed_date, platform_code, marketplace_code, segment_name = key
        group = group.copy()
        sample_asin_count = int(group["asin"].nunique())

        history_window = history_counts[
            (history_counts["platform_code"] == platform_code)
            & (history_counts["marketplace_code"] == marketplace_code)
            & (history_counts["segment_name"] == segment_name)
            & (history_counts["observed_date"] <= observed_date)
            & (history_counts["observed_date"] >= observed_date - pd.Timedelta(days=30))
        ]
        estimated_market_asin_count = sample_asin_count
        if not history_window.empty:
            estimated_market_asin_count = int(max(history_window["asin_count"].max(), sample_asin_count))

        coverage_ratio = min(
            sample_asin_count / float(max(estimated_market_asin_count, 1)),
            1.0,
        )

        previous_window = history_counts[
            (history_counts["platform_code"] == platform_code)
            & (history_counts["marketplace_code"] == marketplace_code)
            & (history_counts["segment_name"] == segment_name)
            & (history_counts["observed_date"] < observed_date)
        ].sort_values("observed_date")
        previous_asins: set = set()
        if not previous_window.empty:
            previous_date = previous_window.iloc[-1]["observed_date"]
            previous_asins = set(
                df[
                    (df["platform_code"] == platform_code)
                    & (df["marketplace_code"] == marketplace_code)
                    & (df["segment_name"] == segment_name)
                    & (df["observed_date"] == previous_date)
                ]["asin"].dropna().tolist()
            )
        current_asins = set(group["asin"].dropna().tolist())
        jaccard = _compute_jaccard_similarity(current_asins, previous_asins)
        stability_score = coverage_ratio if jaccard is None else (coverage_ratio + jaccard) / 2.0

        sales_values = group["weighted_estimated_daily_sales"].to_numpy(dtype=float)
        sample_sales_sum = float(group["weighted_estimated_daily_sales"].sum())
        bootstrap_mean_sales, bootstrap_lower_bound, bootstrap_upper_bound = _bootstrap_scaled_total_sales(
            sales_values=sales_values,
            estimated_market_asin_count=estimated_market_asin_count,
        )

        prior_df = df[
            (df["platform_code"] == platform_code)
            & (df["marketplace_code"] == marketplace_code)
            & (df["segment_name"] == segment_name)
            & (df["observed_date"] < observed_date)
            & (df["observed_date"] >= observed_date - pd.Timedelta(days=30))
        ]
        if prior_df.empty:
            prior_share_map = (
                group.groupby("brand")["weighted_estimated_daily_sales"].sum() / max(sample_sales_sum, 1.0)
            ).to_dict()
        else:
            prior_brand = prior_df.groupby("brand")["weighted_estimated_daily_sales"].sum()
            total_prior_sales = float(prior_brand.sum()) or 1.0
            prior_share_map = {brand: float(total) / total_prior_sales for brand, total in prior_brand.items()}

        prior_strength = max(10.0, sample_sales_sum * (1.0 - coverage_ratio + 0.1))
        brand_agg = (
            group.groupby("brand", dropna=False)
            .agg(
                asin_count=("asin", "nunique"),
                total_estimated_daily_sales=("weighted_estimated_daily_sales", "sum"),
                total_num_ratings=("num_ratings", "sum"),
                total_sales_volume=("sales_volume_num", "sum"),
                avg_price=("price", "mean"),
                avg_star_rating=("star_rating", "mean"),
            )
            .reset_index()
        )

        total_ratings = float(brand_agg["total_num_ratings"].sum()) or 0.0
        total_asins = float(brand_agg["asin_count"].sum()) or 0.0
        for _, row in brand_agg.iterrows():
            brand = row["brand"] or "Unknown"
            observed_sales = float(row["total_estimated_daily_sales"] or 0.0)
            prior_share = prior_share_map.get(brand, 1.0 / max(len(prior_share_map), 1))
            eb_share = (observed_sales + prior_share * prior_strength) / max(sample_sales_sum + prior_strength, 1.0)
            adjusted_sales = bootstrap_mean_sales * eb_share
            share_rows.append(
                {
                    "observed_date": observed_date,
                    "platform_code": platform_code,
                    "marketplace_code": marketplace_code,
                    "segment_name": segment_name,
                    "brand": brand,
                    "asin_count": int(row["asin_count"] or 0),
                    "avg_price": row["avg_price"],
                    "avg_star_rating": row["avg_star_rating"],
                    "review_count": int(row["total_num_ratings"] or 0),
                    "brand_share_pct": round(eb_share * 100.0, 2),
                    "total_estimated_daily_sales": round(adjusted_sales, 2),
                    "total_num_ratings": int(row["total_num_ratings"] or 0),
                    "total_sales_volume": int(row["total_sales_volume"] or 0),
                    "rating_share_pct": round((float(row["total_num_ratings"] or 0) * 100.0 / total_ratings), 2)
                    if total_ratings > 0
                    else 0.0,
                    "asin_share_pct": round((float(row["asin_count"] or 0) * 100.0 / total_asins), 2)
                    if total_asins > 0
                    else 0.0,
                    "eb_adjusted_share_pct": round(eb_share * 100.0, 2),
                    "sample_asin_count": sample_asin_count,
                    "estimated_market_asin_count": estimated_market_asin_count,
                    "bootstrap_mean_sales": bootstrap_mean_sales,
                    "bootstrap_lower_bound": bootstrap_lower_bound,
                    "bootstrap_upper_bound": bootstrap_upper_bound,
                    "coverage_ratio": coverage_ratio,
                    "stability_score": stability_score,
                    "last_observed_at": datetime.utcnow(),
                }
            )

        stats_rows.append(
            {
                "observed_date": observed_date,
                "platform_code": platform_code,
                "marketplace_code": marketplace_code,
                "segment_name": segment_name,
                "sample_asin_count": sample_asin_count,
                "estimated_market_asin_count": estimated_market_asin_count,
                "sample_sales_sum": sample_sales_sum,
                "bootstrap_mean_sales": bootstrap_mean_sales,
                "bootstrap_lower_bound": bootstrap_lower_bound,
                "bootstrap_upper_bound": bootstrap_upper_bound,
                "coverage_ratio": coverage_ratio,
                "stability_score": stability_score,
                "eb_prior_strength": prior_strength,
                "last_observed_at": datetime.utcnow(),
            }
        )

    share_df = pd.DataFrame(share_rows)
    stats_df = pd.DataFrame(stats_rows)
    share_count = _upsert_dataframe(
        connection,
        table="mart.mart_segment_market_share",
        columns=[
            "observed_date",
            "platform_code",
            "marketplace_code",
            "segment_name",
            "brand",
            "asin_count",
            "avg_price",
            "avg_star_rating",
            "review_count",
            "brand_share_pct",
            "total_estimated_daily_sales",
            "total_num_ratings",
            "total_sales_volume",
            "rating_share_pct",
            "asin_share_pct",
            "eb_adjusted_share_pct",
            "sample_asin_count",
            "estimated_market_asin_count",
            "bootstrap_mean_sales",
            "bootstrap_lower_bound",
            "bootstrap_upper_bound",
            "coverage_ratio",
            "stability_score",
            "last_observed_at",
        ],
        conflict_columns=["observed_date", "platform_code", "marketplace_code", "segment_name", "brand"],
        df=share_df,
    )
    _upsert_dataframe(
        connection,
        table="mart.mart_segment_estimation_stats",
        columns=[
            "observed_date",
            "platform_code",
            "marketplace_code",
            "segment_name",
            "sample_asin_count",
            "estimated_market_asin_count",
            "sample_sales_sum",
            "bootstrap_mean_sales",
            "bootstrap_lower_bound",
            "bootstrap_upper_bound",
            "coverage_ratio",
            "stability_score",
            "eb_prior_strength",
            "last_observed_at",
        ],
        conflict_columns=["observed_date", "platform_code", "marketplace_code", "segment_name"],
        df=stats_df,
    )
    return share_count


def build_ops_trend_alerts(connection, lookback_days: int = 30, z_threshold: float = 2.0) -> int:
    sql = """
        SELECT
            estimate_date,
            marketplace_code,
            brand,
            SUM(estimated_daily_sales) AS total_daily_sales,
            AVG(price) AS avg_price,
            AVG(star_rating) AS avg_star_rating
        FROM mart.mart_daily_sales_estimate
        WHERE brand IS NOT NULL
        GROUP BY estimate_date, marketplace_code, brand
        ORDER BY estimate_date
    """
    df = _read_dataframe(connection, sql)
    if len(df) < 7:
        return 0

    alerts: List[Dict[str, Any]] = []
    for (marketplace_code, brand), group in df.groupby(["marketplace_code", "brand"], dropna=False):
        group = group.sort_values("estimate_date")
        for metric_name, value_col in [
            ("daily_sales", "total_daily_sales"),
            ("avg_price", "avg_price"),
            ("avg_rating", "avg_star_rating"),
        ]:
            metric_series = group[value_col].astype(float)
            rolling_mean = metric_series.rolling(lookback_days, min_periods=7).mean()
            rolling_std = metric_series.rolling(lookback_days, min_periods=7).std()
            if pd.isna(rolling_std.iloc[-1]) or float(rolling_std.iloc[-1]) == 0:
                continue
            z_score = (metric_series.iloc[-1] - rolling_mean.iloc[-1]) / rolling_std.iloc[-1]
            if abs(float(z_score)) < z_threshold:
                continue
            direction = "上升" if z_score > 0 else "下降"
            baseline = float(rolling_mean.iloc[-1]) if rolling_mean.iloc[-1] else 0.0
            change_pct = ((float(metric_series.iloc[-1]) - baseline) / baseline * 100.0) if baseline else 0.0
            alerts.append(
                {
                    "alert_date": group["estimate_date"].iloc[-1],
                    "marketplace_code": marketplace_code,
                    "dimension_type": "brand",
                    "dimension_value": brand,
                    "metric_name": metric_name,
                    "current_value": float(metric_series.iloc[-1]),
                    "baseline_value": baseline,
                    "change_pct": round(change_pct, 2),
                    "z_score": round(float(z_score), 2),
                    "alert_level": "critical" if abs(float(z_score)) > 3 else "warning",
                    "alert_message": f"{brand} {metric_name} 显著{direction}，Z={float(z_score):.1f}",
                    "created_at": datetime.utcnow(),
                }
            )

    alert_df = pd.DataFrame(alerts)
    return _upsert_dataframe(
        connection,
        table="ops.trend_alert",
        columns=[
            "alert_date",
            "marketplace_code",
            "dimension_type",
            "dimension_value",
            "metric_name",
            "current_value",
            "baseline_value",
            "change_pct",
            "z_score",
            "alert_level",
            "alert_message",
            "created_at",
        ],
        conflict_columns=["alert_date", "marketplace_code", "dimension_type", "dimension_value", "metric_name"],
        df=alert_df,
    )


def run_postgres_pipeline(connection) -> Dict[str, int]:
    ensure_postgres_data_platform(connection)
    staging_products = build_staging_product_snapshot(connection)
    staging_enrichment = backfill_staging_product_enrichment(connection)
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
        "staging_enrichment": staging_enrichment,
        "staging_reviews": build_staging_review_event(connection),
        "staging_offers": build_staging_offer_snapshot(connection),
        "core_dimensions": build_core_dimensions(connection),
        "core_inventory": build_core_inventory_snapshot(connection),
        "core_prices": build_core_price_snapshot(connection),
        "core_reviews": build_core_review_fact(connection),
        "mart_product_daily_metrics": build_mart_product_daily_metrics(connection),
        "mart_daily_sales_estimate": build_mart_daily_sales_estimate(connection),
        "mart_brand_market_share": build_mart_brand_market_share(connection),
        "mart_segment_market_share": build_mart_segment_market_share(connection),
        "trend_alert": build_ops_trend_alerts(connection),
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
