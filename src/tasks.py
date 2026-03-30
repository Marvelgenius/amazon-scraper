"""
Airflow-callable entry point functions.

Each task function manages its own DB connection lifecycle.
Supports both legacy (OutIn/Coffee) and generic (any brand/category) pipelines.
"""

import logging
from typing import Any, Dict, List, Optional, Sequence, Union

from .amazon_scraper import Amazon
from .config import get_database_backend
from .database import (
    DEFAULT_RAW_PRODUCTS_TABLE,
    close_tunnel,
    create_db_connection,
)
from .debug_runtime import debug_log
from .etl import (
    build_brand_market_share,
    build_coffee_market_share,
    build_daily_sales_estimates,
    build_outin_daily_sales,
    build_outin_monthly_sales,
    build_outin_review_trend,
    build_product_daily_metrics,
    build_segment_market_share,
    build_segment_product_daily_metrics,
    detect_and_store_trend_alerts,
    extract_asin_hierarchy,
    extract_offers_to_dwh,
    extract_reviews_to_dwh,
    extract_to_dwh,
)
from .postgres_pipeline import (
    build_core_dimensions as pg_build_core_dimensions,
    build_core_inventory_snapshot as pg_build_core_inventory_snapshot,
    build_core_price_snapshot as pg_build_core_price_snapshot,
    build_core_review_fact as pg_build_core_review_fact,
    build_mart_brand_market_share as pg_build_mart_brand_market_share,
    build_mart_product_daily_metrics as pg_build_mart_product_daily_metrics,
    build_mart_segment_market_share as pg_build_mart_segment_market_share,
    build_staging_offer_snapshot as pg_build_staging_offer_snapshot,
    build_staging_product_snapshot as pg_build_staging_product_snapshot,
    build_staging_review_event as pg_build_staging_review_event,
    ensure_postgres_data_platform,
    logged_job,
    run_postgres_pipeline,
)

logger = logging.getLogger(__name__)


def _is_postgres_backend() -> bool:
    return get_database_backend() == "postgresql"


def _run_postgres_job(
    job_name: str,
    runner,
    *args: Any,
    database: Optional[str] = None,
    **kwargs: Any,
):
    connection = create_db_connection(database=database)
    try:
        ensure_postgres_data_platform(connection)
        with logged_job(connection, job_name=job_name, job_params=kwargs):
            return runner(connection, *args, **kwargs)
    finally:
        connection.close()
        close_tunnel()


# =========================================================================
# Data collection tasks
# =========================================================================

def task_fetch_and_store_search(
    query: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    # region agent log
    debug_log(
        hypothesis_id="H8",
        location="src/tasks.py:task_fetch_and_store_search",
        message="Search task starting",
        data={
            "query_type": type(query).__name__,
            "query_preview": query if isinstance(query, str) else list(query)[:3],
            "country": country,
            "page": page,
            "database": database,
            "table_name": table_name,
            "key_arg_present": bool(key),
            "request_metadata_keys": sorted(filters.get("request_metadata", {}).keys())[:12]
            if isinstance(filters.get("request_metadata"), dict) else [],
        },
    )
    # endregion
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_search_results(
            query=query, connection=connection, key=key,
            country=country, page=page, table_name=table_name,
            create_table=True, commit=True, **filters,
        )
        logger.info("task_fetch_and_store_search: %d rows for query=%s", len(rows), query)
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_product_details(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_product_details(
            asin=asin, connection=connection, key=key,
            country=country, table_name=table_name,
            create_table=True, commit=True, **filters,
        )
        logger.info("task_fetch_and_store_product_details: %d rows for asin=%s", len(rows), asin)
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_category(
    category_id: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    category_name: Optional[str] = None,
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_category_products(
            category_id=category_id, connection=connection, key=key,
            country=country, page=page, table_name=table_name,
            category_name=category_name, **filters,
        )
        logger.info("task_fetch_and_store_category: %d rows for cat=%s", len(rows), category_id)
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_segment(
    segment_keyword: str,
    segment_name: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_segment_products(
            segment_keyword=segment_keyword,
            segment_name=segment_name,
            connection=connection,
            key=key,
            country=country,
            page=page,
            table_name=table_name,
            **filters,
        )
        logger.info("task_fetch_and_store_segment: %d rows for segment=%s", len(rows), segment_name)
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_bestsellers(
    category: str,
    key: Optional[str] = None,
    country: str = "US",
    page: int = 1,
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_best_sellers(
            category=category, connection=connection, key=key,
            country=country, page=page, table_name=table_name, **filters,
        )
        logger.info("task_fetch_and_store_bestsellers: %d rows for cat=%s", len(rows), category)
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_reviews(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_reviews(
            asin=asin, connection=connection, key=key,
            country=country, table_name=table_name, **filters,
        )
        logger.info("task_fetch_and_store_reviews: %d rows for asin=%s", len(rows), asin)
        return rows
    finally:
        connection.close()
        close_tunnel()


def task_fetch_and_store_offers(
    asin: Union[str, Sequence[str]],
    key: Optional[str] = None,
    country: str = "US",
    database: Optional[str] = None,
    table_name: str = DEFAULT_RAW_PRODUCTS_TABLE,
    **filters: Any,
) -> List[Dict[str, Any]]:
    connection = create_db_connection(database=database)
    try:
        rows = Amazon.fetch_and_store_offers(
            asin=asin, connection=connection, key=key,
            country=country, table_name=table_name, **filters,
        )
        logger.info("task_fetch_and_store_offers: %d rows for asin=%s", len(rows), asin)
        return rows
    finally:
        connection.close()
        close_tunnel()


# =========================================================================
# ETL pipeline tasks
# =========================================================================

def task_extract_to_dwh(**kwargs: Any) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_staging_product_snapshot",
            pg_build_staging_product_snapshot,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = extract_to_dwh(connection)
        logger.info("task_extract_to_dwh: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_extract_reviews_to_dwh(**kwargs: Any) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_staging_review_event",
            pg_build_staging_review_event,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = extract_reviews_to_dwh(connection)
        logger.info("task_extract_reviews_to_dwh: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_extract_offers_to_dwh(**kwargs: Any) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_staging_offer_snapshot",
            pg_build_staging_offer_snapshot,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = extract_offers_to_dwh(connection)
        logger.info("task_extract_offers_to_dwh: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_extract_asin_hierarchy(**kwargs: Any) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "run_postgres_pipeline",
            lambda connection: sum(run_postgres_pipeline(connection).values()),
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = extract_asin_hierarchy(connection)
        logger.info("task_extract_asin_hierarchy: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


# --- Legacy OutIn/Coffee tasks (backward-compatible) ---

def task_build_outin_daily_sales(**kwargs: Any) -> int:
    connection = create_db_connection()
    try:
        count = build_outin_daily_sales(connection)
        logger.info("task_build_outin_daily_sales: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_outin_sales(**kwargs: Any) -> int:
    connection = create_db_connection()
    try:
        count = build_outin_monthly_sales(connection)
        logger.info("task_build_outin_sales: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_review_trend(**kwargs: Any) -> int:
    connection = create_db_connection()
    try:
        count = build_outin_review_trend(connection)
        logger.info("task_build_review_trend: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_market_share(**kwargs: Any) -> int:
    connection = create_db_connection()
    try:
        count = build_coffee_market_share(connection)
        logger.info("task_build_market_share: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


# --- New generic tasks (parameter-driven) ---

def task_build_product_daily_metrics(
    brand_filter: Optional[str] = None,
    category_filter: Optional[str] = None,
    **kwargs: Any,
) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_mart_product_daily_metrics",
            pg_build_mart_product_daily_metrics,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = build_product_daily_metrics(
            connection, brand_filter=brand_filter, category_filter=category_filter,
        )
        logger.info("task_build_product_daily_metrics: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_segment_product_daily_metrics(
    segment_name: Optional[str] = None,
    **kwargs: Any,
) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_core_inventory_snapshot",
            pg_build_core_inventory_snapshot,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = build_segment_product_daily_metrics(connection, segment_name=segment_name)
        logger.info("task_build_segment_product_daily_metrics: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_daily_sales_estimates(
    brand_filter: Optional[str] = None,
    category_hint: Optional[str] = None,
    **kwargs: Any,
) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_core_price_snapshot",
            pg_build_core_price_snapshot,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = build_daily_sales_estimates(
            connection, brand_filter=brand_filter, category_hint=category_hint,
        )
        logger.info("task_build_daily_sales_estimates: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_segment_market_share(
    segment_name: str,
    target_date: Optional[str] = None,
    **kwargs: Any,
) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_mart_segment_market_share",
            pg_build_mart_segment_market_share,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = build_segment_market_share(
            connection, segment_name=segment_name, target_date=target_date,
        )
        logger.info("task_build_segment_market_share: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_brand_market_share(
    category_id: str,
    target_date: Optional[str] = None,
    **kwargs: Any,
) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "build_mart_brand_market_share",
            pg_build_mart_brand_market_share,
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = build_brand_market_share(
            connection, category_id=category_id, target_date=target_date,
        )
        logger.info("task_build_brand_market_share: %d rows.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_detect_trend_alerts(
    brand: Optional[str] = None,
    lookback_days: int = 30,
    z_threshold: float = 2.0,
    **kwargs: Any,
) -> int:
    if _is_postgres_backend():
        return _run_postgres_job(
            "run_postgres_pipeline",
            lambda connection: sum(run_postgres_pipeline(connection).values()),
            **kwargs,
        )
    connection = create_db_connection()
    try:
        count = detect_and_store_trend_alerts(
            connection, brand=brand, lookback_days=lookback_days,
            z_threshold=z_threshold,
        )
        logger.info("task_detect_trend_alerts: %d alerts.", count)
        return count
    finally:
        connection.close()
        close_tunnel()


def task_build_postgres_staging(**kwargs: Any) -> Dict[str, int]:
    return _run_postgres_job(
        "build_postgres_staging",
        lambda connection: {
            "products": pg_build_staging_product_snapshot(connection),
            "reviews": pg_build_staging_review_event(connection),
            "offers": pg_build_staging_offer_snapshot(connection),
        },
        **kwargs,
    )


def task_build_postgres_core(**kwargs: Any) -> Dict[str, int]:
    return _run_postgres_job(
        "build_postgres_core",
        lambda connection: {
            "dimensions": pg_build_core_dimensions(connection),
            "inventory": pg_build_core_inventory_snapshot(connection),
            "prices": pg_build_core_price_snapshot(connection),
            "reviews": pg_build_core_review_fact(connection),
        },
        **kwargs,
    )


def task_build_postgres_marts(**kwargs: Any) -> Dict[str, int]:
    return _run_postgres_job(
        "build_postgres_marts",
        lambda connection: {
            "product_daily_metrics": pg_build_mart_product_daily_metrics(connection),
            "brand_market_share": pg_build_mart_brand_market_share(connection),
            "segment_market_share": pg_build_mart_segment_market_share(connection),
        },
        **kwargs,
    )


def task_run_postgres_pipeline(**kwargs: Any) -> Dict[str, int]:
    return _run_postgres_job(
        "run_postgres_pipeline",
        run_postgres_pipeline,
        **kwargs,
    )
