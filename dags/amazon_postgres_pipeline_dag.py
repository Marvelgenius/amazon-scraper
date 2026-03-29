# pyright: reportMissingImports=false, reportMissingModuleSource=false
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.tasks import (  # noqa: E402
    task_build_postgres_core,
    task_build_postgres_marts,
    task_build_postgres_staging,
    task_fetch_and_store_offers,
    task_fetch_and_store_product_details,
    task_fetch_and_store_reviews,
    task_fetch_and_store_search,
)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, str(default)).strip().lower()
    return raw in {"1", "true", "yes", "y", "on"}


def _request_metadata() -> Dict[str, Any]:
    return {
        "account": os.getenv("AIRFLOW_AMAZON_ACCOUNT", "default"),
        "business_domain": os.getenv(
            "AIRFLOW_AMAZON_BUSINESS_DOMAIN",
            "thirdparty-market-intelligence",
        ),
        "dataset_name": os.getenv(
            "AIRFLOW_AMAZON_DATASET_NAME",
            "amazon_product_catalog_search_airflow",
        ),
        "collection_purpose": os.getenv(
            "AIRFLOW_AMAZON_COLLECTION_PURPOSE",
            "scheduled_market_monitoring",
        ),
        "source_channel": "airflow",
        "page": _env_int("AIRFLOW_AMAZON_SEARCH_PAGE", 1),
    }


def _search_kwargs() -> Dict[str, Any]:
    return {
        "query": os.getenv("AIRFLOW_AMAZON_SEARCH_QUERY", "coffee maker"),
        "country": os.getenv("AIRFLOW_AMAZON_COUNTRY", "US"),
        "page": _env_int("AIRFLOW_AMAZON_SEARCH_PAGE", 1),
        "request_metadata": _request_metadata(),
    }


def _extract_asins(rows: List[Dict[str, Any]], limit: int) -> List[str]:
    seen = set()
    ordered: List[str] = []
    for row in rows or []:
        asin = row.get("asin")
        if not asin or asin in seen:
            continue
        seen.add(asin)
        ordered.append(asin)
        if len(ordered) >= limit:
            break
    return ordered


def _run_search(**_: Any) -> List[Dict[str, Any]]:
    return task_fetch_and_store_search(**_search_kwargs())


def _run_product_details(ti, **_: Any) -> List[Dict[str, Any]]:
    limit = _env_int("AIRFLOW_AMAZON_DETAIL_ASIN_LIMIT", 10)
    rows = ti.xcom_pull(task_ids="collect_search_results") or []
    asins = _extract_asins(rows, limit)
    if not asins:
        return []
    return task_fetch_and_store_product_details(
        asin=asins,
        country=os.getenv("AIRFLOW_AMAZON_COUNTRY", "US"),
        request_metadata={
            **_request_metadata(),
            "dataset_name": os.getenv(
                "AIRFLOW_AMAZON_DETAIL_DATASET_NAME",
                "amazon_product_catalog_detail_airflow",
            ),
        },
    )


def _run_offers(ti, **_: Any) -> List[Dict[str, Any]]:
    if not _env_bool("AIRFLOW_AMAZON_ENABLE_OFFERS", True):
        return []
    limit = _env_int("AIRFLOW_AMAZON_OFFER_ASIN_LIMIT", 10)
    rows = ti.xcom_pull(task_ids="collect_search_results") or []
    asins = _extract_asins(rows, limit)
    if not asins:
        return []
    return task_fetch_and_store_offers(
        asin=asins,
        country=os.getenv("AIRFLOW_AMAZON_COUNTRY", "US"),
        request_metadata={
            **_request_metadata(),
            "dataset_name": os.getenv(
                "AIRFLOW_AMAZON_OFFER_DATASET_NAME",
                "amazon_product_offers_airflow",
            ),
        },
    )


def _run_reviews(ti, **_: Any) -> List[Dict[str, Any]]:
    if not _env_bool("AIRFLOW_AMAZON_ENABLE_REVIEWS", False):
        return []
    limit = _env_int("AIRFLOW_AMAZON_REVIEW_ASIN_LIMIT", 5)
    rows = ti.xcom_pull(task_ids="collect_search_results") or []
    asins = _extract_asins(rows, limit)
    if not asins:
        return []
    return task_fetch_and_store_reviews(
        asin=asins,
        country=os.getenv("AIRFLOW_AMAZON_COUNTRY", "US"),
        request_metadata={
            **_request_metadata(),
            "dataset_name": os.getenv(
                "AIRFLOW_AMAZON_REVIEW_DATASET_NAME",
                "amazon_product_reviews_airflow",
            ),
        },
    )


default_args = {
    "owner": os.getenv("AIRFLOW_DAG_OWNER", "data-platform"),
    "depends_on_past": False,
    "retries": _env_int("AIRFLOW_DAG_RETRIES", 1),
    "retry_delay": timedelta(
        minutes=_env_int("AIRFLOW_DAG_RETRY_DELAY_MINUTES", 10),
    ),
}


with DAG(
    dag_id="amazon_postgres_data_platform",
    description="Amazon collection to S3 + PostgreSQL medallion pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 29),
    schedule_interval=os.getenv("AIRFLOW_DAG_AMAZON_SEARCH_CRON", "0 */6 * * *"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "postgres", "s3", "airflow"],
) as dag:
    collect_search_results = PythonOperator(
        task_id="collect_search_results",
        python_callable=_run_search,
    )

    collect_product_details = PythonOperator(
        task_id="collect_product_details",
        python_callable=_run_product_details,
    )

    collect_product_offers = PythonOperator(
        task_id="collect_product_offers",
        python_callable=_run_offers,
    )

    collect_product_reviews = PythonOperator(
        task_id="collect_product_reviews",
        python_callable=_run_reviews,
    )

    build_postgres_staging = PythonOperator(
        task_id="build_postgres_staging",
        python_callable=task_build_postgres_staging,
    )

    build_postgres_core = PythonOperator(
        task_id="build_postgres_core",
        python_callable=task_build_postgres_core,
    )

    build_postgres_marts = PythonOperator(
        task_id="build_postgres_marts",
        python_callable=task_build_postgres_marts,
    )

    (
        collect_search_results
        >> [collect_product_details, collect_product_offers, collect_product_reviews]
        >> build_postgres_staging
        >> build_postgres_core
        >> build_postgres_marts
    )
