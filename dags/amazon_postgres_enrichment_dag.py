# pyright: reportMissingImports=false, reportMissingModuleSource=false
"""
DAG: amazon_postgres_enrichment_pipeline

Purpose
-------
Run the **full** PostgreSQL pipeline, which includes steps that the main
collection DAG (`amazon_postgres_data_platform`) deliberately skips for speed:

  1. staging_snapshot    – re-extract raw events into staging tables
  2. staging_enrichment  – backfill missing brand, category & BSR signals by
                           cross-referencing the brand registry, title library,
                           and manual override table
  3. core_dimensions     – refresh dim_product / dim_brand_registry
  4. core_facts          – refresh inventory / price / review snapshots
  5. mart_metrics        – rebuild mart_product_daily_metrics
  6. mart_sales_estimate – re-run Bayesian daily-sales estimation with latest
                           calibrated parameters
  7. mart_brand_share    – recompute category-level brand market share
  8. mart_segment_share  – recompute segment-level brand share with bootstrap
                           scaling and empirical-Bayes smoothing
  9. trend_alerts        – refresh rolling z-score alerts

Schedule
--------
Default: every Sunday at 03:00 UTC  (`AIRFLOW_ENRICHMENT_CRON`, default `0 3 * * 0`).
This is intentionally offset from the main DAG's 6-hourly cadence (00:00, 06:00,
12:00, 18:00) so the two DAGs don't compete for DB connections.

Manual trigger
--------------
Trigger via Airflow UI (▶ "Trigger DAG") or CLI:
    airflow dags trigger amazon_postgres_enrichment_pipeline
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from src.tasks import task_run_postgres_pipeline  # noqa: E402


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


default_args = {
    "owner": os.getenv("AIRFLOW_DAG_OWNER", "data-platform"),
    "depends_on_past": False,
    # Enrichment is heavier; give it a bit more time before retrying.
    "retries": _env_int("AIRFLOW_ENRICHMENT_RETRIES", 1),
    "retry_delay": timedelta(
        minutes=_env_int("AIRFLOW_ENRICHMENT_RETRY_DELAY_MINUTES", 20),
    ),
}

with DAG(
    dag_id="amazon_postgres_enrichment_pipeline",
    description=(
        "Weekly full-pipeline run: staging enrichment (brand/BSR backfill) "
        "+ core + mart rebuild. Supports manual trigger at any time."
    ),
    default_args=default_args,
    start_date=datetime(2026, 3, 30),
    # Default: every Sunday at 03:00 UTC, offset from the 6-hourly collect DAG.
    # Override with AIRFLOW_ENRICHMENT_CRON.  Set to None to disable auto-schedule
    # and use manual-trigger only.
    schedule_interval=os.getenv("AIRFLOW_ENRICHMENT_CRON", "0 3 * * 0"),
    catchup=False,
    max_active_runs=1,
    tags=["amazon", "postgres", "enrichment", "weekly"],
) as dag:
    run_full_pipeline = PythonOperator(
        task_id="run_full_pipeline",
        python_callable=task_run_postgres_pipeline,
        doc_md="""
### run_full_pipeline

Calls `task_run_postgres_pipeline`, which executes the following steps in order:

| Step | Function | What it does |
|------|----------|--------------|
| 1 | `build_staging_product_snapshot` | Extract raw API events → `staging.stg_product_snapshot` |
| 2 | `backfill_staging_product_enrichment` | Fill missing brand / category / BSR via brand registry + title library |
| 3 | `build_staging_review_event` | Extract reviews → `staging.stg_review_event` |
| 4 | `build_staging_offer_snapshot` | Extract offers → `staging.stg_offer_snapshot` |
| 5 | `build_core_dimensions` | Refresh `dim_*` tables |
| 6 | `build_core_inventory_snapshot` | Refresh `fact_inventory_snapshot` |
| 7 | `build_core_price_snapshot` | Refresh `fact_price_snapshot` |
| 8 | `build_core_review_fact` | Refresh `fact_review` |
| 9 | `build_mart_product_daily_metrics` | Aggregate daily product metrics |
| 10 | `build_mart_daily_sales_estimate` | Bayesian daily-sales estimation |
| 11 | `build_mart_brand_market_share` | Category-level brand share |
| 12 | `build_mart_segment_market_share` | Segment-level brand share (bootstrap + EB) |
| 13 | `build_ops_trend_alerts` | Rolling z-score trend alerts |
""",
    )
