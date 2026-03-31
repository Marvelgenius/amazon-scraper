import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import load_config_from_db
from src import (
    Amazon,
    backfill_postgres_brand_quality,
    close_tunnel,
    create_db_connection,
    upsert_brand_manual_override,
)
from src.brand_utils import is_plausible_brand_name


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Backfill brand normalization, config-scope detail enrichment, and dependent Postgres marts.",
    )
    parser.add_argument("--segment-name", help="Only recompute rows for one segment in staging enrichment.")
    parser.add_argument("--marketplace", help="Only recompute rows for one marketplace country code.")
    parser.add_argument("--start-date", help="Only recompute rows on/after YYYY-MM-DD in staging enrichment.")
    parser.add_argument("--end-date", help="Only recompute rows on/before YYYY-MM-DD in staging enrichment.")
    parser.add_argument("--profile", default=os.getenv("SCRAPE_PROFILE", "daily"), help="Read active configure rows for this schedule profile.")
    parser.add_argument("--skip-config-sync", action="store_true", help="Skip fetching product_details for configured category/segment scopes.")
    parser.add_argument("--max-detail-asins", type=int, default=int(os.getenv("BRAND_SYNC_MAX_DETAIL_ASINS", "200")), help="Maximum config-scope ASINs to refresh with product_details before backfill.")
    parser.add_argument("--detail-cooldown-days", type=int, default=int(os.getenv("BRAND_SYNC_DETAIL_COOLDOWN_DAYS", "7")), help="Refresh details again only when older than this many days, unless brand still looks suspicious.")
    parser.add_argument("--override-asin", help="Upsert one manual brand override before backfill.")
    parser.add_argument("--override-brand", help="Brand value for --override-asin.")
    parser.add_argument("--override-note", help="Optional note stored with the manual brand override.")
    return parser


def _load_config_scope(profile: str) -> Tuple[Dict[str, Dict[str, set[str]]], float]:
    config = load_config_from_db(profile)
    if not config:
        return {}, float(os.getenv("AMAZON_REQUEST_DELAY", "1"))
    tasks, delay = config
    scope: Dict[str, Dict[str, set[str]]] = {}
    for task in tasks:
        if task.query_type not in {"category_scan", "segment_scan"}:
            continue
        for country in task.countries or ["US"]:
            bucket = scope.setdefault(country.upper(), {"category_ids": set(), "segment_names": set()})
            if task.query_type == "category_scan":
                bucket["category_ids"].add(task.keyword)
            elif task.query_type == "segment_scan":
                bucket["segment_names"].add(str(task.extra.get("segment_name") or task.keyword))
    return scope, delay


def _fetch_scope_candidates(connection, *, marketplace_code: str, category_ids: List[str], segment_names: List[str]) -> List[tuple]:
    sql = """
        SELECT
            s.source_system,
            s.marketplace_country,
            s.asin,
            MAX(s.fetched_at) AS latest_fetched_at,
            MAX(CASE WHEN s.endpoint_name = 'product_details' THEN s.fetched_at END) AS latest_detail_at,
            (ARRAY_REMOVE(ARRAY_AGG(NULLIF(s.brand, '') ORDER BY s.fetched_at DESC, s.raw_event_id DESC), NULL))[1] AS current_brand,
            (ARRAY_REMOVE(ARRAY_AGG(NULLIF(s.product_title, '') ORDER BY s.fetched_at DESC, s.raw_event_id DESC), NULL))[1] AS product_title,
            COUNT(*) AS snapshot_count,
            BOOL_OR(s.category_id = ANY(%s::text[])) AS in_category_scope,
            BOOL_OR(s.segment_name = ANY(%s::text[])) AS in_segment_scope
        FROM staging.stg_product_snapshot s
        WHERE s.marketplace_country = %s
          AND (
              (COALESCE(array_length(%s::text[], 1), 0) > 0 AND s.category_id = ANY(%s::text[]))
              OR (COALESCE(array_length(%s::text[], 1), 0) > 0 AND s.segment_name = ANY(%s::text[]))
          )
        GROUP BY s.source_system, s.marketplace_country, s.asin
    """
    with connection.cursor() as cursor:
        cursor.execute(
            sql,
            (
                category_ids,
                segment_names,
                marketplace_code,
                category_ids,
                category_ids,
                segment_names,
                segment_names,
            ),
        )
        return cursor.fetchall()


def _select_detail_refresh_asins(
    rows: List[tuple],
    *,
    cooldown_days: int,
    remaining_capacity: int,
) -> List[str]:
    if remaining_capacity <= 0:
        return []
    cutoff = time.time() - (cooldown_days * 86400)
    ranked: List[tuple[int, str]] = []
    for row in rows:
        if not row or not row[2]:
            continue
        asin = row[2]
        latest_detail_at = row[4]
        current_brand = row[5]
        snapshot_count = int(row[7] or 0)
        in_category_scope = bool(row[8])
        in_segment_scope = bool(row[9])
        brand_needs_review = not is_plausible_brand_name(current_brand)
        detail_is_stale = latest_detail_at is None or latest_detail_at.timestamp() < cutoff
        if not brand_needs_review and not detail_is_stale:
            continue
        score = 0
        if brand_needs_review:
            score += 1000
        if latest_detail_at is None:
            score += 600
        elif detail_is_stale:
            score += 200
        if in_segment_scope:
            score += 60
        if in_category_scope:
            score += 30
        score += snapshot_count
        ranked.append((score, asin))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    selected: List[str] = []
    seen = set()
    for _, asin in ranked:
        if asin in seen:
            continue
        seen.add(asin)
        selected.append(asin)
        if len(selected) >= remaining_capacity:
            break
    return selected


def _sync_config_scope_details(connection, *, profile: str, max_detail_asins: int, cooldown_days: int) -> dict:
    api_key = os.getenv("RAPIDAPI_KEY")
    if not api_key or max_detail_asins <= 0:
        return {"detail_rows": 0, "detail_asins": 0}
    scope_by_country, delay = _load_config_scope(profile)
    if not scope_by_country:
        return {"detail_rows": 0, "detail_asins": 0}

    total_rows = 0
    total_asins = 0
    remaining = max_detail_asins
    for country, scope in scope_by_country.items():
        if remaining <= 0:
            break
        candidate_rows = _fetch_scope_candidates(
            connection,
            marketplace_code=country,
            category_ids=sorted(scope["category_ids"]),
            segment_names=sorted(scope["segment_names"]),
        )
        refresh_asins = _select_detail_refresh_asins(
            candidate_rows,
            cooldown_days=cooldown_days,
            remaining_capacity=remaining,
        )
        if not refresh_asins:
            continue
        for offset in range(0, len(refresh_asins), 10):
            batch = refresh_asins[offset : offset + 10]
            rows = Amazon.fetch_and_store_product_details(
                asin=batch,
                connection=connection,
                key=api_key,
                country=country,
            )
            total_rows += len(rows)
            total_asins += len(batch)
            remaining -= len(batch)
            if remaining <= 0:
                break
            time.sleep(delay)
    return {"detail_rows": total_rows, "detail_asins": total_asins}


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if bool(args.override_asin) != bool(args.override_brand):
        parser.error("--override-asin and --override-brand must be provided together")
    override_marketplace = (args.marketplace or "US").upper()

    connection = None
    try:
        connection = create_db_connection()
        sync_result = {"detail_rows": 0, "detail_asins": 0}
        if args.override_asin and args.override_brand:
            upsert_brand_manual_override(
                connection,
                asin=args.override_asin,
                brand=args.override_brand,
                marketplace_code=override_marketplace,
                note=args.override_note,
            )
        if not args.skip_config_sync:
            sync_result = _sync_config_scope_details(
                connection,
                profile=args.profile,
                max_detail_asins=args.max_detail_asins,
                cooldown_days=args.detail_cooldown_days,
            )
        result = backfill_postgres_brand_quality(
            connection,
            segment_name=args.segment_name,
            marketplace_code=args.marketplace,
            start_date=args.start_date,
            end_date=args.end_date,
        )
        print({**sync_result, **result})
    finally:
        if connection:
            connection.close()
        close_tunnel()


if __name__ == "__main__":
    main()
