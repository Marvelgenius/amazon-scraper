import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple

from src import Amazon, create_db_connection, close_tunnel
from src.config import get_database_backend
from src.db_compat import get_dict_cursor, normalize_rows
from src.postgres_pipeline import run_postgres_pipeline
from src.task_config import (
    build_brand_candidates,
    build_segment_sampling_plan,
    estimate_segment_sampling_calls,
    resolve_target_brand,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

ALL_COUNTRIES = [
    "US", "AU", "BR", "CA", "CN", "FR", "DE", "IN", "IT", "MX",
    "NL", "SG", "ES", "TR", "AE", "GB", "JP", "SA", "PL", "SE",
    "BE", "EG", "ZA", "IE",
]

CONFIG_DB = None if get_database_backend() == "postgresql" else "gurysk_app"
CONFIG_TABLE = "gurysk_app.app_scraper_config" if get_database_backend() == "postgresql" else "app_scraper_config"


@dataclass
class ScrapeTask:
    """A single scraping task."""
    query_type: str
    keyword: str
    countries: list[str] = field(default_factory=lambda: ["US"])
    pages: int = 1
    extra: dict = field(default_factory=dict)


def _resolve_countries(raw: str) -> list[str]:
    if raw.strip().upper() == "ALL":
        return list(ALL_COUNTRIES)
    return [c.strip().upper() for c in raw.split(",") if c.strip()]


def _parse_csv(env_var: str, default: str = "") -> list[str]:
    value = os.getenv(env_var, default).strip()
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def load_config_from_db(profile: str) -> Optional[Tuple[list[ScrapeTask], float]]:
    try:
        conn = create_db_connection(database=CONFIG_DB)
    except Exception:
        logger.warning("Cannot connect to config store, falling back to env vars")
        return None

    try:
        with get_dict_cursor(conn) as cur:
            cur.execute(
                f"SELECT config_type, config_key, config_value, countries, pages "
                f"FROM {CONFIG_TABLE} "
                f"WHERE is_active = 1 AND schedule_profile IN (%s, 'both') "
                f"ORDER BY config_type, id",
                (profile,),
            )
            rows = normalize_rows(cur.fetchall())

        if not rows:
            logger.warning("No active config rows for profile=%s, falling back to env vars", profile)
            return None

        tasks: list[ScrapeTask] = []
        delay = 1.0

        for r in rows:
            ct = r["config_type"]
            extra = {}
            if r.get("config_value"):
                try:
                    extra = json.loads(r["config_value"])
                except (json.JSONDecodeError, TypeError):
                    extra = {"value": r["config_value"]}

            if ct == "setting":
                if r["config_key"] == "request_delay":
                    delay = float(r.get("config_value") or "1")
            else:
                tasks.append(ScrapeTask(
                    query_type=ct,
                    keyword=r["config_key"],
                    countries=_resolve_countries(r["countries"]),
                    pages=r["pages"],
                    extra=extra,
                ))

        logger.info("Loaded %d tasks from DB (profile=%s)", len(tasks), profile)
        return tasks, delay

    except Exception:
        logger.warning("Failed to read %s.%s, falling back to env vars", CONFIG_DB, CONFIG_TABLE, exc_info=True)
        return None
    finally:
        conn.close()


def build_tasks_from_env() -> tuple[list[ScrapeTask], float]:
    queries = _parse_csv("AMAZON_QUERIES") or _parse_csv("AMAZON_QUERY", "outin")
    asins = _parse_csv("AMAZON_ASINS")
    raw_countries = os.getenv("AMAZON_COUNTRIES", os.getenv("AMAZON_COUNTRY", "US"))
    countries = _resolve_countries(raw_countries)
    pages = int(os.getenv("AMAZON_PAGES", "1"))
    delay = float(os.getenv("AMAZON_REQUEST_DELAY", "1"))

    tasks: list[ScrapeTask] = []
    for q in queries:
        tasks.append(ScrapeTask("product_query", q, countries, pages))
    for a in asins:
        tasks.append(ScrapeTask("target_asin", a, countries, 1))

    return tasks, delay


def _collect_target_brand_candidates(tasks: list[ScrapeTask]) -> list[str]:
    candidates: list[str] = []
    for task in tasks:
        if task.query_type == "brand_search":
            candidates.extend(
                build_brand_candidates(
                    resolve_target_brand(task.keyword, task.extra),
                    task.extra.get("brand_aliases"),
                )
            )
        elif task.query_type in {"segment_scan", "category_scan", "product_query"} and task.extra.get("target_brand"):
            candidates.extend(
                build_brand_candidates(
                    resolve_target_brand(task.keyword, task.extra),
                    task.extra.get("brand_aliases"),
                )
            )
    deduped = []
    seen = set()
    for brand in candidates:
        lowered = brand.lower()
        if lowered not in seen:
            seen.add(lowered)
            deduped.append(brand)
    return deduped


def _estimate_api_calls(tasks: list[ScrapeTask]) -> int:
    """Estimate total API calls to help with budget awareness."""
    total = 0
    asin_tasks = [t for t in tasks if t.query_type == "target_asin"]
    if asin_tasks:
        all_asins = [t.keyword for t in asin_tasks]
        batches = (len(all_asins) + 9) // 10
        total += batches * len(asin_tasks[0].countries)

    for t in tasks:
        if t.query_type in ("product_query", "category_query", "brand_search"):
            total += len(t.countries) * t.pages
        elif t.query_type == "segment_scan":
            total += sum(estimate_segment_sampling_calls(t.keyword, t.pages, t.extra) for _ in t.countries)
        elif t.query_type == "category_scan":
            total += len(t.countries) * t.pages
        elif t.query_type == "bestseller_scan":
            total += len(t.countries) * t.pages
        elif t.query_type == "review_scan":
            total += len(t.countries)
        elif t.query_type == "offer_scan":
            total += len(t.countries)
    if get_database_backend() == "postgresql":
        max_asins = int(os.getenv("DETAIL_ENRICHMENT_MAX_ASINS", "20"))
        if max_asins > 0:
            detail_batches = (max_asins + 9) // 10
            unique_countries = sorted({country for task in tasks for country in task.countries})
            total += detail_batches * max(len(unique_countries), 1)
    return total


def enrich_missing_product_details(
    tasks: list[ScrapeTask],
    api_key: str,
    delay: float,
    connection,
) -> int:
    if not connection or get_database_backend() != "postgresql":
        return 0

    max_asins = int(os.getenv("DETAIL_ENRICHMENT_MAX_ASINS", "20"))
    cooldown_days = int(os.getenv("DETAIL_ENRICHMENT_COOLDOWN_DAYS", "3"))
    if max_asins <= 0:
        return 0

    countries = sorted({country for task in tasks for country in task.countries}) or ["US"]
    target_asins = {task.keyword for task in tasks if task.query_type == "target_asin"}
    target_brand_candidates = [brand.lower() for brand in _collect_target_brand_candidates(tasks)]
    total_rows = 0
    candidate_sql = """
        WITH asin_activity AS (
            SELECT
                marketplace_country,
                COALESCE(source_record_id, business_id) AS asin,
                COUNT(*) AS event_count,
                MAX(fetched_at) AS latest_fetched_at,
                MAX(CASE WHEN endpoint_name = 'product_details' THEN fetched_at END) AS latest_detail_at,
                BOOL_OR(
                    LOWER(
                        COALESCE(
                            response_body_jsonb -> 'product_information' ->> 'Brand',
                            response_body_jsonb -> 'product_information' ->> 'brand',
                            response_body_jsonb ->> 'brand',
                            NULLIF(
                                REGEXP_REPLACE(
                                    split_part(COALESCE(response_body_jsonb ->> 'product_title', ''), ' ', 1),
                                    '(^[[:punct:]]+|[[:punct:]]+$)',
                                    '',
                                    'g'
                                ),
                                ''
                            ),
                            ''
                        )
                    ) = ANY(%s::text[])
                ) AS is_target_brand,
                BOOL_OR(
                    response_body_jsonb #>> '{category_path,-1,id}' IS NOT NULL
                    OR response_body_jsonb -> 'category' ->> 'id' IS NOT NULL
                    OR NULLIF(request_params_json ->> 'category_id', '') IS NOT NULL
                ) AS has_category_id_signal,
                BOOL_OR(
                    response_body_jsonb #>> '{category_path,-1,name}' IS NOT NULL
                    OR response_body_jsonb -> 'category' ->> 'name' IS NOT NULL
                    OR NULLIF(request_params_json ->> 'category_name', '') IS NOT NULL
                ) AS has_category_name_signal,
                BOOL_OR(
                    response_body_jsonb ? 'best_sellers_rank'
                    OR response_body_jsonb ? 'bsr_rank'
                    OR response_body_jsonb ? 'bsr'
                    OR (response_body_jsonb -> 'product_information' ? 'Best Sellers Rank')
                    OR (response_body_jsonb -> 'product_information' ? 'Best Seller Rank')
                ) AS has_bsr_signal
            FROM raw.api_ingest_event
            WHERE marketplace_country = %s
              AND endpoint_name IN ('product_search', 'segment_search', 'products_by_category', 'product_details')
              AND COALESCE(source_record_id, business_id) IS NOT NULL
            GROUP BY marketplace_country, COALESCE(source_record_id, business_id)
        )
        SELECT asin, event_count, latest_fetched_at, latest_detail_at, is_target_brand, has_category_id_signal, has_category_name_signal, has_bsr_signal
        FROM asin_activity
        WHERE (NOT has_category_id_signal OR NOT has_category_name_signal OR NOT has_bsr_signal OR latest_detail_at IS NULL)
          AND (
              latest_detail_at IS NULL
              OR latest_detail_at < NOW() - (%s || ' days')::interval
          )
          AND (
              %s = FALSE
              OR is_target_brand
              OR asin = ANY(%s::text[])
          )
        ORDER BY event_count DESC, latest_fetched_at DESC
        LIMIT %s
    """

    for country in countries:
        with connection.cursor() as cur:
            cur.execute(
                candidate_sql,
                (
                    target_brand_candidates,
                    country,
                    cooldown_days,
                    bool(target_brand_candidates or target_asins),
                    list(target_asins),
                    max_asins * 4,
                ),
            )
            candidate_rows = cur.fetchall()
        ranked_asins = []
        for row in candidate_rows:
            if not row or not row[0]:
                continue
            asin = row[0]
            event_count = row[1] or 0
            is_target_brand = bool(row[4])
            has_category_id_signal = bool(row[5])
            has_category_name_signal = bool(row[6])
            has_bsr_signal = bool(row[7])
            priority = 0
            if asin in target_asins:
                priority += 1000
            if is_target_brand:
                priority += 800
            if not has_bsr_signal:
                priority += 200
            if not has_category_id_signal:
                priority += 160
            if not has_category_name_signal:
                priority += 100
            priority += int(event_count)
            ranked_asins.append((priority, asin))
        ranked_asins.sort(key=lambda item: (-item[0], item[1]))
        asins = [asin for _, asin in ranked_asins[:max_asins]]
        if not asins:
            continue

        logger.info(
            "[detail_enrich] Fetch %d ASIN details in %s for BSR/category enrichment (cooldown=%sd)...",
            len(asins),
            country,
            cooldown_days,
        )
        rows = Amazon.fetch_and_store_product_details(
            asin=asins,
            connection=connection,
            key=api_key,
            country=country,
        )
        total_rows += len(rows)
        logger.info("  -> %d detail rows stored", len(rows))
        time.sleep(delay)

    return total_rows


def run_tasks(tasks: list[ScrapeTask], api_key: str, delay: float, connection) -> tuple[int, int]:
    total_rows = 0
    total_errors = 0

    search_tasks = [t for t in tasks if t.query_type in ("product_query", "category_query")]
    brand_search_tasks = [t for t in tasks if t.query_type == "brand_search"]
    segment_tasks = [t for t in tasks if t.query_type == "segment_scan"]
    asin_tasks = [t for t in tasks if t.query_type == "target_asin"]
    category_scan_tasks = [t for t in tasks if t.query_type == "category_scan"]
    bestseller_tasks = [t for t in tasks if t.query_type == "bestseller_scan"]
    review_tasks = [t for t in tasks if t.query_type == "review_scan"]
    offer_tasks = [t for t in tasks if t.query_type == "offer_scan"]

    # --- product_query / category_query (legacy search) ---
    for task in search_tasks:
        target_brand = task.extra.get("target_brand")
        brand_aliases = build_brand_candidates(target_brand, task.extra.get("brand_aliases")) if target_brand else []
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[%s] Search '%s' in %s (page %d)...",
                                task.query_type, task.keyword, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_search_results(
                            query=task.keyword, connection=connection,
                            key=api_key, country=country, page=page,
                            request_metadata={
                                "target_brand": target_brand,
                                "brand_aliases": brand_aliases,
                            } if target_brand else None,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    else:
                        products = Amazon.search(
                            query=task.keyword, key=api_key, country=country, page=page,
                        )
                        logger.info("  -> %d products found", len(products))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: query='%s' country=%s page=%d",
                                     task.keyword, country, page)
                    total_errors += 1
                    time.sleep(delay)

    # --- brand_search (search with brand filter) ---
    for task in brand_search_tasks:
        brand_name = resolve_target_brand(task.keyword, task.extra) or task.keyword
        brand_aliases = build_brand_candidates(brand_name, task.extra.get("brand_aliases"))
        query = task.extra.get("query", task.keyword)
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[brand_search] '%s' brand=%s in %s (page %d)...",
                                query, brand_name, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_search_results(
                            query=query, connection=connection,
                            key=api_key, country=country, page=page,
                            brand=brand_name,
                            request_metadata={
                                "target_brand": brand_name,
                                "brand_aliases": brand_aliases,
                            },
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: brand_search '%s'", brand_name)
                    total_errors += 1
                    time.sleep(delay)

    # --- segment_scan (custom keyword-defined segment search) ---
    for task in segment_tasks:
        segment_name = task.extra.get("segment_name", task.keyword)
        target_brand = task.extra.get("target_brand")
        brand_aliases = build_brand_candidates(target_brand, task.extra.get("brand_aliases")) if target_brand else []
        for country in task.countries:
            plan = build_segment_sampling_plan(task.keyword, task.pages, task.extra)
            for item in plan:
                try:
                    logger.info(
                        "[segment_scan] '%s' segment=%s in %s (query=%s, round=%d, page=%d, source=%s)...",
                        task.keyword,
                        segment_name,
                        country,
                        item["query"],
                        item["sample_round"],
                        item["page"],
                        item["query_source"],
                    )
                    if connection:
                        rows = Amazon.fetch_and_store_segment_products(
                            segment_keyword=item["query"],
                            segment_name=segment_name,
                            connection=connection,
                            key=api_key,
                            country=country,
                            page=item["page"],
                            request_metadata={
                                "segment_root_keyword": task.keyword,
                                "segment_keyword": item["query"],
                                "segment_name": segment_name,
                                "query_source": item["query_source"],
                                "sample_round": item["sample_round"],
                                "target_brand": target_brand,
                                "brand_aliases": brand_aliases,
                            },
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    else:
                        products = Amazon.search(
                            query=item["query"], key=api_key, country=country, page=item["page"],
                        )
                        logger.info("  -> %d products found", len(products))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: segment_scan '%s'", task.keyword)
                    total_errors += 1
                    time.sleep(delay)

    # --- target_asin (batch details, 10 per call) ---
    if asin_tasks:
        all_asins = [t.keyword for t in asin_tasks]
        asin_countries = asin_tasks[0].countries
        for country in asin_countries:
            try:
                logger.info("[target_asin] Fetch %d ASINs in %s...", len(all_asins), country)
                if connection:
                    rows = Amazon.fetch_and_store_product_details(
                        asin=all_asins, connection=connection,
                        key=api_key, country=country,
                    )
                    total_rows += len(rows)
                    logger.info("  -> %d rows stored", len(rows))
                else:
                    products = Amazon.get_products(
                        asin=all_asins, key=api_key, country=country,
                    )
                    logger.info("  -> %d product details found", len(products))
                time.sleep(delay)
            except Exception:
                logger.exception("  -> Failed: asins=%s country=%s", all_asins, country)
                total_errors += 1
                time.sleep(delay)

    # --- category_scan (/products-by-category) ---
    for task in category_scan_tasks:
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[category_scan] cat=%s in %s (page %d)...",
                                task.keyword, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_category_products(
                            category_id=task.keyword, connection=connection,
                            key=api_key, country=country, page=page,
                            category_name=task.extra.get("category_name"),
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: category_scan '%s'", task.keyword)
                    total_errors += 1
                    time.sleep(delay)

    # --- bestseller_scan (/best-sellers) ---
    for task in bestseller_tasks:
        for country in task.countries:
            for page in range(1, task.pages + 1):
                try:
                    logger.info("[bestseller_scan] cat=%s in %s (page %d)...",
                                task.keyword, country, page)
                    if connection:
                        rows = Amazon.fetch_and_store_best_sellers(
                            category=task.keyword, connection=connection,
                            key=api_key, country=country, page=page,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: bestseller_scan '%s'", task.keyword)
                    total_errors += 1
                    time.sleep(delay)

    # --- review_scan (/top-product-reviews, 1 call per ASIN) ---
    if review_tasks:
        review_asins = [t.keyword for t in review_tasks]
        review_countries = review_tasks[0].countries
        for country in review_countries:
            for asin_val in review_asins:
                try:
                    logger.info("[review_scan] ASIN=%s in %s...", asin_val, country)
                    if connection:
                        rows = Amazon.fetch_and_store_reviews(
                            asin=asin_val, connection=connection,
                            key=api_key, country=country,
                        )
                        total_rows += len(rows)
                        logger.info("  -> %d review rows stored", len(rows))
                    time.sleep(delay)
                except Exception:
                    logger.exception("  -> Failed: review_scan '%s'", asin_val)
                    total_errors += 1
                    time.sleep(delay)

    # --- offer_scan (/product-offers, batch 10) ---
    if offer_tasks:
        offer_asins = [t.keyword for t in offer_tasks]
        offer_countries = offer_tasks[0].countries
        for country in offer_countries:
            try:
                logger.info("[offer_scan] %d ASINs in %s...", len(offer_asins), country)
                if connection:
                    rows = Amazon.fetch_and_store_offers(
                        asin=offer_asins, connection=connection,
                        key=api_key, country=country,
                    )
                    total_rows += len(rows)
                    logger.info("  -> %d offer rows stored", len(rows))
                time.sleep(delay)
            except Exception:
                logger.exception("  -> Failed: offer_scan country=%s", country)
                total_errors += 1
                time.sleep(delay)

    return total_rows, total_errors


if __name__ == "__main__":
    profile = os.getenv("SCRAPE_PROFILE", "daily")
    api_key = os.getenv("RAPIDAPI_KEY")
    store_to_db = os.getenv("STORE_TO_DB", "false").lower() == "true"

    db_config = load_config_from_db(profile)
    if db_config:
        tasks, delay = db_config
        logger.info("Config source: DATABASE (profile=%s)", profile)
    else:
        tasks, delay = build_tasks_from_env()
        logger.info("Config source: ENV VARS")

    est_calls = _estimate_api_calls(tasks)
    logger.info("Estimated API calls this run: %d", est_calls)

    for t in tasks:
        logger.info("  Task: [%s] '%s' -> %d countries, %d pages, extra=%s",
                     t.query_type, t.keyword, len(t.countries), t.pages, t.extra)

    connection = None
    if store_to_db:
        connection = create_db_connection()

    try:
        total_rows, total_errors = run_tasks(tasks, api_key, delay, connection)
        if connection and get_database_backend() == "postgresql":
            enrichment_rows = enrich_missing_product_details(tasks, api_key, delay, connection)
            if enrichment_rows:
                total_rows += enrichment_rows
            pipeline_counts = run_postgres_pipeline(connection)
            logger.info("PostgreSQL pipeline completed: %s", pipeline_counts)
        logger.info("Done. total_rows=%d, total_errors=%d", total_rows, total_errors)
    finally:
        if connection:
            connection.close()
            close_tunnel()
