import math
from typing import Any, Dict, List, Optional, Sequence

from .brand_utils import canonicalize_brand_name

_SEGMENT_TERM_EXPANSIONS = {
    "portable": ["travel", "compact", "mini"],
    "travel": ["portable", "compact"],
    "compact": ["portable", "mini"],
    "mini": ["portable", "compact"],
    "coffee machine": ["coffee maker", "espresso machine", "espresso maker"],
    "coffee maker": ["coffee machine", "espresso maker", "portable espresso maker"],
    "espresso machine": ["espresso maker", "coffee machine", "portable espresso maker"],
    "espresso maker": ["espresso machine", "coffee maker"],
}
def parse_brand_aliases(raw_aliases: Any) -> List[str]:
    if raw_aliases is None:
        return []
    if isinstance(raw_aliases, str):
        values = [item.strip() for item in raw_aliases.split(",")]
    elif isinstance(raw_aliases, Sequence):
        values = [str(item).strip() for item in raw_aliases if str(item).strip()]
    else:
        values = [str(raw_aliases).strip()]
    seen = set()
    aliases: List[str] = []
    for value in values:
        canonical = canonicalize_brand_name(value)
        if canonical and canonical.lower() not in seen:
            seen.add(canonical.lower())
            aliases.append(canonical)
    return aliases


def build_brand_candidates(primary_brand: Optional[str], aliases: Any = None) -> List[str]:
    candidates = []
    if primary_brand:
        candidates.append(primary_brand)
    candidates.extend(parse_brand_aliases(aliases))
    seen = set()
    normalized: List[str] = []
    for brand in candidates:
        canonical = canonicalize_brand_name(brand)
        if canonical and canonical.lower() not in seen:
            seen.add(canonical.lower())
            normalized.append(canonical)
    return normalized


def resolve_target_brand(task_keyword: str, task_extra: Optional[Dict[str, Any]] = None) -> Optional[str]:
    extra = task_extra or {}
    for key in ("target_brand", "brand", "brand_name"):
        if extra.get(key):
            return canonicalize_brand_name(extra.get(key))
    return canonicalize_brand_name(task_keyword)


def build_segment_query_variants(
    segment_keyword: str,
    extra: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, str]]:
    extra = extra or {}
    base_query = " ".join(segment_keyword.strip().split())
    if not base_query:
        return []

    variants: List[Dict[str, str]] = [{"query": base_query, "source": "seed"}]
    manual_variants = extra.get("query_variants") or []
    if isinstance(manual_variants, str):
        manual_variants = [item.strip() for item in manual_variants.split(",") if item.strip()]
    for variant in manual_variants:
        variants.append({"query": " ".join(str(variant).split()), "source": "manual"})

    enable_auto_expand = bool(extra.get("enable_query_perturbation", True))
    related_terms = extra.get("related_terms") or []
    if isinstance(related_terms, str):
        related_terms = [item.strip() for item in related_terms.split(",") if item.strip()]

    if enable_auto_expand:
        lowered = base_query.lower()
        auto_queries: List[str] = []
        for phrase, replacements in _SEGMENT_TERM_EXPANSIONS.items():
            if phrase in lowered:
                for replacement in replacements:
                    auto_queries.append(lowered.replace(phrase, replacement))
        for related in related_terms:
            if related.lower() not in lowered:
                auto_queries.append(f"{lowered} {related.lower()}")
        max_auto = int(extra.get("auto_variant_limit", 4) or 4)
        for query in auto_queries[:max_auto]:
            variants.append({"query": " ".join(query.split()), "source": "auto"})

    deduped: List[Dict[str, str]] = []
    seen = set()
    for item in variants:
        query = item["query"].strip()
        if not query:
            continue
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        deduped.append({"query": query, "source": item["source"]})
    return deduped


def build_segment_sampling_plan(
    segment_keyword: str,
    pages: int,
    extra: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    extra = extra or {}
    variants = build_segment_query_variants(segment_keyword, extra)
    repeat_k = max(int(extra.get("repeat_k", 1) or 1), 1)
    results_per_page = max(int(extra.get("results_per_page", 20) or 20), 1)
    top_n = int(extra.get("top_n", 0) or 0)
    page_count = max(int(extra.get("sampling_pages", pages) or pages), 1)
    if top_n > 0:
        page_count = max(page_count, int(math.ceil(top_n / results_per_page)))

    raw_budget = int(extra.get("sampling_budget", 0) or 0)
    budget = raw_budget if raw_budget > 0 else None
    plan: List[Dict[str, Any]] = []
    for variant in variants:
        for sample_round in range(1, repeat_k + 1):
            for page in range(1, page_count + 1):
                plan.append(
                    {
                        "query": variant["query"],
                        "query_source": variant["source"],
                        "sample_round": sample_round,
                        "page": page,
                    }
                )
                if budget is not None and len(plan) >= budget:
                    return plan
    return plan


def estimate_segment_sampling_calls(task_keyword: str, pages: int, extra: Optional[Dict[str, Any]] = None) -> int:
    return len(build_segment_sampling_plan(task_keyword, pages, extra))
