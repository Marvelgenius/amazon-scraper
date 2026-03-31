import html
import re
from typing import Any, Dict, Optional, Tuple

_MARKETPLACE_PREFIX_RE = re.compile(
    r"【[^】]*(?:限定|限り|セール|特選)[^】]*】\s*",
)

_CURLY_QUOTE_MAP = str.maketrans({
    "\u2018": "'",
    "\u2019": "'",
    "\u201C": '"',
    "\u201D": '"',
})

_KNOWN_BRAND_ALIASES = {
    "black+decker": "BLACK+DECKER",
    "de longhi": "De'Longhi",
    "de'longhi": "De'Longhi",
    "delonghi": "De'Longhi",
    "mr coffee": "Mr. Coffee",
    "mr. coffee": "Mr. Coffee",
    "mr. coffee®": "Mr. Coffee",
    "nescafe": "NESCAFÉ",
    "nescafé": "NESCAFÉ",
    "outin": "OutIn",
}

_NOISE_BRAND_TOKENS = {
    "battery",
    "brewer",
    "brews",
    "camping",
    "capsule",
    "car",
    "coffee",
    "compact",
    "cuban",
    "electric",
    "espresso",
    "gift",
    "ground",
    "heating",
    "hiking",
    "maker",
    "manual",
    "mini",
    "personal",
    "pod",
    "portable",
    "powered",
    "rechargeable",
    "self-heating",
    "self",
    "serve",
    "single",
    "travel",
}

_MULTIWORD_TITLE_CANDIDATES = (
    ("mr.", "coffee"),
    ("mr", "coffee"),
    ("black+decker",),
    ("de'longhi",),
    ("de", "longhi"),
)

_TRAILING_SYMBOL_RE = re.compile(r"^[^\w]+|[^\w.+&'/-]+$")
_NOISE_BRAND_RE = re.compile(r"^\d+(?:\.\d+)?(?:-in-\d+)?$", re.IGNORECASE)
_TITLE_SPLIT_RE = re.compile(r"\s+")
_BRAND_PREFIX_RE = re.compile(r"^(?:brand|store)\s*:\s*", re.IGNORECASE)


def clean_text_fragment(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    cleaned = html.unescape(str(text))
    cleaned = cleaned.translate(_CURLY_QUOTE_MAP)
    cleaned = _MARKETPLACE_PREFIX_RE.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _strip_brand_edge_punctuation(value: str) -> str:
    stripped = _TRAILING_SYMBOL_RE.sub("", value.strip())
    stripped = stripped.strip("()[]{}")
    return stripped.strip()


def _normalize_lookup_key(value: str) -> str:
    lowered = value.lower().replace("®", "").replace("™", "")
    lowered = lowered.replace("’", "'")
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def canonicalize_brand_name(brand: Optional[str]) -> Optional[str]:
    cleaned = clean_text_fragment(brand)
    if not cleaned:
        return None
    cleaned = _strip_brand_edge_punctuation(cleaned)
    if not cleaned:
        return None

    lookup = _normalize_lookup_key(cleaned)
    if not lookup:
        return None

    alias = _KNOWN_BRAND_ALIASES.get(lookup)
    if alias:
        return alias
    if lookup.startswith("de'longhi") or lookup.startswith("de longhi") or lookup.startswith("delonghi"):
        return "De'Longhi"
    return cleaned


def is_plausible_brand_name(brand: Optional[str]) -> bool:
    canonical = canonicalize_brand_name(brand)
    if not canonical:
        return False

    lookup = _normalize_lookup_key(canonical)
    tokens = [token for token in re.split(r"[\s/]+", lookup) if token]
    if not tokens:
        return False
    if len(tokens) == 1:
        token = tokens[0].strip(".")
        if len(token) <= 1:
            return False
        if _NOISE_BRAND_RE.match(token):
            return False
        if token in _NOISE_BRAND_TOKENS:
            return False
    else:
        joined = " ".join(tokens)
        if joined in _NOISE_BRAND_TOKENS:
            return False
    if all(token.strip(".") in _NOISE_BRAND_TOKENS for token in tokens):
        return False
    return True


def clean_brand_name(brand: Optional[str]) -> Optional[str]:
    canonical = canonicalize_brand_name(brand)
    if not canonical:
        return None
    return canonical if is_plausible_brand_name(canonical) else None


def _extract_brand_from_byline(byline: Optional[str]) -> Optional[str]:
    cleaned = clean_text_fragment(byline)
    if not cleaned:
        return None
    if cleaned.lower().startswith("visit the ") and cleaned.lower().endswith(" store"):
        return clean_brand_name(cleaned[len("Visit the ") : -len(" Store")])
    stripped_prefix = _BRAND_PREFIX_RE.sub("", cleaned)
    if stripped_prefix != cleaned:
        return clean_brand_name(stripped_prefix)
    return None


def _build_title_prefix_candidates(title: str) -> list[str]:
    tokens = [token for token in _TITLE_SPLIT_RE.split(title) if token]
    if not tokens:
        return []

    lower_tokens = tuple(_normalize_lookup_key(token).strip(".") for token in tokens[:3])
    candidates: list[str] = []

    for parts in _MULTIWORD_TITLE_CANDIDATES:
        normalized = tuple(_normalize_lookup_key(part).strip(".") for part in parts)
        if lower_tokens[: len(normalized)] == normalized:
            candidates.append(" ".join(tokens[: len(normalized)]))

    candidates.append(tokens[0])
    return candidates


def extract_brand_from_title(title: Optional[str]) -> Optional[str]:
    cleaned = clean_text_fragment(title)
    if not cleaned:
        return None

    for candidate in _build_title_prefix_candidates(cleaned):
        normalized = clean_brand_name(candidate)
        if normalized:
            return normalized
    return None


def extract_authoritative_brand_from_payload(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    product_info = payload.get("product_information", {})
    if isinstance(product_info, dict):
        for key in ("Brand", "brand"):
            candidate = clean_brand_name(product_info.get(key))
            if candidate:
                return candidate, f"product_information.{key}"

    for key in ("brand", "brand_name"):
        candidate = clean_brand_name(payload.get(key))
        if candidate:
            return candidate, key

    byline = _extract_brand_from_byline(payload.get("product_byline"))
    if byline:
        return byline, "product_byline"

    return None, None


def extract_brand_from_payload(payload: Dict[str, Any]) -> Optional[str]:
    authoritative_brand, _ = extract_authoritative_brand_from_payload(payload)
    if authoritative_brand:
        return authoritative_brand

    title_brand = extract_brand_from_title(payload.get("product_title") or payload.get("title"))
    return title_brand


def normalize_title_key(title: Optional[str]) -> Optional[str]:
    cleaned = clean_text_fragment(title)
    if not cleaned:
        return None
    lowered = cleaned.lower()
    lowered = lowered.replace("&", " and ")
    lowered = re.sub(r"[^a-z0-9]+", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered or None
