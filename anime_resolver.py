import re
import unicodedata
from typing import Any, Dict, List, Optional


def _compact(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _normalize_title(text: str) -> str:
    text = _compact(text).casefold()
    text = unicodedata.normalize("NFKC", text)
    text = re.sub(r"[\\/|]+", " ", text)
    text = re.sub(r"[^\w\s]+", " ", text, flags=re.UNICODE)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_year(item: Dict[str, Any]) -> Optional[int]:
    for key in ("source_year", "tmdb_year", "year"):
        value = item.get(key)
        try:
            if value is not None and str(value).strip():
                return int(str(value).strip()[:4])
        except Exception:
            continue

    source_title = str(item.get("source_title") or "")
    match = re.search(r"\b(19\d{2}|20\d{2}|21\d{2})\b", source_title)
    if match:
        try:
            return int(match.group(1))
        except Exception:
            return None
    return None


def _extract_title_candidates(item: Dict[str, Any]) -> List[str]:
    result: List[str] = []

    def push(value: str) -> None:
        value = _compact(value)
        if value and value not in result:
            result.append(value)

    source_title = str(item.get("source_title") or "")
    cleaned_title = str(item.get("cleaned_title") or "")

    push(cleaned_title)
    push(source_title)

    if "/" in source_title:
        for part in source_title.split("/"):
            push(part)

    paren_parts = re.findall(r"\(([^)]+)\)", source_title)
    for part in paren_parts:
        push(part)

    return result


def should_use_anime_resolver(item: Dict[str, Any]) -> bool:
    bucket = str(item.get("manual_bucket") or item.get("bucket") or "").strip().lower()
    if bucket == "anime":
        return True

    category_name = str(item.get("source_category_name") or "").casefold()
    if "аниме" in category_name:
        return True

    title = str(item.get("source_title") or "")
    if re.search(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff]", title):
        return True

    return False


def resolve_anime_tmdb(item: Dict[str, Any], store: Any) -> Optional[Dict[str, Any]]:
    if not should_use_anime_resolver(item):
        return None

    title_norms: List[str] = []
    for title in _extract_title_candidates(item):
        norm = _normalize_title(title)
        if norm and norm not in title_norms:
            title_norms.append(norm)

    if not title_norms:
        return None

    year = _extract_year(item)
    match = store.find_best(title_norms, year=year)
    if not match:
        return None

    return {
        "tmdb_id": int(match.tmdb_id),
        "media_type": str(match.media_type or "tv"),
        "resolver_source": str(match.source or "unknown"),
        "resolver_confidence": "high",
        "resolver_matched_title": next((t for t in match.titles if _normalize_title(t) in title_norms), match.titles[0] if match.titles else ""),
        "resolver_year": match.year,
    }
