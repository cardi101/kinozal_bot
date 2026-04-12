from typing import Any, Dict, List

from country_helpers import (
    ANIME_COUNTRY_CODES,
    asian_dorama_signal_score,
    effective_item_countries,
    has_asian_script,
    normalize_tmdb_language,
    parse_jsonish_list,
)
from source_categories import normalize_source_category_id, source_category_bucket_hint
from title_prep import split_title_parts
from tmdb_aliases import ANIME_TITLE_MARKER_RE, anime_alias_candidates_from_text
from utils import compact_spaces


def anime_fallback_signal_score(item: Dict[str, Any]) -> int:
    score = 0
    genres = {int(g) for g in parse_jsonish_list(item.get("genre_ids")) if str(g).isdigit()}
    countries = set(effective_item_countries(item))
    lang = normalize_tmdb_language(item.get("tmdb_original_language"))

    if 16 in genres:
        score += 3
    if countries & ANIME_COUNTRY_CODES:
        score += 2
    if lang in {"ja", "zh", "ko"}:
        score += 2

    source_title = compact_spaces(str(item.get("source_title") or ""))
    cleaned_title = compact_spaces(str(item.get("cleaned_title") or ""))
    tmdb_title = compact_spaces(str(item.get("tmdb_title") or ""))
    tmdb_original_title = compact_spaces(str(item.get("tmdb_original_title") or ""))
    texts = [source_title, cleaned_title, tmdb_title, tmdb_original_title]

    alias_hits: List[str] = []
    for text in texts:
        for alias in anime_alias_candidates_from_text(text):
            if alias not in alias_hits:
                alias_hits.append(alias)
    if alias_hits:
        score += 2

    if any(ANIME_TITLE_MARKER_RE.search(text or "") for text in texts):
        score += 2

    if alias_hits:
        ru, en = split_title_parts(source_title)
        if compact_spaces(ru) and compact_spaces(en):
            score += 1

    return score


def resolve_item_content_bucket(item: Dict[str, Any]) -> Dict[str, str]:
    manual_bucket = str(item.get("manual_bucket") or "").strip().lower()
    if manual_bucket in {"anime", "dorama", "regular"}:
        return {"bucket": manual_bucket, "reason": "manual_bucket_override"}

    category_bucket = source_category_bucket_hint(item.get("source_category_id"), item.get("source_category_name"))
    media_type = str(item.get("media_type") or "movie")
    category_id = normalize_source_category_id(item.get("source_category_id") or item.get("source_category_name"))
    category_name = compact_spaces(str(item.get("source_category_name") or ""))
    source_is_animation = category_id in {"20", "21", "22", "1003"} or "мульт" in category_name.casefold()
    countries = set(effective_item_countries(item))
    genres = {int(g) for g in parse_jsonish_list(item.get("genre_ids")) if str(g).isdigit()}
    is_animation = 16 in genres or source_is_animation
    anime_score = anime_fallback_signal_score(item)

    source_title = compact_spaces(str(item.get("source_title") or ""))
    cleaned_title = compact_spaces(str(item.get("cleaned_title") or ""))
    tmdb_title = compact_spaces(str(item.get("tmdb_title") or ""))
    tmdb_original_title = compact_spaces(str(item.get("tmdb_original_title") or ""))
    texts = [source_title, cleaned_title, tmdb_title, tmdb_original_title]
    lang = normalize_tmdb_language(item.get("tmdb_original_language"))
    has_anime_country = bool(countries & ANIME_COUNTRY_CODES)
    has_anime_language = lang in {"ja", "zh", "ko"}
    has_anime_script = any(has_asian_script(text) for text in texts if text)
    has_anime_marker = any(ANIME_TITLE_MARKER_RE.search(text) for text in texts if text)
    strong_anime_signal = has_anime_country or has_anime_language or has_anime_script or has_anime_marker
    dorama_score = asian_dorama_signal_score(item)

    if category_bucket == "anime":
        return {"bucket": "anime", "reason": "source_category_anime_hint"}
    if source_is_animation and strong_anime_signal and anime_score >= 2:
        if category_bucket == "dorama":
            return {"bucket": "anime", "reason": "asian_animation_overrides_dorama_hint"}
        return {"bucket": "anime", "reason": "asian_animation_invariant"}
    if is_animation and strong_anime_signal and anime_score >= 3:
        if category_bucket == "dorama":
            return {"bucket": "anime", "reason": "asian_animation_overrides_dorama_hint"}
        return {"bucket": "anime", "reason": "asian_animation_genre_invariant"}
    if category_bucket == "dorama" and not is_animation and media_type in {"tv", "movie"}:
        return {"bucket": "dorama", "reason": "source_category_dorama_hint"}
    if media_type in {"tv", "movie"} and not is_animation:
        if not item.get("tmdb_id") and strong_anime_signal and anime_score >= 3:
            return {"bucket": "anime", "reason": "anime_fallback_without_tmdb"}
        if dorama_score >= 2:
            return {"bucket": "dorama", "reason": "asian_dorama_signal"}
    return {"bucket": "regular", "reason": "default_regular"}


def item_content_bucket(item: Dict[str, Any]) -> str:
    return resolve_item_content_bucket(item)["bucket"]
