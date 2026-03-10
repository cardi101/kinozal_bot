from typing import Any, Dict

from content_buckets import item_content_bucket
from country_helpers import effective_item_countries, parse_country_codes
from item_years import item_filter_years
from keyword_filters import build_keyword_haystacks, keyword_matches_item


def _item_field(item, *names):
    for name in names:
        if isinstance(item, dict):
            value = item.get(name)
        else:
            value = getattr(item, name, None)
        if value not in (None, ""):
            return value
    return ""


def _is_globally_ignored_item(item) -> bool:
    source_category = str(
        _item_field(
            item,
            "source_category_name",
            "source_category",
            "api_category",
            "category_api",
        )
    ).strip().lower()
    return source_category == "кино - концерт" or "концерт" in source_category


def match_subscription(db: Any, sub: Dict[str, Any], item: Dict[str, Any]) -> bool:
    if _is_globally_ignored_item(item):
        return False

    if not sub or not item:
        return False
    if not sub.get("is_enabled"):
        return False

    sub_media = sub.get("media_type") or "any"
    item_media = item.get("media_type") or "movie"
    if item_media == "other" and sub_media != "other":
        return False
    if sub_media != "any" and sub_media != item_media:
        return False

    year_from = sub.get("year_from")
    year_to = sub.get("year_to")
    item_years = item_filter_years(item)
    if year_from is not None or year_to is not None:
        if not item_years:
            return False
        lo = int(year_from) if year_from is not None else min(item_years)
        hi = int(year_to) if year_to is not None else max(item_years)
        if not any(lo <= int(year) <= hi for year in item_years):
            return False

    allow_formats = []
    if sub.get("allow_720"):
        allow_formats.append("720")
    if sub.get("allow_1080"):
        allow_formats.append("1080")
    if sub.get("allow_2160"):
        allow_formats.append("2160")
    if allow_formats:
        if (item.get("source_format") or "") not in allow_formats:
            return False

    min_rating = sub.get("min_tmdb_rating")
    item_rating = item.get("tmdb_rating")
    if min_rating is not None:
        if item_rating is None or float(item_rating) < float(min_rating):
            return False

    sub_genres = set(sub.get("genre_ids") or db.get_subscription_genres(int(sub["id"])))
    if sub_genres:
        item_genres = {int(g) for g in item.get("genre_ids", [])}
        if not (item_genres & sub_genres):
            return False

    content_filter = str(sub.get("content_filter") or "any")
    bucket = item_content_bucket(item)
    if content_filter == "only_anime" and bucket != "anime":
        return False
    if content_filter == "only_dorama" and bucket != "dorama":
        return False
    if content_filter == "exclude_anime" and bucket == "anime":
        return False
    if content_filter == "exclude_dorama" and bucket == "dorama":
        return False
    if content_filter == "exclude_anime_dorama" and bucket in {"anime", "dorama"}:
        return False

    item_countries = set(effective_item_countries(item))
    sub_countries = set(parse_country_codes(sub.get("country_codes") or sub.get("country_codes_list")))
    if sub_countries:
        if not item_countries or not (item_countries & sub_countries):
            return False

    excluded_countries = set(parse_country_codes(sub.get("exclude_country_codes") or sub.get("exclude_country_codes_list")))
    if excluded_countries and item_countries and (item_countries & excluded_countries):
        return False

    text_haystack, tech_haystack = build_keyword_haystacks(item)

    include = [x.strip().lower() for x in (sub.get("include_keywords") or "").split(",") if x.strip()]
    exclude = [x.strip().lower() for x in (sub.get("exclude_keywords") or "").split(",") if x.strip()]

    if include and not any(keyword_matches_item(word, item, text_haystack, tech_haystack) for word in include):
        return False
    if exclude and any(keyword_matches_item(word, item, text_haystack, tech_haystack) for word in exclude):
        return False

    return True
