from typing import Any, Dict

from content_buckets import item_content_bucket
from country_helpers import effective_item_countries, parse_country_codes
from item_years import item_filter_years
from subscription_presets import detect_subscription_preset_key
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


def _excluded_country_blocks_item(sub: Dict[str, Any], item_countries: set[str], bucket: str) -> bool:
    excluded_countries = set(parse_country_codes(sub.get("exclude_country_codes") or sub.get("exclude_country_codes_list")))
    if not excluded_countries or not item_countries:
        return False

    matched_excluded = item_countries & excluded_countries
    if not matched_excluded:
        return False

    # "World" is a fallback route for regular content. Co-productions like US+JP
    # should stay in world as long as they also have a non-excluded country.
    if detect_subscription_preset_key(sub) == "world" and bucket == "regular":
        return item_countries <= excluded_countries

    return True


def explain_subscription_match(db: Any, sub: Dict[str, Any], item: Dict[str, Any]) -> str:
    if _is_globally_ignored_item(item):
        return "globally_ignored"

    if not sub or not item:
        return "missing_sub_or_item"
    if not sub.get("is_enabled"):
        return "disabled"

    sub_media = sub.get("media_type") or "any"
    item_media = item.get("media_type") or "movie"
    if item_media == "other" and sub_media != "other":
        return "media_other_mismatch"
    if sub_media != "any" and sub_media != item_media:
        return f"media_mismatch:{item_media}"

    year_from = sub.get("year_from")
    year_to = sub.get("year_to")
    item_years = item_filter_years(item)
    if year_from is not None or year_to is not None:
        if not item_years:
            return "year_missing"
        lo = int(year_from) if year_from is not None else min(item_years)
        hi = int(year_to) if year_to is not None else max(item_years)
        if not any(lo <= int(year) <= hi for year in item_years):
            return f"year_mismatch:{sorted(item_years)}"

    allow_formats = []
    if sub.get("allow_720"):
        allow_formats.append("720")
    if sub.get("allow_1080"):
        allow_formats.append("1080")
    if sub.get("allow_2160"):
        allow_formats.append("2160")
    if allow_formats:
        actual_format = str(item.get("source_format") or "")
        if actual_format not in allow_formats:
            return f"format_mismatch:{actual_format}"

    min_rating = sub.get("min_tmdb_rating")
    item_rating = item.get("tmdb_rating")
    if min_rating is not None:
        if item_rating is None:
            return "rating_missing"
        if float(item_rating) < float(min_rating):
            return f"rating_mismatch:{item_rating}"

    sub_genres = set(sub.get("genre_ids") or db.get_subscription_genres(int(sub["id"])))
    if sub_genres:
        item_genres = {int(g) for g in item.get("genre_ids", [])}
        if not (item_genres & sub_genres):
            return f"genre_mismatch:{sorted(item_genres)}"

    content_filter = str(sub.get("content_filter") or "any")
    bucket = item_content_bucket(item)
    if content_filter == "only_anime" and bucket != "anime":
        return f"bucket_mismatch:{bucket}"
    if content_filter == "only_dorama" and bucket != "dorama":
        return f"bucket_mismatch:{bucket}"
    if content_filter == "exclude_anime" and bucket == "anime":
        return "bucket_excluded:anime"
    if content_filter == "exclude_dorama" and bucket == "dorama":
        return "bucket_excluded:dorama"
    if content_filter == "exclude_anime_dorama" and bucket in {"anime", "dorama"}:
        return f"bucket_excluded:{bucket}"

    item_countries = set(effective_item_countries(item))
    sub_countries = set(parse_country_codes(sub.get("country_codes") or sub.get("country_codes_list")))
    if sub_countries:
        if not item_countries:
            return "country_missing"
        if not (item_countries & sub_countries):
            return f"country_mismatch:{sorted(item_countries)}"

    excluded_countries = set(parse_country_codes(sub.get("exclude_country_codes") or sub.get("exclude_country_codes_list")))
    if _excluded_country_blocks_item(sub, item_countries, bucket):
        return f"excluded_country:{sorted(item_countries & excluded_countries)}"

    text_haystack, tech_haystack = build_keyword_haystacks(item)

    include = [x.strip().lower() for x in (sub.get("include_keywords") or "").split(",") if x.strip()]
    exclude = [x.strip().lower() for x in (sub.get("exclude_keywords") or "").split(",") if x.strip()]

    if include:
        matched_include = [word for word in include if keyword_matches_item(word, item, text_haystack, tech_haystack)]
        if not matched_include:
            return "include_keyword_mismatch"

    if exclude:
        matched_exclude = [word for word in exclude if keyword_matches_item(word, item, text_haystack, tech_haystack)]
        if matched_exclude:
            return f"exclude_keyword:{'|'.join(matched_exclude)}"

    return "passed"


def match_subscription(db: Any, sub: Dict[str, Any], item: Dict[str, Any]) -> bool:
    return explain_subscription_match(db, sub, item) == "passed"
