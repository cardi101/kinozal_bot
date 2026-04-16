from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from content_buckets import item_content_bucket
from country_helpers import effective_item_countries, parse_country_codes
from domain import CompiledSubscription
from item_years import item_filter_years
from keyword_filters import (
    TECH_KEYWORD_REGEXES,
    build_keyword_haystacks,
    keyword_matches_item,
    split_keyword_tokens,
)
from subscription_presets import detect_subscription_preset_key
from utils import compact_spaces


def _item_field(item, *names):
    for name in names:
        if isinstance(item, dict):
            value = item.get(name)
        else:
            value = getattr(item, name, None)
        if value not in (None, ""):
            return value
    return ""


def _item_payload(item: Any) -> Dict[str, Any]:
    if hasattr(item, "to_dict"):
        return item.to_dict()
    return dict(item or {})


def _sub_payload(sub: Any) -> Dict[str, Any]:
    if isinstance(sub, CompiledSubscription):
        return sub.to_dict()
    if hasattr(sub, "to_dict"):
        return sub.to_dict()
    return dict(sub or {})


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


def _keyword_mode(token: str) -> str:
    token = compact_spaces(str(token or "")).lower()
    if not token:
        return "text"
    if token in TECH_KEYWORD_REGEXES:
        return "special"
    if len(token) <= 3:
        return "word"
    return "text"


def _sorted_tuple(values: Iterable[Any], *, cast=str) -> tuple:
    normalized: List[Any] = []
    for value in values:
        try:
            normalized.append(cast(value))
        except Exception:
            continue
    return tuple(sorted(set(normalized)))


def compile_subscription(db: Any, sub: Any) -> CompiledSubscription:
    if isinstance(sub, CompiledSubscription):
        return sub

    payload = _sub_payload(sub)
    sub_id = int(payload.get("id") or 0)
    genre_ids = payload.get("genre_ids")
    if genre_ids is None and db is not None and sub_id:
        genre_ids = db.get_subscription_genres(sub_id)

    allow_formats: List[str] = []
    if payload.get("allow_720"):
        allow_formats.append("720")
    if payload.get("allow_1080"):
        allow_formats.append("1080")
    if payload.get("allow_2160"):
        allow_formats.append("2160")

    min_rating = payload.get("min_tmdb_rating")
    try:
        min_rating_value = float(min_rating) if min_rating is not None else None
    except Exception:
        min_rating_value = None

    include_keywords = split_keyword_tokens(payload.get("include_keywords"))
    exclude_keywords = split_keyword_tokens(payload.get("exclude_keywords"))
    include_keyword_modes = {token: _keyword_mode(token) for token in include_keywords}
    exclude_keyword_modes = {token: _keyword_mode(token) for token in exclude_keywords}

    year_from = payload.get("year_from")
    year_to = payload.get("year_to")
    try:
        year_from_value = int(year_from) if year_from is not None else None
    except Exception:
        year_from_value = None
    try:
        year_to_value = int(year_to) if year_to is not None else None
    except Exception:
        year_to_value = None

    return CompiledSubscription.from_payload(
        payload,
        media_type=compact_spaces(str(payload.get("media_type") or "any")).lower() or "any",
        year_from=year_from_value,
        year_to=year_to_value,
        allow_formats=_sorted_tuple(allow_formats),
        min_rating=min_rating_value,
        genre_ids=_sorted_tuple(genre_ids or [], cast=int),
        content_filter=compact_spaces(str(payload.get("content_filter") or "any")).lower() or "any",
        country_codes=_sorted_tuple(
            parse_country_codes(payload.get("country_codes") or payload.get("country_codes_list")),
            cast=str,
        ),
        exclude_country_codes=_sorted_tuple(
            parse_country_codes(payload.get("exclude_country_codes") or payload.get("exclude_country_codes_list")),
            cast=str,
        ),
        include_keywords=tuple(include_keywords),
        exclude_keywords=tuple(exclude_keywords),
        include_keyword_modes=include_keyword_modes,
        exclude_keyword_modes=exclude_keyword_modes,
    )


@dataclass(slots=True)
class _PreparedItemContext:
    payload: Dict[str, Any]
    media_type: str
    bucket: str
    years: tuple[int, ...]
    source_format: str
    rating: Optional[float]
    genre_ids: frozenset[int]
    countries: frozenset[str]
    globally_ignored: bool
    _text_haystack: str = ""
    _tech_haystack: str = ""

    @classmethod
    def from_item(cls, item: Any) -> "_PreparedItemContext":
        payload = _item_payload(item)
        rating = payload.get("tmdb_rating")
        try:
            rating_value = float(rating) if rating is not None else None
        except Exception:
            rating_value = None
        return cls(
            payload=payload,
            media_type=compact_spaces(str(payload.get("media_type") or "movie")).lower() or "movie",
            bucket=item_content_bucket(payload),
            years=_sorted_tuple(item_filter_years(payload), cast=int),
            source_format=compact_spaces(str(payload.get("source_format") or "")),
            rating=rating_value,
            genre_ids=frozenset(int(value) for value in (payload.get("genre_ids") or [])),
            countries=frozenset(effective_item_countries(payload)),
            globally_ignored=_is_globally_ignored_item(payload),
        )

    def keyword_haystacks(self) -> tuple[str, str]:
        if not self._text_haystack and not self._tech_haystack:
            self._text_haystack, self._tech_haystack = build_keyword_haystacks(self.payload)
        return self._text_haystack, self._tech_haystack


def _check_entry(check: str, passed: bool, **values: Any) -> Dict[str, Any]:
    payload = {"check": check, "passed": bool(passed)}
    for key, value in values.items():
        if value not in (None, "", [], (), {}, set()):
            payload[key] = value
    return payload


def _excluded_country_blocks_item(sub: CompiledSubscription, item_countries: set[str], bucket: str) -> bool:
    excluded_countries = set(sub.exclude_country_codes)
    if not excluded_countries or not item_countries:
        return False

    matched_excluded = item_countries & excluded_countries
    if not matched_excluded:
        return False

    if detect_subscription_preset_key(sub.to_dict()) == "world" and bucket == "regular":
        return item_countries <= excluded_countries

    return True


def _evaluate_subscription_match(
    db: Any,
    sub: Any,
    item: Any,
) -> Dict[str, Any]:
    if not sub or not item:
        return {
            "summary": "missing_sub_or_item",
            "checks": [_check_entry("input", False, reason="missing_sub_or_item")],
            "compiled_subscription": None,
        }

    compiled = compile_subscription(db, sub)
    ctx = _PreparedItemContext.from_item(item)
    checks: List[Dict[str, Any]] = []

    if ctx.globally_ignored:
        checks.append(_check_entry("global_ignore", False, source_category=ctx.payload.get("source_category_name"), reason="globally_ignored"))
        return {"summary": "globally_ignored", "checks": checks, "compiled_subscription": compiled}

    if not compiled.get("is_enabled"):
        checks.append(_check_entry("enabled", False, actual=False, reason="disabled"))
        return {"summary": "disabled", "checks": checks, "compiled_subscription": compiled}
    checks.append(_check_entry("enabled", True, actual=True))

    sub_media = compiled.media_type
    item_media = ctx.media_type
    if item_media == "other" and sub_media != "other":
        checks.append(_check_entry("media", False, expected=[sub_media], actual=item_media, reason="media_other_mismatch"))
        return {"summary": "media_other_mismatch", "checks": checks, "compiled_subscription": compiled}
    if sub_media != "any" and sub_media != item_media:
        checks.append(_check_entry("media", False, expected=[sub_media], actual=item_media, reason=f"media_mismatch:{item_media}"))
        return {"summary": f"media_mismatch:{item_media}", "checks": checks, "compiled_subscription": compiled}
    checks.append(_check_entry("media", True, expected=[sub_media], actual=item_media))

    if compiled.year_from is not None or compiled.year_to is not None:
        if not ctx.years:
            checks.append(_check_entry("year", False, expected_min=compiled.year_from, expected_max=compiled.year_to, actual=[], reason="year_missing"))
            return {"summary": "year_missing", "checks": checks, "compiled_subscription": compiled}
        lo = compiled.year_from if compiled.year_from is not None else min(ctx.years)
        hi = compiled.year_to if compiled.year_to is not None else max(ctx.years)
        if not any(lo <= int(year) <= hi for year in ctx.years):
            reason = f"year_mismatch:{sorted(ctx.years)}"
            checks.append(_check_entry("year", False, expected_min=lo, expected_max=hi, actual=sorted(ctx.years), reason=reason))
            return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
        checks.append(_check_entry("year", True, expected_min=lo, expected_max=hi, actual=sorted(ctx.years)))
    else:
        checks.append(_check_entry("year", True, configured=False, actual=sorted(ctx.years)))

    if compiled.allow_formats:
        if ctx.source_format not in compiled.allow_formats:
            reason = f"format_mismatch:{ctx.source_format}"
            checks.append(_check_entry("format", False, expected=list(compiled.allow_formats), actual=ctx.source_format, reason=reason))
            return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
        checks.append(_check_entry("format", True, expected=list(compiled.allow_formats), actual=ctx.source_format))
    else:
        checks.append(_check_entry("format", True, configured=False, actual=ctx.source_format))

    if compiled.min_rating is not None:
        if ctx.rating is None:
            checks.append(_check_entry("rating", False, expected_min=compiled.min_rating, actual=None, reason="rating_missing"))
            return {"summary": "rating_missing", "checks": checks, "compiled_subscription": compiled}
        if float(ctx.rating) < float(compiled.min_rating):
            reason = f"rating_mismatch:{ctx.rating}"
            checks.append(_check_entry("rating", False, expected_min=compiled.min_rating, actual=ctx.rating, reason=reason))
            return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
        checks.append(_check_entry("rating", True, expected_min=compiled.min_rating, actual=ctx.rating))
    else:
        checks.append(_check_entry("rating", True, configured=False, actual=ctx.rating))

    content_filter = compiled.content_filter
    bucket = ctx.bucket
    if content_filter == "only_anime" and bucket != "anime":
        reason = f"bucket_mismatch:{bucket}"
        checks.append(_check_entry("bucket", False, expected=["anime"], actual=bucket, reason=reason))
        return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
    if content_filter == "only_dorama" and bucket != "dorama":
        reason = f"bucket_mismatch:{bucket}"
        checks.append(_check_entry("bucket", False, expected=["dorama"], actual=bucket, reason=reason))
        return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
    if content_filter == "exclude_anime" and bucket == "anime":
        checks.append(_check_entry("bucket", False, blocked=["anime"], actual=bucket, reason="bucket_excluded:anime"))
        return {"summary": "bucket_excluded:anime", "checks": checks, "compiled_subscription": compiled}
    if content_filter == "exclude_dorama" and bucket == "dorama":
        checks.append(_check_entry("bucket", False, blocked=["dorama"], actual=bucket, reason="bucket_excluded:dorama"))
        return {"summary": "bucket_excluded:dorama", "checks": checks, "compiled_subscription": compiled}
    if content_filter == "exclude_anime_dorama" and bucket in {"anime", "dorama"}:
        reason = f"bucket_excluded:{bucket}"
        checks.append(_check_entry("bucket", False, blocked=["anime", "dorama"], actual=bucket, reason=reason))
        return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
    checks.append(_check_entry("bucket", True, expected=[content_filter], actual=bucket))

    if compiled.genre_ids:
        if not (ctx.genre_ids & set(compiled.genre_ids)):
            reason = f"genre_mismatch:{sorted(ctx.genre_ids)}"
            checks.append(
                _check_entry(
                    "genre",
                    False,
                    expected=sorted(compiled.genre_ids),
                    actual=sorted(ctx.genre_ids),
                    reason=reason,
                )
            )
            return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
        checks.append(
            _check_entry(
                "genre",
                True,
                expected=sorted(compiled.genre_ids),
                actual=sorted(ctx.genre_ids),
                matched=sorted(ctx.genre_ids & set(compiled.genre_ids)),
            )
        )
    else:
        checks.append(_check_entry("genre", True, configured=False, actual=sorted(ctx.genre_ids)))

    if compiled.country_codes:
        if not ctx.countries:
            checks.append(_check_entry("country", False, expected=sorted(compiled.country_codes), actual=[], reason="country_missing"))
            return {"summary": "country_missing", "checks": checks, "compiled_subscription": compiled}
        matched_countries = sorted(ctx.countries & set(compiled.country_codes))
        if not matched_countries:
            reason = f"country_mismatch:{sorted(ctx.countries)}"
            checks.append(_check_entry("country", False, expected=sorted(compiled.country_codes), actual=sorted(ctx.countries), reason=reason))
            return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
        checks.append(_check_entry("country", True, expected=sorted(compiled.country_codes), actual=sorted(ctx.countries), matched=matched_countries))
    else:
        checks.append(_check_entry("country", True, configured=False, actual=sorted(ctx.countries)))

    if _excluded_country_blocks_item(compiled, set(ctx.countries), bucket):
        blocked = sorted(set(ctx.countries) & set(compiled.exclude_country_codes))
        reason = f"excluded_country:{blocked}"
        checks.append(_check_entry("excluded_country", False, blocked=blocked, actual=sorted(ctx.countries), reason=reason))
        return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
    checks.append(_check_entry("excluded_country", True, blocked=sorted(compiled.exclude_country_codes), actual=sorted(ctx.countries)))

    text_haystack = ""
    tech_haystack = ""
    if compiled.include_keywords or compiled.exclude_keywords:
        text_haystack, tech_haystack = ctx.keyword_haystacks()

    if compiled.include_keywords:
        matched_include = [
            token
            for token in compiled.include_keywords
            if keyword_matches_item(token, ctx.payload, text_haystack, tech_haystack)
        ]
        if not matched_include:
            checks.append(_check_entry("include_keywords", False, expected=list(compiled.include_keywords), matched=[], reason="include_keyword_mismatch"))
            return {"summary": "include_keyword_mismatch", "checks": checks, "compiled_subscription": compiled}
        checks.append(_check_entry("include_keywords", True, expected=list(compiled.include_keywords), matched=matched_include))
    else:
        checks.append(_check_entry("include_keywords", True, configured=False))

    if compiled.exclude_keywords:
        matched_exclude = [
            token
            for token in compiled.exclude_keywords
            if keyword_matches_item(token, ctx.payload, text_haystack, tech_haystack)
        ]
        if matched_exclude:
            reason = f"exclude_keyword:{'|'.join(matched_exclude)}"
            checks.append(_check_entry("exclude_keywords", False, blocked=list(compiled.exclude_keywords), matched=matched_exclude, reason=reason))
            return {"summary": reason, "checks": checks, "compiled_subscription": compiled}
        checks.append(_check_entry("exclude_keywords", True, blocked=list(compiled.exclude_keywords), matched=[]))
    else:
        checks.append(_check_entry("exclude_keywords", True, configured=False))

    return {"summary": "passed", "checks": checks, "compiled_subscription": compiled}


def explain_subscription_match_details(db: Any, sub: Any, item: Any) -> Dict[str, Any]:
    details = _evaluate_subscription_match(db, sub, item)
    compiled = details.get("compiled_subscription")
    payload = compiled.to_dict() if isinstance(compiled, CompiledSubscription) else _sub_payload(sub)
    details["compiled_subscription_snapshot"] = {
        "id": int(payload.get("id") or 0) if payload else 0,
        "tg_user_id": int(payload.get("tg_user_id") or 0) if payload else 0,
        "preset_key": compact_spaces(str(payload.get("preset_key") or "")),
        "media_type": compiled.media_type if compiled else compact_spaces(str(payload.get("media_type") or "any")),
        "year_from": compiled.year_from if compiled else payload.get("year_from"),
        "year_to": compiled.year_to if compiled else payload.get("year_to"),
        "allow_formats": list(compiled.allow_formats) if compiled else [],
        "min_rating": compiled.min_rating if compiled else payload.get("min_tmdb_rating"),
        "genre_ids": list(compiled.genre_ids) if compiled else list(payload.get("genre_ids") or []),
        "content_filter": compiled.content_filter if compiled else compact_spaces(str(payload.get("content_filter") or "any")),
        "country_codes": list(compiled.country_codes) if compiled else parse_country_codes(payload.get("country_codes") or payload.get("country_codes_list")),
        "exclude_country_codes": list(compiled.exclude_country_codes) if compiled else parse_country_codes(payload.get("exclude_country_codes") or payload.get("exclude_country_codes_list")),
        "include_keywords": list(compiled.include_keywords) if compiled else list(split_keyword_tokens(payload.get("include_keywords"))),
        "exclude_keywords": list(compiled.exclude_keywords) if compiled else list(split_keyword_tokens(payload.get("exclude_keywords"))),
    }
    return details


def explain_subscription_match(db: Any, sub: Any, item: Any) -> str:
    return str(explain_subscription_match_details(db, sub, item).get("summary") or "missing_sub_or_item")


def match_subscription(db: Any, sub: Any, item: Any) -> bool:
    return explain_subscription_match(db, sub, item) == "passed"
