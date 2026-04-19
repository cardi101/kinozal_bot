import re
import logging
from typing import Any, Dict, List, Optional

from content_buckets import anime_fallback_signal_score, item_content_bucket
from country_helpers import has_asian_script, normalize_tmdb_language
from item_years import (
    item_source_years,
    min_year_delta,
    extract_expected_tv_totals,
    extract_tv_season_hint,
)
from match_text import (
    similarity,
    is_generic_cyrillic_title,
    normalize_match_text,
    text_tokens,
    raw_text_tokens,
    token_overlap_ratio,
)
from parsing_basic import parse_year
from title_prep import clean_release_title, extract_title_aliases_from_text
from tmdb_aliases import is_short_or_common_tmdb_query, anime_alias_candidates_from_text
from utils import compact_spaces


log = logging.getLogger(__name__)


TMDB_VALIDATION_REASON_CODE_MAP = {
    "tmdb_match_looks_valid:L208": "SOURCE_TV_CANDIDATE_MOVIE",
    "tmdb_match_looks_valid:L290": "SHORT_COMMON_QUERY_TOKEN_MISMATCH",
    "tmdb_match_looks_valid:L292": "SHORT_COMMON_QUERY_LOW_SIMILARITY",
    "tmdb_match_looks_valid:truncated_prefix_query": "TRUNCATED_PREFIX_QUERY_MATCH",
    "tmdb_match_looks_valid:derivative_suffix": "DERIVATIVE_SUFFIX_MISMATCH",
    "tmdb_match_looks_valid:L297": "LATIN_TITLE_WEAK_MATCH_TV",
    "tmdb_match_looks_valid:L300": "LATIN_TITLE_WEAK_MATCH",
    "tmdb_match_looks_valid:L314": "ALIAS_ONLY_MEDIA_MISMATCH",
    "tmdb_match_looks_valid:L377": "ALIAS_ONLY_WEAK_MAIN_TITLE",
    "tmdb_match_looks_valid:L380": "ALIAS_ONLY_NONFICTION_WEAK_MATCH",
    "tmdb_match_looks_valid:L383": "ALIAS_ONLY_GENERIC_PARENTHETICAL",
    "tmdb_match_looks_valid:L419": "SOURCE_TV_MOVIE_YEAR_MISMATCH",
    "tmdb_match_looks_valid:L441": "TV_YEAR_DELTA_EXTREME",
    "tmdb_match_looks_valid:L443": "TV_YEAR_DELTA_VERY_LARGE",
    "tmdb_match_looks_valid:L445": "TV_YEAR_DELTA_LARGE_WEAK_TITLE",
    "tmdb_match_looks_valid:L447": "TV_YEAR_DELTA_MEDIUM_WEAK_TITLE",
    "tmdb_match_looks_valid:L450": "TV_SHORT_COMMON_YEAR_DELTA_LARGE",
    "tmdb_match_looks_valid:L452": "TV_SHORT_COMMON_YEAR_DELTA_EXACTNESS",
    "tmdb_match_looks_valid:L454": "TV_LATER_SEASON_COMMON_YEAR_DELTA",
    "tmdb_match_looks_valid:L458": "TV_SEASON_COUNT_TOO_LOW",
    "tmdb_match_looks_valid:L460": "TV_SEASON_COUNT_FAR_TOO_LOW_COMMON_QUERY",
    "tmdb_match_looks_valid:L465": "TV_EPISODE_TOTAL_MISMATCH",
    "tmdb_match_looks_valid:L467": "TV_EPISODE_TOTAL_MISMATCH_COMMON_QUERY",
    "tmdb_match_looks_valid:L472": "MOVIE_GENERIC_YEAR_DELTA_LARGE",
    "tmdb_match_looks_valid:L474": "MOVIE_GENERIC_YEAR_DELTA_WEAK_TITLE",
    "tmdb_match_looks_valid:L476": "MOVIE_YEAR_DELTA_LARGE_WEAK_TITLE",
    "tmdb_match_looks_valid:L478": "MOVIE_YEAR_DELTA_EXTREME_NO_EXACT",
    "tmdb_match_looks_valid:L480": "MOVIE_YEAR_DELTA_WEAK_SUBSTRING",
    "tmdb_match_looks_valid:L482": "MOVIE_YEAR_DELTA_ABSURD",
    "tmdb_match_looks_valid:L486": "TV_GENERIC_YEAR_DELTA_LARGE",
    "tmdb_match_looks_valid:L488": "TV_GENERIC_YEAR_DELTA_WEAK_TITLE",
}


def tmdb_validation_reason_code(reason: str) -> str:
    normalized = compact_spaces(reason)
    if not normalized:
        return "VALIDATION_REJECT"
    return TMDB_VALIDATION_REASON_CODE_MAP.get(normalized, "VALIDATION_REJECT")


def is_anime_franchise_parent_fallback(item: Dict[str, Any], query: str, details: Dict[str, Any]) -> bool:
    source_is_tv = bool(item.get("source_episode_progress")) or str(item.get("media_type") or "") == "tv"
    if not source_is_tv:
        return False
    if str(details.get("media_type") or "") != "tv":
        return False
    if item_content_bucket(item) != "anime" and anime_fallback_signal_score(item) < 2:
        return False

    query_clean = compact_spaces(clean_release_title(query or ""))
    query_norm = normalize_match_text(query_clean)
    if not query_clean or not query_norm or not is_short_or_common_tmdb_query(query_clean):
        return False

    alias_norms: List[str] = []
    for alias in anime_alias_candidates_from_text(query_clean):
        alias_norm = normalize_match_text(alias)
        if not alias_norm or alias_norm == query_norm:
            continue
        if alias_norm not in alias_norms:
            alias_norms.append(alias_norm)
    if not alias_norms:
        return False

    query_tokens = set(text_tokens(query_clean))
    if not query_tokens or len(query_tokens) > 2:
        return False

    source_variants = [
        item.get("source_title") or "",
        item.get("cleaned_title") or "",
    ]
    source_has_specific_subtitle = False
    for source_variant in source_variants:
        source_tokens = set(text_tokens(source_variant))
        if query_tokens <= source_tokens and len(source_tokens - query_tokens) >= 1:
            source_has_specific_subtitle = True
            break
    if not source_has_specific_subtitle:
        return False

    detail_variants = [
        details.get("tmdb_title") or "",
        details.get("tmdb_original_title") or "",
        details.get("search_match_title") or "",
        details.get("search_match_original_title") or "",
    ]
    detail_norms = [normalize_match_text(value) for value in detail_variants if compact_spaces(value)]
    if not detail_norms:
        return False

    for alias_norm in alias_norms:
        alias_token = f" {alias_norm} "
        for detail_norm in detail_norms:
            if alias_norm == detail_norm:
                return True
            if detail_norm.startswith(alias_norm + " "):
                return True
            if alias_token in f" {detail_norm} ":
                return True
    return False


def is_tv_continuation_parent_match(
    item: Dict[str, Any],
    details: Dict[str, Any],
    has_exact_normalized: bool,
    best_main_overlap: float,
    best_main_similarity: float,
    has_substring: bool = False,
) -> bool:
    strong_title_match = has_exact_normalized or best_main_overlap >= 0.72 or best_main_similarity >= 0.92 or has_substring
    season_hint = extract_tv_season_hint(item)
    if not season_hint or season_hint < 2:
        return False
    expected_seasons, _ = extract_expected_tv_totals(item)
    reference_season = expected_seasons or season_hint
    if not reference_season or reference_season < 2:
        return False
    source_has_original_latin = bool(re.search(r"/\s*[A-Za-z]", str(item.get("source_title") or "")))
    active_tv_status = str(details.get("tmdb_status") or "").strip().lower() in {
        "returning series",
        "in production",
        "planned",
        "pilot",
    }
    anime_parent_continuation = (
        (item_content_bucket(item) == "anime" or anime_fallback_signal_score(item) >= 2)
        and reference_season >= 2
        and (best_main_overlap >= 0.62 or best_main_similarity >= 0.64)
    )
    try:
        tmdb_seasons_int = int(details.get("tmdb_number_of_seasons")) if details.get("tmdb_number_of_seasons") is not None else None
    except Exception:
        tmdb_seasons_int = None
    try:
        tmdb_episodes_int = int(details.get("tmdb_number_of_episodes")) if details.get("tmdb_number_of_episodes") is not None else None
    except Exception:
        tmdb_episodes_int = None
    if tmdb_seasons_int is None:
        return (
            active_tv_status
            and reference_season >= 2
            and (anime_parent_continuation or source_has_original_latin)
            and (best_main_overlap >= 0.62 or best_main_similarity >= 0.64 or has_substring)
        )
    if tmdb_seasons_int == 1:
        long_running_parent_anime = (
            active_tv_status
            and (item_content_bucket(item) == "anime" or anime_fallback_signal_score(item) >= 2 or source_has_original_latin)
            and tmdb_episodes_int is not None
            and tmdb_episodes_int >= 60
            and (best_main_overlap >= 0.62 or best_main_similarity >= 0.64 or has_substring)
        )
        if long_running_parent_anime:
            return True
    if tmdb_seasons_int < 2:
        return False
    if not strong_title_match:
        anime_parent_continuation = anime_parent_continuation and tmdb_seasons_int + 1 >= reference_season
        if not anime_parent_continuation:
            return False
    return tmdb_seasons_int + 1 >= reference_season


def is_tv_revival_reset_match(
    item: Dict[str, Any],
    details: Dict[str, Any],
    has_exact_normalized: bool,
    year_delta: Optional[int],
    best_overlap: float,
    best_similarity_norm: float,
) -> bool:
    if not has_exact_normalized:
        return False
    if year_delta is None or year_delta > 1:
        return False
    season_hint = extract_tv_season_hint(item)
    expected_seasons, expected_episodes = extract_expected_tv_totals(item)
    reference_season = expected_seasons or season_hint
    if not reference_season or reference_season < 2:
        return False
    try:
        tmdb_seasons_int = int(details.get("tmdb_number_of_seasons")) if details.get("tmdb_number_of_seasons") is not None else None
    except Exception:
        tmdb_seasons_int = None
    try:
        tmdb_episodes_int = int(details.get("tmdb_number_of_episodes")) if details.get("tmdb_number_of_episodes") is not None else None
    except Exception:
        tmdb_episodes_int = None
    if tmdb_seasons_int != 1:
        return False
    if expected_episodes and tmdb_episodes_int:
        if abs(tmdb_episodes_int - expected_episodes) > max(2, int(expected_episodes * 0.35)):
            return False
    if best_overlap < 0.72 and best_similarity_norm < 0.92:
        return False
    return True


def tmdb_match_looks_valid(item: Dict[str, Any], query: str, details: Dict[str, Any], requested_media_type: str) -> bool:
    source_is_tv = bool(item.get("source_episode_progress")) or str(item.get("media_type") or "") == "tv"
    details_media = str(details.get("media_type") or requested_media_type or "")
    details.pop("tmdb_validation_reject_reason", None)
    details.pop("tmdb_validation_reject_code", None)
    details.pop("tmdb_validation_warnings", None)
    anime_franchise_fallback = False
    short_or_common_query = False
    query_norm = ""
    alias_only_query = False
    best_overlap = 0.0
    best_similarity_norm = 0.0
    best_common_tokens = 0
    has_substring = False
    has_exact_normalized = False
    best_main_overlap = 0.0
    best_main_similarity = 0.0
    year_delta = None
    season_hint = extract_tv_season_hint(item)
    expected_seasons = None
    expected_episodes = None
    tmdb_seasons_int = None
    tmdb_episodes_int = None
    tv_continuation_parent_match = False
    tv_revival_reset_match = False
    validation_warnings: List[str] = []

    def reject(reason: str) -> bool:
        code = tmdb_validation_reason_code(reason)
        details["tmdb_validation_reject_reason"] = reason
        details["tmdb_validation_reject_code"] = code
        try:
            log.debug(
                "TMDB validation reject: code=%s reason=%s | query=%s | source=%s | tmdb=%s | media=%s/%s | alias_only=%s | exact=%s | substring=%s | best_overlap=%.3f | best_similarity=%.3f | best_main_overlap=%.3f | best_main_similarity=%.3f | common_tokens=%s | year_delta=%s | season_hint=%s | expected_seasons=%s | expected_episodes=%s | tmdb_seasons=%s | tmdb_episodes=%s | anime_fallback=%s | continuation=%s | revival=%s",
                code,
                reason,
                query,
                item.get("source_title") or item.get("cleaned_title") or "",
                details.get("tmdb_title") or details.get("tmdb_original_title") or details.get("search_match_title") or details.get("search_match_original_title") or "",
                item.get("media_type") or "",
                details_media,
                alias_only_query,
                has_exact_normalized,
                has_substring,
                float(best_overlap or 0.0),
                float(best_similarity_norm or 0.0),
                float(best_main_overlap or 0.0),
                float(best_main_similarity or 0.0),
                best_common_tokens,
                year_delta,
                season_hint,
                expected_seasons,
                expected_episodes,
                tmdb_seasons_int,
                tmdb_episodes_int,
                anime_franchise_fallback,
                tv_continuation_parent_match,
                tv_revival_reset_match,
            )
        except Exception:
            pass
        return False

    def warn(reason: str) -> None:
        if reason not in validation_warnings:
            validation_warnings.append(reason)
        details["tmdb_validation_warnings"] = list(validation_warnings)
    if source_is_tv and details_media == "movie":
        return reject("tmdb_match_looks_valid:L208")

    anime_franchise_fallback = is_anime_franchise_parent_fallback(item, query, details)

    short_or_common_query = any(
        is_short_or_common_tmdb_query(value or "")
        for value in [
            query or "",
            item.get("cleaned_title") or "",
            item.get("source_title") or "",
        ]
        if compact_spaces(value or "")
    )

    detail_variants = [
        details.get("tmdb_title") or "",
        details.get("tmdb_original_title") or "",
        details.get("search_match_title") or "",
        details.get("search_match_original_title") or "",
    ]
    query_variants = [
        query or "",
        clean_release_title(query or ""),
        item.get("cleaned_title") or "",
        clean_release_title(item.get("source_title") or ""),
    ]

    source_aliases = extract_title_aliases_from_text(item.get("source_title") or "") + extract_title_aliases_from_text(item.get("cleaned_title") or "")
    alias_norms = {normalize_match_text(alias) for alias in source_aliases if compact_spaces(alias)}
    main_title_variants = [
        clean_release_title(item.get("cleaned_title") or ""),
        clean_release_title(item.get("source_title") or ""),
    ]
    main_norms = {normalize_match_text(value) for value in main_title_variants if compact_spaces(value)}
    query_norm = normalize_match_text(clean_release_title(query or ""))
    alias_only_query = bool(query_norm and query_norm in alias_norms and query_norm not in main_norms)

    best_overlap = 0.0
    best_similarity_norm = 0.0
    best_common_tokens = 0
    has_substring = False
    has_exact_normalized = False
    for q in query_variants:
        low_q = compact_spaces(q).lower()
        norm_q = normalize_match_text(q)
        if not low_q and not norm_q:
            continue
        for d in detail_variants:
            low_d = compact_spaces(d).lower()
            norm_d = normalize_match_text(d)
            if not low_d and not norm_d:
                continue
            best_overlap = max(best_overlap, token_overlap_ratio(q, d))
            if norm_q and norm_d:
                best_similarity_norm = max(best_similarity_norm, similarity(norm_q, norm_d))
                best_common_tokens = max(best_common_tokens, len(set(text_tokens(norm_q)) & set(text_tokens(norm_d))))
                if norm_q == norm_d:
                    has_exact_normalized = True
                if norm_q in norm_d or norm_d in norm_q:
                    has_substring = True
            if low_q and low_d and (low_q in low_d or low_d in low_q):
                has_substring = True

    best_main_overlap = 0.0
    best_main_similarity = 0.0
    for q in main_title_variants:
        if not compact_spaces(q):
            continue
        for d in detail_variants:
            if not compact_spaces(d):
                continue
            best_main_overlap = max(best_main_overlap, token_overlap_ratio(q, d))
            norm_q = normalize_match_text(q)
            norm_d = normalize_match_text(d)
            if norm_q and norm_d:
                best_main_similarity = max(best_main_similarity, similarity(norm_q, norm_d))

    query_raw_token_sizes = [len(set(raw_text_tokens(q))) for q in query_variants if compact_spaces(q)]
    detail_raw_token_sizes = [len(set(raw_text_tokens(d))) for d in detail_variants if compact_spaces(d)]
    if short_or_common_query and query_raw_token_sizes and min(query_raw_token_sizes) == 1 and not has_exact_normalized:
        if detail_raw_token_sizes and all(size > 1 for size in detail_raw_token_sizes):
            if best_common_tokens <= 1:
                return reject("tmdb_match_looks_valid:L290")
            if best_similarity_norm < 0.985 and not has_substring:
                return reject("tmdb_match_looks_valid:L292")

    query_clean = compact_spaces(clean_release_title(query or "") or query or "")
    query_clean_norm = normalize_match_text(query_clean)
    query_clean_tokens = text_tokens(query_clean)
    detail_norms = [normalize_match_text(value) for value in detail_variants if compact_spaces(value)]
    detail_exact_query = bool(query_clean_norm and any(value == query_clean_norm for value in detail_norms))
    detail_extends_query = bool(query_clean_norm and any(value.startswith(query_clean_norm + " ") for value in detail_norms))
    main_title_parts: List[str] = []
    for main_value in [
        item.get("cleaned_title") or "",
        clean_release_title(item.get("source_title") or ""),
    ]:
        for part in [compact_spaces(value) for value in str(main_value).split(" / ") if compact_spaces(value)]:
            part_norm = normalize_match_text(part)
            if part_norm and all(part_norm != normalize_match_text(existing) for existing in main_title_parts):
                main_title_parts.append(part)

    if (
        short_or_common_query
        and has_exact_normalized
        and detail_exact_query
        and not detail_extends_query
        and len(query_clean_tokens) >= 2
    ):
        for part in main_title_parts:
            part_norm = normalize_match_text(part)
            part_tokens = text_tokens(part)
            if not part_norm or part_norm == query_clean_norm:
                continue
            if len(part_tokens) < len(query_clean_tokens) + 2:
                continue
            if part_tokens[: len(query_clean_tokens)] != query_clean_tokens:
                continue
            if best_main_overlap < 0.67 and best_main_similarity < 0.88:
                return reject("tmdb_match_looks_valid:truncated_prefix_query")

    _DERIVATIVE_SUFFIXES = {
        "podcast", "podcasts", "compilation", "compilations",
        "soundtrack", "soundtracks", "ost",
        "behind the scenes", "making of",
        "aftershow", "after show", "talk show", "talkshow",
        "recap", "recaps", "rewind",
        "stories", "diaries", "diary",
    }
    if not has_exact_normalized and has_substring:
        for d in detail_variants:
            d_low = compact_spaces(d).lower()
            if not d_low:
                continue
            for q in query_variants:
                q_low = compact_spaces(q).lower()
                if not q_low or q_low not in d_low:
                    continue
                remainder = compact_spaces(d_low.replace(q_low, "", 1)).strip(" :-–—")
                if remainder and any(remainder == s or remainder.endswith(" " + s) for s in _DERIVATIVE_SUFFIXES):
                    return reject("tmdb_match_looks_valid:derivative_suffix")

    def is_cjk_romanized_alias_search_hit() -> bool:
        if has_exact_normalized or has_substring:
            return False
        query_clean = compact_spaces(clean_release_title(query or "") or query or "")
        query_clean_norm = normalize_match_text(query_clean)
        if not query_clean_norm:
            return False
        source_title_parts = {
            normalize_match_text(part)
            for value in [
                item.get("cleaned_title") or "",
                clean_release_title(item.get("source_title") or ""),
            ]
            for part in [compact_spaces(chunk) for chunk in str(value).split(" / ") if compact_spaces(chunk)]
            if normalize_match_text(part)
        }
        if query_clean_norm not in source_title_parts and query_clean_norm not in alias_norms:
            return False
        if normalize_tmdb_language(details.get("tmdb_original_language")) not in {"zh", "ja", "ko"}:
            return False
        if not any(has_asian_script(value) for value in detail_variants if compact_spaces(value)):
            return False
        latin_tokens = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", query_clean)
        if len(latin_tokens) < 3:
            return False
        try:
            search_rank = int(details.get("search_rank")) if details.get("search_rank") is not None else None
        except Exception:
            search_rank = None
        if search_rank not in (None, 0):
            return False
        details_year = parse_year(str(details.get("tmdb_release_date") or ""))
        title_year_delta = min_year_delta(item_source_years(item), details_year)
        if title_year_delta is None or title_year_delta > 1:
            return False
        expected_seasons_local, expected_episodes_local = extract_expected_tv_totals(item)
        try:
            tmdb_seasons_local = int(details.get("tmdb_number_of_seasons")) if details.get("tmdb_number_of_seasons") is not None else None
        except Exception:
            tmdb_seasons_local = None
        try:
            tmdb_episodes_local = int(details.get("tmdb_number_of_episodes")) if details.get("tmdb_number_of_episodes") is not None else None
        except Exception:
            tmdb_episodes_local = None
        seasons_align = bool(
            expected_seasons_local
            and tmdb_seasons_local
            and abs(tmdb_seasons_local - expected_seasons_local) <= 1
        )
        episodes_align = bool(
            expected_episodes_local
            and tmdb_episodes_local
            and expected_episodes_local == tmdb_episodes_local
        )
        return seasons_align or episodes_align

    if re.search(r"[A-Za-z]", query or ""):
        if source_is_tv and details_media == "tv":
            if (
                best_overlap < 0.18
                and best_similarity_norm < 0.50
                and best_common_tokens < 2
                and not has_substring
                and not is_cjk_romanized_alias_search_hit()
            ):
                return reject("tmdb_match_looks_valid:L297")
        else:
            if best_overlap < 0.28 and best_similarity_norm < 0.58 and best_common_tokens < 2 and not has_substring:
                return reject("tmdb_match_looks_valid:L300")

    if alias_only_query:
        source_category_name = compact_spaces(str(item.get("source_category_name") or "")).lower()
        expanded_parenthetical_alias = False
        query_tokens = set(text_tokens(query or ""))
        if query_tokens:
            for alias in source_aliases:
                alias_tokens = set(text_tokens(alias))
                if alias_tokens and query_tokens < alias_tokens:
                    expanded_parenthetical_alias = True
                    break

        if requested_media_type in ("movie", "tv") and details_media != requested_media_type:
            return reject("tmdb_match_looks_valid:L314")

        alias_exact_to_details = any(
            query_norm == normalize_match_text(value or "")
            for value in detail_variants
            if compact_spaces(value or "")
        )

        alias_source_years = item_source_years(item)
        alias_details_year = parse_year(str(details.get("tmdb_release_date") or ""))
        alias_year_ok = (
            not alias_source_years
            or alias_details_year is None
            or min(abs(alias_details_year - year) for year in alias_source_years) <= 1
        )

        alias_expected_seasons, alias_expected_episodes = extract_expected_tv_totals(item)

        try:
            alias_tmdb_seasons = int(details.get("tmdb_number_of_seasons")) if details.get("tmdb_number_of_seasons") is not None else None
        except Exception:
            alias_tmdb_seasons = None

        try:
            alias_tmdb_episodes = int(details.get("tmdb_number_of_episodes")) if details.get("tmdb_number_of_episodes") is not None else None
        except Exception:
            alias_tmdb_episodes = None

        alias_tv_exact_totals_ok = (
            (
                alias_expected_seasons is None
                or alias_tmdb_seasons is None
                or alias_expected_seasons == alias_tmdb_seasons
            )
            and (
                alias_expected_episodes is None
                or alias_tmdb_episodes is None
                or abs(alias_tmdb_episodes - alias_expected_episodes) <= max(2, int(alias_expected_episodes * 0.20))
            )
        )

        alias_tv_parent_totals_ok = (
            (
                alias_expected_seasons is None
                or alias_tmdb_seasons is None
                or alias_tmdb_seasons >= alias_expected_seasons
            )
            and (
                alias_expected_episodes is None
                or alias_tmdb_episodes is None
                or alias_tmdb_episodes >= alias_expected_episodes
            )
        )

        alias_tv_semantic_ok = (
            source_is_tv
            and details_media == "tv"
            and alias_exact_to_details
            and alias_year_ok
            and (alias_tv_exact_totals_ok or alias_tv_parent_totals_ok)
        )

        if not alias_tv_semantic_ok and best_main_overlap < 0.34 and best_main_similarity < 0.58:
            return reject("tmdb_match_looks_valid:L377")

        if any(marker in source_category_name for marker in ("документ", "спорт", "передачи", "тв-шоу")) and best_main_overlap < 0.60:
            return reject("tmdb_match_looks_valid:L380")

        if expanded_parenthetical_alias and is_generic_cyrillic_title(query or "") and len(text_tokens(query or "")) <= 1:
            return reject("tmdb_match_looks_valid:L383")

    source_years = item_source_years(item)
    details_year = parse_year(str(details.get("tmdb_release_date") or ""))
    year_delta = min_year_delta(source_years, details_year)
    source_has_original_latin = bool(re.search(r"/\s*[A-Za-z]", str(item.get("source_title") or "")))
    generic_cyrillic_title_risk = (
        short_or_common_query
        and not source_has_original_latin
        and any(
            is_generic_cyrillic_title(value or "")
            for value in [
                query or "",
                item.get("cleaned_title") or "",
                item.get("source_title") or "",
            ]
        )
    )

    expected_seasons, expected_episodes = extract_expected_tv_totals(item)
    tmdb_seasons = details.get("tmdb_number_of_seasons")
    tmdb_episodes = details.get("tmdb_number_of_episodes")
    try:
        tmdb_seasons_int = int(tmdb_seasons) if tmdb_seasons is not None else None
    except Exception:
        tmdb_seasons_int = None
    try:
        tmdb_episodes_int = int(tmdb_episodes) if tmdb_episodes is not None else None
    except Exception:
        tmdb_episodes_int = None

    tv_continuation_parent_match = False
    tv_revival_reset_match = False

    if source_is_tv and details_media == "movie" and source_years and details_year:
        if year_delta is not None and year_delta >= 2:
            return reject("tmdb_match_looks_valid:L419")
    if source_is_tv and details_media == "tv":
        season_hint = extract_tv_season_hint(item)
        later_season_release = bool(season_hint and season_hint >= 2)
        tv_continuation_parent_match = is_tv_continuation_parent_match(
            item,
            details,
            has_exact_normalized,
            best_main_overlap,
            best_main_similarity,
            has_substring=has_substring,
        )
        tv_revival_reset_match = is_tv_revival_reset_match(
            item,
            details,
            has_exact_normalized,
            year_delta,
            best_overlap,
            best_similarity_norm,
        )

        if year_delta is not None:
            if year_delta >= 35:
                return reject("tmdb_match_looks_valid:L441")
            if year_delta >= 20 and not tv_continuation_parent_match:
                return reject("tmdb_match_looks_valid:L443")
            if year_delta >= 10 and not has_exact_normalized and not anime_franchise_fallback and not tv_continuation_parent_match:
                return reject("tmdb_match_looks_valid:L445")
            if year_delta >= 8 and best_overlap < 0.70 and best_similarity_norm < 0.84 and not anime_franchise_fallback and not tv_continuation_parent_match:
                return reject("tmdb_match_looks_valid:L447")
            if not later_season_release:
                if short_or_common_query and year_delta >= 3 and not anime_franchise_fallback:
                    return reject("tmdb_match_looks_valid:L450")
                if short_or_common_query and year_delta >= 2 and not has_exact_normalized and not anime_franchise_fallback:
                    return reject("tmdb_match_looks_valid:L452")
            elif short_or_common_query and year_delta >= 6 and not has_exact_normalized and best_overlap < 0.85 and best_similarity_norm < 0.90 and not anime_franchise_fallback and not tv_continuation_parent_match:
                return reject("tmdb_match_looks_valid:L454")

        if expected_seasons and tmdb_seasons_int:
            long_running_parent_anime_ok = (
                source_is_tv
                and details_media == "tv"
                and (
                    item_content_bucket(item) == "anime"
                    or anime_fallback_signal_score(item) >= 2
                    or source_has_original_latin
                )
                and tmdb_seasons_int == 1
                and tmdb_episodes_int is not None
                and expected_episodes is not None
                and tmdb_episodes_int >= max(expected_episodes * 3, 60)
                and (
                    has_exact_normalized
                    or has_substring
                    or best_main_overlap >= 0.62
                    or best_main_similarity >= 0.64
                )
            )

            if (
                expected_seasons >= 2
                and tmdb_seasons_int + 1 < expected_seasons
                and not tv_revival_reset_match
                and not long_running_parent_anime_ok
            ):
                return reject("tmdb_match_looks_valid:L458")

            if (
                short_or_common_query
                and expected_seasons >= 3
                and tmdb_seasons_int + 2 < expected_seasons
                and not has_exact_normalized
                and not tv_revival_reset_match
                and not long_running_parent_anime_ok
            ):
                return reject("tmdb_match_looks_valid:L460")

        single_season_context = (expected_seasons in (None, 1)) and (tmdb_seasons_int in (None, 1))
        if single_season_context and expected_episodes and tmdb_episodes_int:
            episode_mismatch = abs(tmdb_episodes_int - expected_episodes)
            anime_or_on_air_case = (
                item_content_bucket(item) == "anime"
                or anime_fallback_signal_score(item) >= 2
                or str(details.get("tmdb_status") or "").lower() in {"returning series", "in production", "planned", "pilot"}
            )
            strongish_title_match = (
                has_exact_normalized
                or has_substring
                or best_overlap >= 0.45
                or best_similarity_norm >= 0.68
                or best_main_overlap >= 0.40
                or best_main_similarity >= 0.68
            )
            soft_episode_total_case = (
                source_is_tv
                and details_media == "tv"
                and strongish_title_match
                and (year_delta is None or year_delta <= 1)
                and anime_or_on_air_case
            )
            if expected_episodes >= 8 and episode_mismatch >= max(4, int(expected_episodes * 0.60)):
                if soft_episode_total_case:
                    warn("episode_total_mismatch_soft")
                else:
                    return reject("tmdb_match_looks_valid:L465")
            if short_or_common_query and expected_episodes >= 6 and abs(tmdb_episodes_int - expected_episodes) >= max(4, int(expected_episodes * 0.75)) and not has_exact_normalized:
                return reject("tmdb_match_looks_valid:L467")

    if not source_is_tv and details_media == "movie" and source_years and details_year:
        min_delta = year_delta if year_delta is not None else min(abs(details_year - year) for year in source_years)
        if generic_cyrillic_title_risk and min_delta >= 5:
            return reject("tmdb_match_looks_valid:L472")
        if generic_cyrillic_title_risk and min_delta >= 3 and best_overlap < 0.95 and best_similarity_norm < 0.98:
            return reject("tmdb_match_looks_valid:L474")
        if min_delta >= 6 and not has_exact_normalized and best_overlap < 0.72 and best_similarity_norm < 0.84:
            return reject("tmdb_match_looks_valid:L476")
        if min_delta >= 10 and not has_exact_normalized:
            return reject("tmdb_match_looks_valid:L478")
        if min_delta >= 8 and best_overlap < 0.60 and not has_substring:
            return reject("tmdb_match_looks_valid:L480")
        if min_delta >= 20:
            return reject("tmdb_match_looks_valid:L482")

    if source_is_tv and details_media == "tv" and generic_cyrillic_title_risk and year_delta is not None:
        if year_delta >= 6 and not tv_continuation_parent_match:
            return reject("tmdb_match_looks_valid:L486")
        if year_delta >= 4 and best_overlap < 0.90 and best_similarity_norm < 0.96 and not tv_continuation_parent_match:
            return reject("tmdb_match_looks_valid:L488")

    if validation_warnings:
        details["tmdb_validation_warnings"] = list(validation_warnings)
    return True
