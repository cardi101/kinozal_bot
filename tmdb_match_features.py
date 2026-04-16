from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from content_buckets import anime_fallback_signal_score, item_content_bucket
from item_years import extract_expected_tv_totals, extract_tv_season_hint, item_source_years, min_year_delta
from match_text import normalize_match_text, similarity, token_overlap_ratio
from parsing_basic import parse_year
from title_prep import clean_release_title, extract_title_aliases_from_text
from tmdb_aliases import is_short_or_common_tmdb_query
from utils import compact_spaces


@dataclass(slots=True)
class TMDBMatchFeatures:
    query_used: str
    requested_media_type: str
    candidate_media_type: str
    source_title: str
    source_cleaned_title: str
    candidate_title: str
    candidate_original_title: str
    title_similarity: float
    title_overlap: float
    exact_normalized: bool
    substring_match: bool
    year_delta: Optional[int]
    source_year: Optional[int]
    candidate_year: Optional[int]
    season_hint: Optional[int]
    expected_seasons: Optional[int]
    expected_episodes: Optional[int]
    tmdb_number_of_seasons: Optional[int]
    tmdb_number_of_episodes: Optional[int]
    bucket: str
    bucket_alignment: bool
    anime_hint: bool
    alias_only_query: bool
    short_or_common_query: bool
    search_score: float

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        for key in ("title_similarity", "title_overlap", "search_score"):
            payload[key] = round(float(payload.get(key) or 0.0), 3)
        return payload


def extract_tmdb_match_features(
    item: Dict[str, Any],
    query: str,
    details: Dict[str, Any],
    requested_media_type: str,
) -> TMDBMatchFeatures:
    source_title = compact_spaces(str(item.get("source_title") or ""))
    source_cleaned_title = compact_spaces(str(item.get("cleaned_title") or clean_release_title(source_title)))
    candidate_title = compact_spaces(str(details.get("tmdb_title") or details.get("search_match_title") or ""))
    candidate_original_title = compact_spaces(
        str(details.get("tmdb_original_title") or details.get("search_match_original_title") or "")
    )

    query_clean = compact_spaces(clean_release_title(query or "") or query or "")
    source_variants = [query_clean, source_cleaned_title, source_title]
    candidate_variants = [candidate_title, candidate_original_title]

    best_similarity = 0.0
    best_overlap = 0.0
    exact_normalized = False
    substring_match = False
    for left in source_variants:
        left_clean = compact_spaces(left)
        if not left_clean:
            continue
        left_norm = normalize_match_text(left_clean)
        for right in candidate_variants:
            right_clean = compact_spaces(right)
            if not right_clean:
                continue
            right_norm = normalize_match_text(right_clean)
            if left_norm and right_norm:
                best_similarity = max(best_similarity, similarity(left_norm, right_norm))
                if left_norm == right_norm:
                    exact_normalized = True
                if left_norm in right_norm or right_norm in left_norm:
                    substring_match = True
            best_overlap = max(best_overlap, token_overlap_ratio(left_clean, right_clean))

    source_years = item_source_years(item)
    candidate_year = parse_year(str(details.get("tmdb_release_date") or ""))
    year_delta = min_year_delta(source_years, candidate_year)
    source_year = min(source_years) if source_years else None
    expected_seasons, expected_episodes = extract_expected_tv_totals(item)
    season_hint = extract_tv_season_hint(item)
    candidate_media_type = compact_spaces(str(details.get("media_type") or requested_media_type or "movie")).lower() or "movie"
    bucket = item_content_bucket(item)
    anime_hint = bucket == "anime" or anime_fallback_signal_score(item) >= 2

    alias_norms = {
        normalize_match_text(alias)
        for alias in (extract_title_aliases_from_text(source_title) + extract_title_aliases_from_text(source_cleaned_title))
        if compact_spaces(alias)
    }
    query_norm = normalize_match_text(query_clean)
    main_norms = {
        normalize_match_text(value)
        for value in (source_cleaned_title, clean_release_title(source_title))
        if compact_spaces(value)
    }

    raw_tmdb_seasons = details.get("tmdb_number_of_seasons")
    try:
        tmdb_number_of_seasons = int(raw_tmdb_seasons) if raw_tmdb_seasons is not None else None
    except Exception:
        tmdb_number_of_seasons = None
    raw_tmdb_episodes = details.get("tmdb_number_of_episodes")
    try:
        tmdb_number_of_episodes = int(raw_tmdb_episodes) if raw_tmdb_episodes is not None else None
    except Exception:
        tmdb_number_of_episodes = None

    return TMDBMatchFeatures(
        query_used=compact_spaces(query or ""),
        requested_media_type=compact_spaces(str(requested_media_type or "")).lower() or "movie",
        candidate_media_type=candidate_media_type,
        source_title=source_title,
        source_cleaned_title=source_cleaned_title,
        candidate_title=candidate_title,
        candidate_original_title=candidate_original_title,
        title_similarity=best_similarity,
        title_overlap=best_overlap,
        exact_normalized=exact_normalized,
        substring_match=substring_match,
        year_delta=year_delta,
        source_year=source_year,
        candidate_year=candidate_year,
        season_hint=season_hint,
        expected_seasons=expected_seasons,
        expected_episodes=expected_episodes,
        tmdb_number_of_seasons=tmdb_number_of_seasons,
        tmdb_number_of_episodes=tmdb_number_of_episodes,
        bucket=bucket,
        bucket_alignment=(bucket != "anime" or candidate_media_type == "tv"),
        anime_hint=anime_hint,
        alias_only_query=bool(query_norm and query_norm in alias_norms and query_norm not in main_norms),
        short_or_common_query=is_short_or_common_tmdb_query(query_clean),
        search_score=float(details.get("search_score") or 0.0),
    )


def score_tmdb_match_candidate(features: TMDBMatchFeatures) -> float:
    score = float(features.search_score or 0.0)
    score += features.title_similarity * 0.90
    score += features.title_overlap * 0.70
    if features.exact_normalized:
        score += 0.55
    if features.substring_match:
        score += 0.18
    if features.requested_media_type == features.candidate_media_type:
        score += 0.24
    if features.bucket_alignment:
        score += 0.10
    if features.anime_hint and features.candidate_media_type == "tv":
        score += 0.08
    if features.year_delta is not None:
        score -= min(features.year_delta, 12) * 0.06
        if features.year_delta <= 1:
            score += 0.14
        elif features.year_delta >= 5:
            score -= 0.16
    if (
        features.season_hint
        and features.season_hint >= 2
        and features.tmdb_number_of_seasons
        and features.tmdb_number_of_seasons + 1 >= features.season_hint
        and features.candidate_media_type == "tv"
    ):
        score += 0.12
    if features.alias_only_query:
        score -= 0.10
    if features.short_or_common_query and not features.exact_normalized:
        score -= 0.12
    return round(score, 3)
