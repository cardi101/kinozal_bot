import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from parsing_basic import parse_year, parse_years
from utils import compact_spaces


def item_source_years(item: Dict[str, Any]) -> List[int]:
    source_text = compact_spaces(f"{item.get('source_title') or ''} {item.get('source_description') or ''}")
    return parse_years(source_text)


def min_year_delta(years: Sequence[int], target_year: Optional[int]) -> Optional[int]:
    if target_year is None:
        return None
    normalized = [int(year) for year in years if str(year).isdigit()]
    if not normalized:
        return None
    return min(abs(int(year) - int(target_year)) for year in normalized)


def extract_expected_tv_totals(item: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    texts = [
        compact_spaces(str(item.get("source_episode_progress") or "")),
        compact_spaces(str(item.get("episode_progress") or "")),
        compact_spaces(str(item.get("source_series_update") or "")),
        compact_spaces(str(item.get("source_title") or "")),
        compact_spaces(str(item.get("source_description") or "")),
    ]
    haystack = "\n".join(text for text in texts if text)
    if not haystack:
        return None, None

    total_seasons: Optional[int] = None
    total_episodes: Optional[int] = None

    season_patterns = [
        r"\b\d+\s*[-–—]\s*(\d+)\s*сезон(?:а|ов|ы)?\b",
        r"\b\d+\s*[-–—]\s*(\d+)\s*season(?:s)?\b",
        r"\b(\d+)\s*сезон(?:а|ов|ы)?\s*:",
        r"\b(\d+)\s*season(?:s)?\s*:",
        r"\b(\d+)\s*сезон(?:а|ов|ы)?\b",
        r"\b(\d+)\s*season(?:s)?\b",
    ]
    for pattern in season_patterns:
        match = re.search(pattern, haystack, flags=re.I)
        if match:
            try:
                value = int(match.group(1))
                if value > 0:
                    total_seasons = value
                    break
            except Exception:
                pass

    episode_patterns = [
        r"\bиз\s*(\d{1,4})\b",
        r"\btotal\s*episodes?\s*[:\-]?\s*(\d{1,4})\b",
        r"\bэпизодов\s*[:\-]?\s*(\d{1,4})\b",
        r"\bсер(?:ия|ии|ий)\s*[:\-]?\s*(\d{1,4})\b",
        r"\bepisodes?\s*[:\-]?\s*(\d{1,4})\b",
    ]
    for pattern in episode_patterns:
        match = re.search(pattern, haystack, flags=re.I)
        if match:
            try:
                value = int(match.group(1))
                if value > 0:
                    total_episodes = value
                    break
            except Exception:
                pass

    return total_seasons, total_episodes


def extract_tv_season_hint(item: Dict[str, Any]) -> Optional[int]:
    texts = [
        compact_spaces(str(item.get("source_episode_progress") or "")),
        compact_spaces(str(item.get("episode_progress") or "")),
        compact_spaces(str(item.get("source_series_update") or "")),
        compact_spaces(str(item.get("source_title") or "")),
        compact_spaces(str(item.get("source_description") or "")),
    ]
    haystack = "\n".join(text for text in texts if text)
    if not haystack:
        return None

    patterns = [
        r"\b\d+\s*[-–—]\s*(\d+)\s*сезон(?:а|ов|ы)?\b",
        r"\b\d+\s*[-–—]\s*(\d+)\s*season(?:s)?\b",
        r"\b(\d+)\s*сезон(?:а|ов|ы)?\s*:",
        r"\b(\d+)\s*season(?:s)?\s*:",
        r"\b(\d+)\s*сезон(?:а|ов|ы)?\b",
        r"\b(\d+)\s*season(?:s)?\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, haystack, flags=re.I)
        if match:
            try:
                value = int(match.group(1))
                if value > 0:
                    return value
            except Exception:
                pass
    return None


def item_filter_years(item: Dict[str, Any]) -> List[int]:
    media_type = item.get("media_type") or "movie"
    tmdb_year = parse_year(item.get("tmdb_release_date") or "")
    source_years = item_source_years(item)

    years: List[int] = []
    if media_type == "tv":
        for year in source_years:
            if year not in years:
                years.append(year)
        if tmdb_year is not None and tmdb_year not in years:
            years.append(tmdb_year)
    else:
        if tmdb_year is not None:
            years.append(tmdb_year)
        for year in source_years:
            if year not in years:
                years.append(year)
    return years


def item_display_year(item: Dict[str, Any]) -> Optional[str]:
    media_type = item.get("media_type") or "movie"
    source_years = item_source_years(item)
    tmdb_year = parse_year(item.get("tmdb_release_date") or "")

    display_years = source_years if media_type == "tv" and source_years else ([tmdb_year] if tmdb_year else source_years)
    if not display_years:
        return None
    if len(display_years) >= 2 and display_years[0] != display_years[-1]:
        return f"{display_years[0]}-{display_years[-1]}"
    return str(display_years[0])
