import re
from typing import Any, Dict, List, Optional, Tuple

from country_helpers import parse_jsonish_list, effective_item_countries, human_country_names
from utils import compact_spaces


TECH_KEYWORD_PATTERNS: Dict[str, List[str]] = {
    "hdr": [r"(?<!\w)hdr(?:10|10\+|\+)?(?!\w)", r"dolby\s*vision", r"(?<!\w)dv(?!\w)"],
    "dv": [r"dolby\s*vision", r"(?<!\w)dv(?!\w)"],
    "lossless": [r"lossless", r"(?<!\w)flac(?!\w)", r"(?<!\w)ape(?!\w)", r"(?<!\w)alac(?!\w)", r"truehd", r"dts[\s\-]*hd", r"pcm"],
    "mp3": [r"(?<!\w)mp3(?!\w)"],
    "ру": [r"(?<!\w)ру(?!\w)", r"(?<!\w)rus(?!\w)", r"\bрус(?:ский|ская|ские|ское)?\b", r"russian"],
    "укр": [r"(?<!\w)укр(?!\w)", r"(?<!\w)ua(?!\w)", r"\bукраин(?:ский|ская|ские|ское)?\b", r"ukrainian"],
    "2160": [r"(?<!\w)2160p?(?!\w)", r"(?<!\w)4k(?!\w)", r"(?<!\w)uhd(?!\w)"],
    "1080": [r"(?<!\w)1080p?(?!\w)"],
    "720": [r"(?<!\w)720p?(?!\w)"],
    "hevc": [r"(?<!\w)hevc(?!\w)", r"(?<!\w)x265(?!\w)", r"(?<!\w)h265(?!\w)"],
    "x265": [r"(?<!\w)x265(?!\w)", r"(?<!\w)h265(?!\w)", r"(?<!\w)hevc(?!\w)"],
    "web-dl": [r"web[\-\s]?dl"],
    "webrip": [r"web[\-\s]?rip"],
    "remux": [r"remux"],
}


def parse_rating(text: str) -> Optional[float]:
    if not text:
        return None
    patterns = [
        r"(?:rating|рейтинг|imdb|kinopoisk|кп)\D{0,8}(\d(?:[.,]\d)?)",
        r"\b(\d(?:[.,]\d)?)\s*/\s*10\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            try:
                value = float(match.group(1).replace(",", "."))
                if 0 <= value <= 10:
                    return value
            except Exception:
                pass
    return None


def normalize_keywords_input(text: str) -> Tuple[str, str]:
    include: List[str] = []
    exclude: List[str] = []
    for token in re.split(r"[\s,;]+", compact_spaces(text)):
        token = compact_spaces(token).strip()
        if not token:
            continue
        if token.startswith("+") and len(token) > 1:
            include.append(token[1:])
        elif token.startswith("-") and len(token) > 1:
            exclude.append(token[1:])
    return ",".join(include), ",".join(exclude)


def build_keyword_haystacks(item: Dict[str, Any]) -> Tuple[str, str]:
    title = compact_spaces(item.get("source_title") or "")
    desc = compact_spaces(item.get("source_description") or "")
    tmdb_title = compact_spaces(item.get("tmdb_title") or "")
    original = compact_spaces(item.get("tmdb_original_title") or "")
    overview = compact_spaces(item.get("tmdb_overview") or "")
    audio = " ".join(parse_jsonish_list(item.get("source_audio_tracks")))
    countries = " ".join(effective_item_countries(item))
    countries_human = " ".join(human_country_names(effective_item_countries(item)))
    source_category_id = compact_spaces(item.get("source_category_id") or "")
    source_category_name = compact_spaces(item.get("source_category_name") or "")
    text_haystack = "\n".join(x for x in [title, desc, tmdb_title, original, overview, countries_human, source_category_name] if x).lower()
    tech_haystack = "\n".join(
        x
        for x in [
            title,
            desc,
            str(item.get("source_format") or ""),
            str(item.get("source_year") or ""),
            str(item.get("source_episode_progress") or ""),
            str(item.get("media_type") or ""),
            audio,
            countries,
            source_category_id,
            source_category_name,
        ]
        if x
    ).lower()
    return text_haystack, tech_haystack


def keyword_matches_item(token: str, item: Dict[str, Any], text_haystack: Optional[str] = None, tech_haystack: Optional[str] = None) -> bool:
    token = compact_spaces(str(token or "")).lower()
    if not token:
        return False
    if text_haystack is None or tech_haystack is None:
        text_haystack, tech_haystack = build_keyword_haystacks(item)

    special_patterns = TECH_KEYWORD_PATTERNS.get(token)
    if special_patterns:
        return any(re.search(pattern, tech_haystack, flags=re.I) for pattern in special_patterns)

    if len(token) <= 3 and re.fullmatch(r"[\w\-\+]+", token, flags=re.I):
        return re.search(rf"(?<!\w){re.escape(token)}(?!\w)", text_haystack, flags=re.I) is not None

    return token in text_haystack
