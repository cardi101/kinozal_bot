import re
from typing import Any, Dict, List, Tuple

from episode_progress import parse_episode_progress
from parsing_audio import AUDIO_TAGS, NON_TITLE_TECH_TAGS, infer_release_type, parse_audio_variants
from utils import compact_spaces, strip_html


RELEASE_GROUP_PATTERNS: List[str] = [
    r"lostfilm",
    r"tvshows?",
    r"hdrezka(?:\s*studio)?",
    r"coldfilm",
    r"newstudio",
    r"baibako",
    r"red\s*head\s*sound",
    r"dragon\s*money\s*studio",
    r"greb\s*&\s*creative",
    r"dezidenizi",
    r"le(?:d)?[\s\-]*production",
    r"anilibria",
    r"ani(?:dub|star)",
    r"softbox",
    r"dub(?:lik|club)",
    r"ru?dub",
    r"voice\s*project\s*studio",
    r"reanimedia",
]


def _token_pattern(token: str) -> str:
    return re.escape(token).replace(r"\-", r"[\-\s]?").replace(r"\ ", r"\s+")


_AUDIO_TOKEN_RE = r"(?:%s)" % "|".join(_token_pattern(tag) for tag in sorted(AUDIO_TAGS, key=len, reverse=True))
_TECH_TOKEN_RE = r"(?:%s)" % "|".join(_token_pattern(tag) for tag in sorted(NON_TITLE_TECH_TAGS, key=len, reverse=True))
_AUDIO_LABEL_RE = re.compile(
    r'(?:(\d{1,2})\s*[xх×]\s*)?'
    rf'(?<!\w)({_AUDIO_TOKEN_RE})(?!\w)'
    r'(?:\s*\(([^)]{1,120})\))?'
    r'(?:\s*[xх×]\s*(\d{1,2}))?',
    flags=re.I,
)

TECH_ONLY_CANDIDATE_PATTERNS: List[str] = [
    rf"^{_TECH_TOKEN_RE}$",
    rf"^{_AUDIO_TOKEN_RE}$",
    r"^(?:subs?)$",
]


def split_release_segments(text: str) -> List[str]:
    raw = compact_spaces(strip_html(text or ""))
    if not raw:
        return []

    parts: List[str] = []
    buf: List[str] = []
    paren_depth = 0
    bracket_depth = 0

    for ch in raw:
        if ch == "(":
            paren_depth += 1
        elif ch == ")" and paren_depth > 0:
            paren_depth -= 1
        elif ch == "[":
            bracket_depth += 1
        elif ch == "]" and bracket_depth > 0:
            bracket_depth -= 1

        if ch == "/" and paren_depth == 0 and bracket_depth == 0:
            segment = compact_spaces("".join(buf)).strip(" /.-")
            if segment:
                parts.append(segment)
            buf = []
            continue
        buf.append(ch)

    tail = compact_spaces("".join(buf)).strip(" /.-")
    if tail:
        parts.append(tail)
    return parts


def _compact_segment(value: str) -> str:
    value = compact_spaces(value or "")
    value = re.sub(r"(?:\s+-\s+){2,}", " - ", value)
    value = re.sub(r"^(?:-\s*)+", "", value)
    value = re.sub(r"(?:\s*-)+$", "", value)
    return compact_spaces(value).strip(" /.-")


def _looks_like_year_segment(segment: str) -> bool:
    return bool(re.fullmatch(r"(?:19\d{2}|20\d{2})(?:\s*[-/]\s*(?:19\d{2}|20\d{2}|\d{2}))?", compact_spaces(segment or "")))


def _looks_like_audio_segment(segment: str) -> bool:
    segment = compact_spaces(segment or "")
    if not segment:
        return False
    if not parse_audio_variants(segment):
        return False
    residual = _AUDIO_LABEL_RE.sub(" ", segment)
    residual = re.sub(r"[\s,;+&|]+", " ", residual)
    residual = compact_spaces(residual).strip(" /.-")
    return residual == ""


def _looks_like_tech_segment(segment: str) -> bool:
    value = compact_spaces(segment or "")
    if not value:
        return False
    lowered = value.lower()
    if any(re.fullmatch(pattern, lowered, flags=re.I) for pattern in TECH_ONLY_CANDIDATE_PATTERNS):
        return True
    tokens = [token for token in re.split(r"[\s,;:+/&|()\[\]]+", lowered) if token]
    if not tokens:
        return False
    tech_tokens = {
        token.lower().replace(" ", "").replace("-", "")
        for token in NON_TITLE_TECH_TAGS
    } | {"sub", "subs", "aac", "ac3", "dts", "dolby", "vision"}
    normalized = [token.replace("-", "") for token in tokens]
    return all(token in tech_tokens or token.isdigit() for token in normalized)


def _clean_plain_title_text(text: str) -> str:
    value = compact_spaces(text or "")
    if not value:
        return ""

    replacements = [
        r"\[[^\]]+\]",
        r"\([^\)]*\)",
        r"\b(?:19\d{2}|20\d{2})\s*[-/]\s*(?:19\d{2}|20\d{2}|\d{2})\b",
        r"\b\d+\s*[xх×]\s*\b",
        r"\b(19\d{2}|20\d{2})\b",
        r"\b(сезон|season)\s*\d+\b",
        r"\b\d+\s*\-\s*\d+\s*серии\s*из\s*\d+\b",
        r"\b\d+\s*серии\s*из\s*\d+\b",
        r"\b(s\d{1,2}e\d{1,3})\b",
        rf"\b(?:{_AUDIO_TOKEN_RE}|tvshows|lostfilm|hdrezka|coldfilm|newstudio|baibako|red\s*head\s*sound|dragon\s*money\s*studio|greb\s*&\s*creative|dezidenizi)\b",
        rf"\b(?:{_TECH_TOKEN_RE}|subs?|dolby\s*vision|dolby|vision|aac|ac3|dts)\b",
        r"\b(portable|windows|linux|macos|pc)\b",
    ]
    for pattern in replacements:
        value = re.sub(pattern, " ", value, flags=re.I)

    value = re.sub(r"[^\w\dА-Яа-яЁё&:\-'\. /]+", " ", value)
    value = re.sub(r"\s*[-–—]\s*", " - ", value)
    value = re.sub(r"\s*/\s*", " / ", value)
    value = compact_spaces(value)

    cleaned_segments = [_compact_segment(segment) for segment in split_release_segments(value)]
    cleaned_segments = [segment for segment in cleaned_segments if segment and segment != "-"]
    return compact_spaces(" / ".join(cleaned_segments)).strip(" /.-")


def _title_aliases_from_segment(segment: str) -> List[str]:
    aliases: List[str] = []

    def add(value: str) -> None:
        candidate = compact_spaces(clean_release_title(value) or value).strip(" /.-")
        if not candidate:
            return
        if is_release_group_candidate(candidate):
            return
        if not looks_like_structured_numeric_title(candidate) and is_bad_tmdb_candidate(candidate):
            return
        if candidate not in aliases:
            aliases.append(candidate)

    for match in re.finditer(r"\(([^()]{1,80})\)", segment):
        inner = compact_spaces(match.group(1))
        if not inner:
            continue
        if _looks_like_year_segment(inner):
            continue
        if parse_episode_progress(inner):
            continue
        if _looks_like_audio_segment(inner):
            continue
        if infer_release_type(inner):
            continue
        if _looks_like_tech_segment(inner):
            continue
        if is_release_group_candidate(inner):
            continue
        add(inner)
    return aliases


def _clean_title_segment(segment: str) -> str:
    cleaned = compact_spaces(segment or "")
    cleaned = re.sub(r"\[[^\]]+\]", " ", cleaned)
    cleaned = re.sub(r"\([^\)]*\)", " ", cleaned)
    cleaned = compact_spaces(cleaned)
    if parse_episode_progress(cleaned):
        cleaned = re.sub(r"\b(?:\d+\s*сезон\s*:\s*)?\d+\s*(?:-\s*\d+\s*)?(?:сер(?:ия|ии|ий)|выпуск(?:а|ов)?)\s*(?:из\s*\d+)?\b", " ", cleaned, flags=re.I)
        cleaned = re.sub(r"\b(?:s\d{1,2}\s*e\d{1,3}(?:\s*-\s*e\d{1,3})?|\d{1,2}x\d{1,3}(?:\s*-\s*(?:\d{1,2}x)?\d{1,3})?)\b", " ", cleaned, flags=re.I)
    cleaned = _clean_plain_title_text(cleaned)
    return compact_spaces(cleaned).strip(" /.-")


def classify_release_segments(text: str) -> List[Dict[str, Any]]:
    segments: List[Dict[str, Any]] = []
    for raw in split_release_segments(text):
        segment = compact_spaces(raw)
        info: Dict[str, Any] = {
            "raw": segment,
            "kind": "title_ru",
            "title": "",
            "aliases": [],
            "episode_progress": parse_episode_progress(segment) or "",
        }

        if _looks_like_year_segment(segment):
            info["kind"] = "year"
        elif _looks_like_audio_segment(segment):
            info["kind"] = "audio"
        elif infer_release_type(segment):
            info["kind"] = "release_type"
        elif _looks_like_tech_segment(segment):
            info["kind"] = "tech"
        elif info["episode_progress"] and not _clean_title_segment(segment):
            info["kind"] = "episode_progress"
        else:
            title = _clean_title_segment(segment) or _compact_segment(segment)
            info["title"] = title
            info["aliases"] = _title_aliases_from_segment(segment)
            info["kind"] = "title_en" if _latin_letter_count(title) > _cyrillic_letter_count(title) else "title_ru"
        segments.append(info)
    return segments


def clean_release_title(text: str) -> str:
    base = compact_spaces(strip_html(text or ""))
    if not base:
        return ""

    title_segments = [
        compact_spaces(segment.get("title") or "")
        for segment in classify_release_segments(base)
        if str(segment.get("kind") or "").startswith("title")
    ]
    deduped = [segment for segment in dict.fromkeys(title_segments) if segment]
    if deduped:
        return compact_spaces(" / ".join(deduped)).strip(" /.-")
    return _clean_plain_title_text(base)


def looks_like_structured_numeric_title(text: str) -> bool:
    text = compact_spaces(str(text or "")).strip(" /.-")
    if not text:
        return False
    return bool(re.fullmatch(r"\d+(?:\s*[-–—:]\s*\d+){2,}", text))


def is_release_group_candidate(text: str) -> bool:
    raw = compact_spaces(str(text or "")).strip(" /.-")
    if not raw:
        return False
    lowered = raw.lower()
    return any(re.fullmatch(pattern, lowered, flags=re.I) for pattern in RELEASE_GROUP_PATTERNS)


def normalize_structured_numeric_title(text: str) -> str:
    value = compact_spaces(str(text or "")).strip(" /.-")
    if not value:
        return ""
    value = re.sub(r"\s*[-–—:]\s*", "-", value)
    return value


def extract_structured_numeric_title_candidates(text: str) -> List[str]:
    raw = compact_spaces(strip_html(text or ""))
    if not raw:
        return []
    found: List[str] = []
    for match in re.finditer(r"(?<!\d)(\d+(?:\s*[-–—:]\s*\d+){2,})(?!\d)", raw):
        raw_candidate = compact_spaces(match.group(1)).strip(" /.-")
        canonical_candidate = normalize_structured_numeric_title(raw_candidate)
        for candidate in (canonical_candidate, raw_candidate):
            candidate = compact_spaces(candidate).strip(" /.-")
            if candidate and candidate not in found:
                found.append(candidate)
    return found


def should_skip_tmdb_lookup(item: Dict[str, Any]) -> bool:
    category_name = compact_spaces(str(item.get("source_category_name") or "")).lower()
    category_id = compact_spaces(str(item.get("source_category_id") or ""))
    title = compact_spaces(str(item.get("source_title") or ""))
    title_l = title.lower()

    if not title:
        return False

    is_sport_category = category_id == "37" or "спорт" in category_name
    is_show_category = category_id in {"49", "50"} or "тв-шоу" in category_name or "передачи" in category_name

    sports_title_signal = bool(re.search(r"\b(?:футбол|хоккей|теннис|баскетбол|биатлон|бокс|мма|ufc|nhl|nba|formula\s*1|formula\s*2|motogp|лыж\w*|динамо\s+киев|бильярд|снукер|пирамида)\b", title_l, flags=re.I))
    sports_event_signal = bool(re.search(r"\b(?:матч|матчи|все\s+матчи|сезона?|20\d{2}/\d{2}|чемпионат|лига|кубок|турнир|тур|раунд|обзор|плей[\-\s]?офф|vs|v\.|эфир)\b", title_l, flags=re.I))
    episode_or_issue_signal = bool(re.search(r"\b(?:выпуск|выпуски|выпусков|эфир|эфиры)\b", title_l, flags=re.I))

    if is_sport_category:
        return True
    if sports_title_signal and sports_event_signal:
        return True
    if is_show_category and sports_title_signal:
        return True
    if is_show_category and episode_or_issue_signal and not item.get("imdb_id"):
        return True

    return False


def extract_title_aliases_from_text(text: str) -> List[str]:
    base = compact_spaces(strip_html(text or ""))
    if not base:
        return []

    aliases: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value:
            return
        if is_release_group_candidate(value):
            return
        if looks_like_structured_numeric_title(value):
            cleaned_value = value
        else:
            cleaned_value = clean_release_title(value)
        candidate = compact_spaces(cleaned_value or value).strip(" /.-")
        if not candidate:
            return
        if not looks_like_structured_numeric_title(candidate) and is_bad_tmdb_candidate(candidate):
            return
        if candidate not in aliases:
            aliases.append(candidate)

    for segment in classify_release_segments(base):
        if not str(segment.get("kind") or "").startswith("title"):
            continue
        for alias in segment.get("aliases") or []:
            add(str(alias))

    return aliases


def _latin_letter_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z]", text or ""))


def _cyrillic_letter_count(text: str) -> int:
    return len(re.findall(r"[А-Яа-яЁё]", text or ""))


def _strip_title_part_metadata(part: str) -> str:
    cleaned = _clean_title_segment(part)
    if cleaned and is_bad_tmdb_candidate(cleaned):
        fallback = clean_release_title(cleaned)
        if fallback and not is_bad_tmdb_candidate(fallback):
            cleaned = fallback
    return compact_spaces(cleaned).strip(" /.-")


def split_title_parts(source_title: str) -> Tuple[str, str]:
    source_title = compact_spaces(strip_html(source_title or ""))
    parts: List[str] = []
    for segment in classify_release_segments(source_title):
        if not str(segment.get("kind") or "").startswith("title"):
            continue
        cleaned = _strip_title_part_metadata(str(segment.get("title") or segment.get("raw") or ""))
        if cleaned:
            parts.append(cleaned)

    ru = ""
    en = ""

    def is_good_en_candidate(part: str, strict: bool) -> bool:
        if looks_like_structured_numeric_title(part):
            return True
        latin = _latin_letter_count(part)
        cyr = _cyrillic_letter_count(part)
        if latin < 3:
            return False
        if strict:
            return cyr == 0 and not is_bad_tmdb_candidate(part)
        return latin > cyr * 2 and not is_bad_tmdb_candidate(part)

    def is_good_ru_candidate(part: str, strict: bool) -> bool:
        if looks_like_structured_numeric_title(part):
            return False
        latin = _latin_letter_count(part)
        cyr = _cyrillic_letter_count(part)
        if cyr < 2:
            return False
        if strict:
            return latin == 0
        return cyr >= latin and not is_bad_tmdb_candidate(part)

    for part in parts:
        if not ru and is_good_ru_candidate(part, strict=True):
            ru = part
        if not en and is_good_en_candidate(part, strict=True):
            en = part

    for part in parts:
        if not ru and is_good_ru_candidate(part, strict=False):
            ru = part
        if not en and is_good_en_candidate(part, strict=False):
            en = part

    return ru, en


def is_bad_tmdb_candidate(text: str) -> bool:
    raw = compact_spaces(text or "").strip(" /.-")
    if not raw:
        return True
    if looks_like_structured_numeric_title(raw):
        return False

    lowered = raw.lower()
    if any(re.fullmatch(pattern, lowered, flags=re.I) for pattern in TECH_ONLY_CANDIDATE_PATTERNS):
        return True

    if re.search(r"\b(?:сезон|сезона|сезоны|серия|серии|серий|выпуск|выпуски|выпусков|эпизод|эпизоды|эпизодов)\b", lowered, flags=re.I):
        return True
    if re.search(r"\bиз\s*\d+\b", lowered, flags=re.I):
        return True
    if re.search(r"\b\d+\s*[-–—:]\s*\d+\b", lowered):
        return True

    tokens = [t for t in re.split(r"[\s/\-:,;()\[\]]+", lowered) if t]
    if not tokens:
        return True

    tech_tokens = {
        "mp3", "flac", "ape", "alac", "fb2", "epub", "mobi", "pdf",
        "webrip", "web", "webdl", "web-dl", "webdlrip", "web-dlrip",
        "hdtv", "hdtvrip", "iptv", "dvb", "dvbrip", "satrip",
        "2160", "2160p", "1080", "1080p", "1080i", "720", "720p", "4k", "uhd",
        "hdr", "hdr10", "hdr10+", "sdr", "hevc", "avc", "x264", "x265", "h264", "h265",
        "remux", "bdrip", "dvdrip", "bluray", "blu-ray", "rip",
        "ру", "rus", "sub", "subs", "ст", "дб", "пм", "лм", "пд", "по",
    }
    if all(token in tech_tokens or token.isdigit() for token in tokens):
        return True

    if len(tokens) <= 2 and sum(token in tech_tokens for token in tokens) >= 1:
        meaningful = [token for token in tokens if token not in tech_tokens and not token.isdigit()]
        if not meaningful:
            return True

    return False
