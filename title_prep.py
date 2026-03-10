import re
from typing import Any, Dict, List, Tuple

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


TECH_ONLY_CANDIDATE_PATTERNS: List[str] = [
    r"^(?:mp3|flac|ape|alac|fb2|epub|mobi|pdf)$",
    r"^(?:webrip|web[\-\s]?dl(?:rip)?|bdrip|dvdrip|bluray|blu[\-\s]?ray|remux)$",
    r"^(?:hdtv(?:rip)?|iptv|dvb(?:rip)?|sat(?:rip)?)$",
    r"^(?:2160p?|1080p?|1080i|720p?|4k|uhd|hdr10\+?|hdr|sdr|hevc|avc|x264|x265|h264|h265)$",
    r"^(?:ру|rus|sub|subs|ст|дб|пм|лм|пд|по)$",
]


def clean_release_title(text: str) -> str:
    text = compact_spaces(strip_html(text or ""))
    if not text:
        return ""

    text = re.sub(r"\s+/\s+", " / ", text)
    replacements = [
        r"\[[^\]]+\]",
        r"\([^\)]*\)",
        r"\b(?:19\d{2}|20\d{2})\s*[-/]\s*(?:19\d{2}|20\d{2}|\d{2})\b",
        r"\b\d+\s*[xх]\s*\b",
        r"\b(19\d{2}|20\d{2})\b",
        r"\b(сезон|season)\s*\d+\b",
        r"\b\d+\s*\-\s*\d+\s*серии\s*из\s*\d+\b",
        r"\b\d+\s*серии\s*из\s*\d+\b",
        r"\b(s\d{1,2}e\d{1,3})\b",
        r"\b(дб|пм|лм|ст|пд|по|ру|укр|озвучка|sub|subs?|tvshows|lostfilm|hdrezka|coldfilm|newstudio|baibako|red\s*head\s*sound|dragon\s*money\s*studio|greb&creative|dezidenizi)\b",
        r"\b(2160p?|1080p?|1080i|720p?|4k|uhd|hdr10\+?|hdr|sdr|dolby\s*vision|dolby|vision|hevc|avc|x264|x265|h264|h265|aac|ac3|dts)\b",
        r"\b(web\-dlrip|web\-dl|webrip|hdtvrip|hdtv|iptv|dvb(?:rip)?|sat(?:rip)?|bluray\s*remux|blu\-ray\s*remux|bluray|blu\-ray|bdrip|dvdrip|remux|rip)\b",
        r"\b(mp3|flac|ape|alac|fb2|epub|mobi|pdf)\b",
        r"\b(portable|windows|linux|macos|pc)\b",
    ]
    for pattern in replacements:
        text = re.sub(pattern, " ", text, flags=re.I)

    text = re.sub(r"[^\w\dА-Яа-яЁё&:\-'\. /]+", " ", text)
    text = re.sub(r"\s*[-–—]\s*", " - ", text)
    text = re.sub(r"\s*/\s*", " / ", text)
    text = compact_spaces(text)

    segments = [segment.strip(" /.-") for segment in re.split(r"\s*/\s*", text) if segment.strip(" /.-")]
    cleaned_segments: List[str] = []
    for segment in segments:
        segment = re.sub(r"(?:\s+-\s+){2,}", " - ", segment)
        segment = re.sub(r"^(?:-\s*)+", "", segment)
        segment = re.sub(r"(?:\s*-)+$", "", segment)
        segment = compact_spaces(segment).strip(" /.-")
        if segment and segment != "-":
            cleaned_segments.append(segment)

    text = " / ".join(cleaned_segments)
    text = compact_spaces(text)
    return text.strip(" /.-")


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
    metadata_re = re.compile(
        r"\b(?:19\d{2}|20\d{2}|season|сезон|серия|серии|серий|эпизод|эпизоды|эпизодов|выпуск|выпуски|выпусков|"
        r"web(?:\-dlrip|\-dl|rip)?|hdtv(?:rip)?|iptv|dvb(?:rip)?|sat(?:rip)?|"
        r"1080p?|1080i|720p?|2160p?|4k|uhd|hdr10\+?|hdr|sdr|hevc|avc|x264|x265|h264|h265|"
        r"bdrip|bluray|blu\-ray|remux|"
        r"дб|пм|лм|ст|пд|по|ру|укр)\b",
        flags=re.I,
    )

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value:
            return
        if metadata_re.search(value):
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

    for match in re.finditer(r"\(([^()]{1,80})\)", base):
        inner = compact_spaces(match.group(1))
        if inner:
            add(inner)

    return aliases


def _latin_letter_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z]", text or ""))


def _cyrillic_letter_count(text: str) -> int:
    return len(re.findall(r"[А-Яа-яЁё]", text or ""))


def _strip_title_part_metadata(part: str) -> str:
    part = compact_spaces(part or "")
    part = re.sub(r"\([^\)]*\)", " ", part)
    part = re.split(
        r"\b(?:19\d{2}|20\d{2}|ДБ|ПМ|ЛМ|ПД|ПО|СТ|РУ|УКР|WEB|WEB\-DL|WEBRip|Blu|BDRip|Remux|Rip|HDR10\+?|HDR|SDR|HEVC|AVC|HDTV|HDTVRip|IPTV|DVB|DVBRip|SATRip|MP3|FLAC|FB2|EPUB|MOBI|PDF)\b",
        part,
        maxsplit=1,
        flags=re.I,
    )[0]
    part = compact_spaces(part).strip(" /.-")
    if part and is_bad_tmdb_candidate(part):
        cleaned = clean_release_title(part)
        if cleaned and not is_bad_tmdb_candidate(cleaned):
            part = cleaned
    return compact_spaces(part).strip(" /.-")


def split_title_parts(source_title: str) -> Tuple[str, str]:
    source_title = compact_spaces(strip_html(source_title or ""))
    raw_parts = [compact_spaces(p).strip(" /.-") for p in re.split(r"\s*/\s*", source_title) if compact_spaces(p).strip(" /.-")]
    parts: List[str] = []
    for part in raw_parts:
        cleaned = _strip_title_part_metadata(part)
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
