import asyncio
import hashlib
import html
import json
import logging
import os
import random
import re
import string
import threading
import time
import unicodedata
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from email.utils import parsedate_to_datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import httpx
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from psycopg import connect
from psycopg.rows import dict_row
import redis.asyncio as redis
from urllib.parse import urlencode
from config import CFG, ACCESS_EXPIRY_UNSET
from states import EditInputState
from utils import utc_ts, now_utc, parse_dt, compact_spaces, strip_html, short, md5_text, sha1_text
from parsing_basic import parse_year, parse_years, parse_format, parse_imdb_id
from text_access import format_dt, user_access_state, format_access_expiry, human_media_type, html_to_plain_text, require_access_message
from source_categories import normalize_source_category_id, resolve_source_category_name, source_category_is_non_video, source_category_forced_media_type, source_category_bucket_hint, source_category_fallback_country_codes
from release_versioning import parse_episode_progress, normalize_episode_progress_signature, extract_kinozal_id, resolve_item_kinozal_id, build_source_uid, normalize_audio_tracks_signature, version_release_type_signature, build_variant_signature, build_item_variant_signature, get_variant_components, get_item_variant_components, describe_variant_change, format_variant_summary, build_version_signature
from country_helpers import ANIME_COUNTRY_CODES, parse_jsonish_list, parse_country_codes, country_name_ru, human_country_names, effective_item_countries, normalize_tmdb_language, has_asian_script, asian_dorama_signal_score, human_content_filter
from parsing_audio import parse_audio_variants, format_audio_variants, count_audio_variants, parse_audio_tracks, infer_release_type, format_release_full_title
from keyboards import main_menu_kb, subscriptions_list_kb, sub_view_kb, sub_type_kb, year_preset_kb, rating_kb, format_kb, preset_kb, wizard_type_kb, wizard_years_kb, wizard_rating_kb, admin_invites_kb, admin_users_kb

try:
    import pycountry
except Exception:
    pycountry = None

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("kinozal-news-bot")


def is_non_video_release(text: str) -> bool:
    text = compact_spaces((text or "").lower())

    music_genres_pattern = r"(?:pop|rock|hip[\s\-]*hop|rap|jazz|blues|metal|disco|house|techno|trance|edm|folk|country|k[\s\-]*pop|j[\s\-]*pop|r[\s&]*b)"
    if re.search(rf"/\s*{music_genres_pattern}\s*/", text, flags=re.I):
        return True
    if re.search(rf"^[^/\n]{{2,120}}\s-\s[^/\n]{{2,120}}/\s*{music_genres_pattern}\s*/", text, flags=re.I):
        return True

    strong_patterns = [
        r"\bpc\s*\(windows\)\b",
        r"\bplaystation\b",
        r"\bps5\b",
        r"\bps4\b",
        r"\bxbox\b",
        r"\bnintendo\b",
        r"\bswitch\b",
        r"\brepack\b",
        r"\bdlc\b",
        r"\bgog\b",
        r"\bsteam\b",
        r"\bbuild\s*\d+\b",
        r"\bupdate\s*v?\d+(?:\.\d+)*\b",
        r"\bplugin\b",
        r"\bdriver\b",
        r"\bcrack\b",
        r"\bmp3\b",
        r"\bflac\b",
        r"\balac\b",
        r"\bape\b",
        r"\blossless\b",
        r"\bdiscography\b",
        r"\bvinyl\b",
        r"\bconcert\s+recording\b",
        r"\bаудиокнига\b",
        r"\bдискография\b",
        r"\bальбом\b",
        r"\bсингл\b",
        r"\bсаундтрек\b",
        r"\bтреклист\b",
        r"\bebook\b",
        r"\bpdf\b",
        r"\bepub\b",
        r"\bmobi\b",
        r"\bfb2\b",
        r"\bmagazine\b",
        r"\bжурнал\b",
        r"\bкомикс\b",
        r"\bманга\b",
        r"\bаудиоспектакль\b",
    ]
    if any(re.search(p, text, flags=re.I) for p in strong_patterns):
        return True

    software_platforms = [
        r"\bandroid\b", r"\bios\b", r"\bmacos\b", r"\blinux\b", r"\bportable\b",
        r"\bx64\b", r"\bx86\b", r"\bapk\b", r"\bipa\b", r"\bexe\b", r"\bmsi\b",
    ]
    software_context = [
        r"\bapp\b", r"\bapplication\b", r"\bsoftware\b", r"\bprogram\b", r"\butility\b",
        r"\binstaller\b", r"\bsetup\b", r"\bpatch\b", r"\bmod\b", r"\bplugin\b",
        r"\bdriver\b", r"\bbuild\s*\d+\b", r"\bupdate\s*v?\d+(?:\.\d+)*\b",
        r"\bверсия\b", r"\bустановщик\b", r"\bпрограмма\b",
    ]
    has_software_platform = any(re.search(p, text, flags=re.I) for p in software_platforms)
    has_software_context = any(re.search(p, text, flags=re.I) for p in software_context)
    if has_software_platform and has_software_context:
        return True

    game_platform_patterns = [
        r"\bpc\s*\(windows\)\b", r"\bwindows\b", r"\bx64\b", r"\bx86\b",
        r"\bportable\b", r"\brepack\b", r"\bgog\b", r"\bsteam\b", r"\bdlc\b",
    ]
    game_genre_patterns = [
        r"\brpg\b", r"\bsimulator\b", r"\bstrategy\b", r"\badventure\b",
        r"\baction\b", r"\bshooter\b", r"\bracing\b", r"\bhorror\b",
        r"\bsurvival\b", r"\barcade\b", r"\bquest\b",
    ]
    has_game_platform = any(re.search(p, text, flags=re.I) for p in game_platform_patterns)
    has_game_genre = any(re.search(p, text, flags=re.I) for p in game_genre_patterns)
    if has_game_platform and has_game_genre:
        return True

    bookish_patterns = [
        r"\bbook\b.+\b(?:pdf|epub|mobi|fb2)\b",
        r"\b(?:pdf|epub|mobi|fb2)\b.+\bbook\b",
        r"\bкнига\b.+\b(?:pdf|epub|mobi|fb2)\b",
        r"\b(?:pdf|epub|mobi|fb2)\b.+\bкнига\b",
    ]
    if any(re.search(p, text, flags=re.I) for p in bookish_patterns):
        return True

    sports_entities = [
        r"\bnhl\b", r"\bnba\b", r"\bufc\b", r"\bformula\s*1\b", r"\bformula\s*2\b",
        r"\bmoto\s*gp\b", r"\bmotogp\b", r"\bboxing\b", r"\bwrestling\b", r"\bfootball\b",
        r"\bsoccer\b", r"\btennis\b", r"\bbiathlon\b", r"\bski(?:ing)?\b", r"\brelay\b",
        r"\bхоккей\b", r"\bфутбол\b", r"\bтеннис\b", r"\bбиатлон\b", r"\bлыж\w*\b",
        r"\bбокс\b", r"\bмма\b", r"\bбаскетбол\b", r"\bформула\s*1\b", r"\bформула\s*2\b",
        r"\bмотогп\b", r"\bединоборств\w*\b", r"\bэстафет\w*\b",
        r"\bмасс[\s\-]?старт\b", r"\bспринт\w*\b", r"\bгонка\s+преследован\w*\b",
        r"\bиндивидуальн\w*\s+гонк\w*\b", r"\bпасьют\b",
    ]
    sports_context = [
        r"\bматч\b", r"\bчемпионат\b", r"\bтурнир\b", r"\blive\b", r"\bпрямая\s+трансляц\w*\b",
        r"\bэфир\b", r"\bобзор\b", r"\bраунд\b", r"\bvs\b", r"\bgrand\s+prix\b", r"\bgp\b",
        r"\bгонка\b", r"\bквалификац\w*\b", r"\bкубок\b", r"\bфинал\w*\b", r"\bлига\b",
        r"\bгран[\-\s]?при\b", r"\bэтап\b", r"\bспринт\w*\b", r"\bпрактик\w*\b",
        r"\brace\b", r"\bqualifying\b", r"\bsprint\b", r"\bpractice\b", r"\brelay\b",
        r"\bmass\s*start\b", r"\bpursuit\b", r"\bindividual\b", r"\bworld\s+cup\b",
        r"\bэстафет\w*\b", r"\bмасс[\s\-]?старт\b", r"\bгонка\s+преследован\w*\b",
        r"\bиндивидуальн\w*\s+гонк\w*\b", r"\bкубок\s+мира\b", r"\bчемпионат\s+мира\b",
        r"\b1/8\b", r"\b1/4\b", r"\b1/2\b", r"\bплей[\-\s]?офф\b", r"\bplay[\-\s]?off\b",
        r"\b[А-ЯA-Z][^/\n]{1,30}\s[-–]\s[А-ЯA-Z][^/\n]{1,30}\b",
    ]
    has_sports_entity = any(re.search(p, text, flags=re.I) for p in sports_entities)
    has_sports_context = any(re.search(p, text, flags=re.I) for p in sports_context)
    if has_sports_entity and has_sports_context:
        return True

    return False

def detect_media_type(text: str) -> str:
    text = (text or "").lower()
    if is_non_video_release(text):
        return "other"
    tv_patterns = [
        r"\bs\d{1,2}e\d{1,3}\b",
        r"\bseason\b",
        r"\bсер(ия|ии|ий)\b",
        r"\bсезон\b",
        r"\bэпизод\b",
        r"\bepisode\b",
    ]
    for pattern in tv_patterns:
        if re.search(pattern, text, flags=re.I):
            return "tv"
    return "movie"


def item_duplicate_quality_score(item: Dict[str, Any]) -> int:
    score = 0
    if item.get("tmdb_id"):
        score += 100
    if compact_spaces(str(item.get("imdb_id") or "")):
        score += 25
    if compact_spaces(str(item.get("tmdb_overview") or "")):
        score += 10
    if compact_spaces(str(item.get("source_description") or "")):
        score += 8
    if compact_spaces(str(item.get("tmdb_poster_url") or "")):
        score += 5
    if parse_country_codes(item.get("tmdb_countries")):
        score += 4
    if compact_spaces(str(item.get("tmdb_status") or "")):
        score += 3
    if compact_spaces(str(item.get("manual_bucket") or "")):
        score += 2
    if int(item.get("tmdb_vote_count") or 0) > 0:
        score += 2
    return score


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

TECH_ONLY_CANDIDATE_PATTERNS: List[str] = [
    r"^(?:mp3|flac|ape|alac|fb2|epub|mobi|pdf)$",
    r"^(?:webrip|web[\-\s]?dl(?:rip)?|bdrip|dvdrip|bluray|blu[\-\s]?ray|remux)$",
    r"^(?:hdtv(?:rip)?|iptv|dvb(?:rip)?|sat(?:rip)?)$",
    r"^(?:2160p?|1080p?|1080i|720p?|4k|uhd|hdr10\+?|hdr|sdr|hevc|avc|x264|x265|h264|h265)$",
    r"^(?:ру|rus|sub|subs|ст|дб|пм|лм|пд|по)$",
]


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


def expand_tmdb_candidate_variants(text: str) -> List[str]:
    base = compact_spaces(text or "").strip(" /.-")
    if not base:
        return []

    variants: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value or len(value.split()) > 8 or len(value) > 120:
            return
        if is_bad_tmdb_candidate(value):
            return
        if value not in variants:
            variants.append(value)

    add(base)
    add(clean_release_title(base))

    def should_skip_split_fragment(fragment: str, original: str) -> bool:
        fragment = compact_spaces(fragment or "").strip(" /.-")
        original = compact_spaces(original or "").strip(" /.-")
        if not fragment or not original:
            return False
        fragment_tokens = text_tokens(fragment)
        original_tokens = text_tokens(original)
        if len(fragment_tokens) == 1 and len(original_tokens) >= 2 and is_generic_cyrillic_title(fragment):
            return True
        return False

    # Полезный алиас в круглых скобках должен идти отдельным кандидатом:
    # Wolgannamchin (Boyfriend on Demand) -> Boyfriend on Demand
    for match in re.finditer(r"\(([^()]{2,80})\)", base):
        inner = compact_spaces(match.group(1)).strip(" /.-")
        outer = compact_spaces(base[:match.start()] + " " + base[match.end():]).strip(" /.-")
        if inner and not is_bad_tmdb_candidate(inner):
            add(inner)
            add(clean_release_title(inner))
        if outer and outer != base:
            add(outer)
            add(clean_release_title(outer))
        if inner and outer:
            add(f"{outer} {inner}")

    for sep_pattern in [r"\s*:\s*", r"\s+[\-–—]\s+"]:
        parts = re.split(sep_pattern, base, maxsplit=1)
        if len(parts) == 2:
            left, right = parts[0], parts[1]
            if not should_skip_split_fragment(left, base):
                add(left)
                add(clean_release_title(left))
            if not should_skip_split_fragment(right, base):
                add(right)
                add(clean_release_title(right))

    latin_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", base)
    if len(latin_words) >= 4:
        for n in (5, 4, 3, 2):
            if len(latin_words) >= n:
                add(" ".join(latin_words[:n]))
        if ":" in base:
            left = compact_spaces(base.split(":", 1)[0])
            add(left)
            left_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", left)
            if len(left_words) >= 2:
                add(" ".join(left_words[: min(4, len(left_words))]))

    return variants


def is_long_latin_tmdb_query(query: str) -> bool:
    query = compact_spaces(query or "")
    latin_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", query)
    if len(latin_words) < 4:
        return False
    return ":" in query or len(query) >= 28 or any("-" in word for word in latin_words)


def is_short_or_common_tmdb_query(query: str) -> bool:
    q = clean_release_title(query or "")
    tokens = text_tokens(q)
    latin_words = re.findall(r"[A-Za-z][A-Za-z0-9'\-]*", q)
    if len(tokens) <= 2:
        return True
    if len(q) <= 14:
        return True
    if len(latin_words) <= 2 and len(q) <= 18:
        return True
    return False


def is_short_acronym_tmdb_query(query: str) -> bool:
    q = compact_spaces(clean_release_title(query or ""))
    if not q:
        return False
    if not re.fullmatch(r"[A-Za-z]{2,4}", q):
        return False
    return q.upper() == q


ANIME_SEARCH_ALIAS_MAP: Dict[str, List[str]] = {
    "trigun": ["Trigun Stampede", "Trigun"],
    "триган": ["Trigun Stampede", "Trigun"],
    "gundam": ["Mobile Suit Gundam", "Gundam"],
    "гандам": ["Mobile Suit Gundam", "Gundam"],
    "detective conan": ["Detective Conan", "Case Closed"],
    "детектив конан": ["Detective Conan", "Case Closed"],
    "case closed": ["Detective Conan", "Case Closed"],
    "one piece": ["One Piece"],
    "ван пис": ["One Piece"],
    "naruto": ["Naruto"],
    "наруто": ["Naruto"],
    "bleach": ["Bleach"],
    "блич": ["Bleach"],
    "jojo": ["JoJo's Bizarre Adventure", "JoJo no Kimyou na Bouken"],
    "джоджо": ["JoJo's Bizarre Adventure", "JoJo no Kimyou na Bouken"],
    "evangelion": ["Neon Genesis Evangelion", "Evangelion"],
    "евангелион": ["Neon Genesis Evangelion", "Evangelion"],
    "gintama": ["Gintama"],
    "гинтама": ["Gintama"],
    "lupin": ["Lupin the Third", "Lupin III"],
    "люпен": ["Lupin the Third", "Lupin III"],
    "dragon ball": ["Dragon Ball"],
    "драгон болл": ["Dragon Ball"],
    "sailor moon": ["Sailor Moon"],
    "сейлор мун": ["Sailor Moon"],
    "monogatari": ["Monogatari"],
    "bakemonogatari": ["Monogatari"],
    "one punch man": ["One-Punch Man"],
    "ванпанчмен": ["One-Punch Man"],
    "mob psycho": ["Mob Psycho 100"],
    "soul land": ["Soul Land", "Douluo Continent"],
    "боевой континент": ["Soul Land", "Douluo Continent"],
    "douluo": ["Soul Land", "Douluo Continent"],
    "battle through the heavens": ["Battle Through the Heavens", "Fights Break Sphere"],
    "doupo cangqiong": ["Battle Through the Heavens", "Fights Break Sphere"],
    "the king s avatar": ["The King's Avatar", "Quan Zhi Gao Shou"],
    "аватар короля": ["The King's Avatar", "Quan Zhi Gao Shou"],
    "quan zhi gao shou": ["The King's Avatar", "Quan Zhi Gao Shou"],
    "link click": ["Link Click", "Shiguang Dailiren"],
    "агент времени": ["Link Click", "Shiguang Dailiren"],
    "shiguang dailiren": ["Link Click", "Shiguang Dailiren"],
    "scissor seven": ["Scissor Seven", "Cike Wu Liuqi"],
    "white cat legend": ["White Cat Legend", "Dali Si Rizhi"],
    "grandmaster of demonic cultivation": ["The Founder of Diabolism", "Mo Dao Zu Shi"],
    "mdzs": ["The Founder of Diabolism", "Mo Dao Zu Shi"],
    "mo dao zu shi": ["The Founder of Diabolism", "Mo Dao Zu Shi"],
    "heaven official s blessing": ["Heaven Official's Blessing", "Tian Guan Ci Fu"],
    "tian guan ci fu": ["Heaven Official's Blessing", "Tian Guan Ci Fu"],
}

ANIME_TITLE_MARKER_RE = re.compile(r"\b(?:ova|ona|anime|donghua|manhwa|manhua)\b", flags=re.I)


MANUAL_SEARCH_ALIAS_MAP: Dict[str, List[str]] = {
    "mihonhan saramdeului hyoyuljeokin mannam": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
        "Efficient Dating of Single Men and Women",
    ],
    "mihonhan saramdeului hyoyuljeokin": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "mihonhan saramdeului": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "mihonnamnyeoui hyoyuljeok mannam": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "미혼한 사람들의 효율적인 만남": [
        "미혼남녀의 효율적 만남",
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "미혼남녀의 효율적 만남": [
        "미혼남녀의 효율적 만남",
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
    "эффективные свидания для одиночек": [
        "Mihonnamnyeoui Hyoyuljeok Mannam",
        "The Practical Guide to Love",
        "Efficient Dating for Singles",
    ],
}



MANUAL_TMDB_OVERRIDE_MAP: Dict[str, Tuple[str, int]] = {
    "oshi no ko": ("tv", 203737),
    "звёздное дитя": ("tv", 203737),
    "ребёнок айдола": ("tv", 203737),
    "【推しの子】": ("tv", 203737),
}


def manual_tmdb_override_for_item(item: Dict[str, Any]) -> Optional[Tuple[str, int, str]]:
    values = [
        compact_spaces(str(item.get("source_title") or "")),
        compact_spaces(str(item.get("cleaned_title") or "")),
        compact_spaces(str(item.get("source_episode_progress") or "")),
    ]
    candidates: List[str] = []
    for value in values:
        if not value:
            continue
        candidates.append(value)
        candidates.extend(extract_title_aliases_from_text(value))
        candidates.extend(title_search_candidates(value, clean_release_title(value) or value))
    seen: set[str] = set()
    for value in candidates:
        norm = normalize_match_text(value)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        for key, override in MANUAL_TMDB_OVERRIDE_MAP.items():
            key_norm = normalize_match_text(key)
            if not key_norm:
                continue
            if norm == key_norm or key_norm in norm or norm in key_norm:
                media_type, tmdb_id = override
                return media_type, int(tmdb_id), key
    return None


def manual_alias_candidates_from_text(text: str) -> List[str]:
    base = compact_spaces(text or "").strip()
    if not base:
        return []
    norm = normalize_match_text(base)
    if not norm:
        return []
    norm_tokens = set(re.findall(r"[a-zа-яё0-9]+", norm))
    variants: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value:
            return
        if looks_like_structured_numeric_title(value):
            candidate = value
        else:
            candidate = clean_release_title(value) or value
        candidate = compact_spaces(candidate).strip(" /.-")
        if not candidate:
            return
        if not looks_like_structured_numeric_title(candidate) and is_bad_tmdb_candidate(candidate):
            return
        if candidate not in variants:
            variants.append(candidate)

    for key, aliases in MANUAL_SEARCH_ALIAS_MAP.items():
        key_norm = normalize_match_text(key)
        if not key_norm:
            continue
        key_tokens = set(re.findall(r"[a-zа-яё0-9]+", key_norm))
        if not key_tokens:
            continue
        matched = False
        if key_norm in norm or norm in key_norm:
            matched = True
        elif norm_tokens and key_tokens <= norm_tokens:
            matched = True
        elif norm_tokens and len(key_tokens) >= 2 and len(key_tokens & norm_tokens) >= max(2, len(key_tokens) - 1):
            matched = True
        if matched:
            for alias in aliases:
                add(alias)

    return variants


def anime_alias_candidates_from_text(text: str) -> List[str]:
    base = compact_spaces(text or "").strip()
    if not base:
        return []
    norm = normalize_match_text(base)
    if not norm:
        return []
    norm_tokens = set(re.findall(r"[a-zа-яё0-9]+", norm))
    variants: List[str] = []

    def add(value: str) -> None:
        value = compact_spaces(value or "").strip(" /.-")
        if not value or is_bad_tmdb_candidate(value):
            return
        if value not in variants:
            variants.append(value)

    for key, aliases in ANIME_SEARCH_ALIAS_MAP.items():
        key_norm = normalize_match_text(key)
        if not key_norm:
            continue
        key_tokens = set(re.findall(r"[a-zа-яё0-9]+", key_norm))
        if not key_tokens:
            continue
        matched = False
        if key_norm in norm or norm in key_norm:
            matched = True
        elif norm_tokens and key_tokens <= norm_tokens:
            matched = True
        elif norm_tokens and len(key_tokens) <= 3 and len(key_tokens & norm_tokens) >= len(key_tokens):
            matched = True
        if matched:
            for alias in aliases:
                add(alias)

    return variants


def title_search_candidates(source_title: str, cleaned_title: str) -> List[str]:
    ru, en = split_title_parts(source_title)
    candidates: List[str] = []
    numeric_titles = extract_structured_numeric_title_candidates(source_title) + extract_structured_numeric_title_candidates(cleaned_title)
    dedup_numeric_titles: List[str] = []
    for candidate in numeric_titles:
        candidate = compact_spaces(candidate)
        if candidate and candidate not in dedup_numeric_titles:
            dedup_numeric_titles.append(candidate)
    aliases = extract_title_aliases_from_text(source_title) + extract_title_aliases_from_text(cleaned_title)
    dedup_aliases: List[str] = []
    for alias in aliases:
        alias = compact_spaces(alias)
        if alias and alias not in dedup_aliases:
            dedup_aliases.append(alias)

    seeds = [
        *dedup_numeric_titles,
        en,
        ru,
        *dedup_aliases,
        clean_release_title(en),
        clean_release_title(ru),
        clean_release_title(cleaned_title or ""),
        clean_release_title(source_title or ""),
    ]

    def add_candidate(value: str) -> None:
        value = compact_spaces(value or "").strip()
        if not value:
            return
        if is_release_group_candidate(value):
            return
        if value not in candidates:
            candidates.append(value)

    for seed in seeds:
        for c in expand_tmdb_candidate_variants(seed):
            add_candidate(c)

    for seed in list(candidates) + seeds:
        for alias in manual_alias_candidates_from_text(seed or ""):
            for c in expand_tmdb_candidate_variants(alias):
                add_candidate(c)

    for seed in list(candidates) + seeds:
        for alias in anime_alias_candidates_from_text(seed or ""):
            for c in expand_tmdb_candidate_variants(alias):
                add_candidate(c)

    return candidates


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


def is_tv_continuation_parent_match(item: Dict[str, Any], details: Dict[str, Any], has_exact_normalized: bool) -> bool:
    if not has_exact_normalized:
        return False
    season_hint = extract_tv_season_hint(item)
    if not season_hint or season_hint < 2:
        return False
    expected_seasons, _ = extract_expected_tv_totals(item)
    reference_season = expected_seasons or season_hint
    if not reference_season or reference_season < 2:
        return False
    try:
        tmdb_seasons_int = int(details.get("tmdb_number_of_seasons")) if details.get("tmdb_number_of_seasons") is not None else None
    except Exception:
        tmdb_seasons_int = None
    if tmdb_seasons_int is None or tmdb_seasons_int < 2:
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

def similarity(a: str, b: str) -> float:
    a = (a or "").lower()
    b = (b or "").lower()
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()


def is_generic_cyrillic_title(value: str) -> bool:
    cleaned = clean_release_title(value or "")
    if not cleaned:
        return False
    if re.search(r"[A-Za-z]", cleaned):
        return False
    if not re.search(r"[А-Яа-яЁё]", cleaned):
        return False
    tokens = text_tokens(cleaned)
    return 1 <= len(tokens) <= 5


def normalize_match_text(text: str) -> str:
    text = compact_spaces(strip_html(text or "")).lower()
    if not text:
        return ""
    translit_map = str.maketrans({
        "ı": "i", "İ": "i", "ş": "s", "Ş": "s", "ğ": "g", "Ğ": "g",
        "ç": "c", "Ç": "c", "ö": "o", "Ö": "o", "ü": "u", "Ü": "u",
        "æ": "ae", "Æ": "ae", "œ": "oe", "Œ": "oe",
    })
    text = text.translate(translit_map)
    text = "".join(ch for ch in unicodedata.normalize("NFKD", text) if not unicodedata.combining(ch))
    text = re.sub(r"[^a-zа-яё0-9]+", " ", text, flags=re.I)
    return compact_spaces(text)


TITLE_STOPWORDS = {
    "the", "a", "an", "and", "of", "to", "in", "on", "for", "at", "by", "with",
    "la", "le", "el", "los", "las", "der", "die", "das", "de", "du",
    "и", "в", "во", "на", "по", "с", "со", "к", "ко", "от", "до", "из", "у", "про",
}


def text_tokens(text: str) -> List[str]:
    tokens = re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", normalize_match_text(text or ""))
    filtered = [t for t in tokens if t not in TITLE_STOPWORDS]
    return filtered or tokens


def raw_text_tokens(text: str) -> List[str]:
    return re.findall(r"[A-Za-zА-Яа-яЁё0-9]+", normalize_match_text(text or ""))


def token_overlap_ratio(a: str, b: str) -> float:
    a_tokens = set(text_tokens(a))
    b_tokens = set(text_tokens(b))
    if not a_tokens or not b_tokens:
        return 0.0
    return len(a_tokens & b_tokens) / max(len(a_tokens), len(b_tokens), 1)


def tmdb_match_looks_valid(item: Dict[str, Any], query: str, details: Dict[str, Any], requested_media_type: str) -> bool:
    source_is_tv = bool(item.get("source_episode_progress")) or str(item.get("media_type") or "") == "tv"
    details_media = str(details.get("media_type") or requested_media_type or "")
    if source_is_tv and details_media == "movie":
        return False

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
                return False
            if best_similarity_norm < 0.985 and not has_substring:
                return False

    if re.search(r"[A-Za-z]", query or ""):
        if source_is_tv and details_media == "tv":
            if best_overlap < 0.18 and best_similarity_norm < 0.50 and best_common_tokens < 2 and not has_substring:
                return False
        else:
            if best_overlap < 0.28 and best_similarity_norm < 0.58 and best_common_tokens < 2 and not has_substring:
                return False

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
            return False
        if best_main_overlap < 0.34 and best_main_similarity < 0.58:
            return False
        if any(marker in source_category_name for marker in ("документ", "спорт", "передачи", "тв-шоу")) and best_main_overlap < 0.60:
            return False
        if expanded_parenthetical_alias and is_generic_cyrillic_title(query or "") and len(text_tokens(query or "")) <= 1:
            return False

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
            return False
    if source_is_tv and details_media == "tv":
        season_hint = extract_tv_season_hint(item)
        later_season_release = bool(season_hint and season_hint >= 2)
        tv_continuation_parent_match = is_tv_continuation_parent_match(item, details, has_exact_normalized)
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
                return False
            if year_delta >= 20 and not tv_continuation_parent_match:
                return False
            if year_delta >= 10 and not has_exact_normalized and not anime_franchise_fallback and not tv_continuation_parent_match:
                return False
            if year_delta >= 8 and best_overlap < 0.70 and best_similarity_norm < 0.84 and not anime_franchise_fallback and not tv_continuation_parent_match:
                return False
            if not later_season_release:
                if short_or_common_query and year_delta >= 3 and not anime_franchise_fallback:
                    return False
                if short_or_common_query and year_delta >= 2 and not has_exact_normalized and not anime_franchise_fallback:
                    return False
            elif short_or_common_query and year_delta >= 6 and not has_exact_normalized and best_overlap < 0.85 and best_similarity_norm < 0.90 and not anime_franchise_fallback and not tv_continuation_parent_match:
                return False

        if expected_seasons and tmdb_seasons_int:
            if expected_seasons >= 2 and tmdb_seasons_int + 1 < expected_seasons and not tv_revival_reset_match:
                return False
            if short_or_common_query and expected_seasons >= 3 and tmdb_seasons_int + 2 < expected_seasons and not has_exact_normalized and not tv_revival_reset_match:
                return False

        single_season_context = (expected_seasons in (None, 1)) and (tmdb_seasons_int in (None, 1))
        if single_season_context and expected_episodes and tmdb_episodes_int:
            if expected_episodes >= 8 and abs(tmdb_episodes_int - expected_episodes) >= max(4, int(expected_episodes * 0.60)):
                return False
            if short_or_common_query and expected_episodes >= 6 and abs(tmdb_episodes_int - expected_episodes) >= max(4, int(expected_episodes * 0.75)) and not has_exact_normalized:
                return False

    if not source_is_tv and details_media == "movie" and source_years and details_year:
        min_delta = year_delta if year_delta is not None else min(abs(details_year - year) for year in source_years)
        if generic_cyrillic_title_risk and min_delta >= 5:
            return False
        if generic_cyrillic_title_risk and min_delta >= 3 and best_overlap < 0.95 and best_similarity_norm < 0.98:
            return False
        if min_delta >= 6 and not has_exact_normalized and best_overlap < 0.72 and best_similarity_norm < 0.84:
            return False
        if min_delta >= 10 and not has_exact_normalized:
            return False
        if min_delta >= 8 and best_overlap < 0.60 and not has_substring:
            return False
        if min_delta >= 20:
            return False

    if source_is_tv and details_media == "tv" and generic_cyrillic_title_risk and year_delta is not None:
        if year_delta >= 6 and not tv_continuation_parent_match:
            return False
        if year_delta >= 4 and best_overlap < 0.90 and best_similarity_norm < 0.96 and not tv_continuation_parent_match:
            return False

    return True

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


def item_content_bucket(item: Dict[str, Any]) -> str:
    manual_bucket = str(item.get("manual_bucket") or "").strip().lower()
    if manual_bucket in {"anime", "dorama", "regular"}:
        return manual_bucket
    category_bucket = source_category_bucket_hint(item.get("source_category_id"), item.get("source_category_name"))
    if category_bucket in {"anime", "dorama"}:
        return category_bucket
    media_type = str(item.get("media_type") or "movie")
    countries = set(effective_item_countries(item))
    genres = {int(g) for g in parse_jsonish_list(item.get("genre_ids")) if str(g).isdigit()}
    is_animation = 16 in genres
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

    if is_animation and strong_anime_signal and anime_score >= 3:
        return "anime"
    if media_type in {"tv", "movie"} and not is_animation:
        if not item.get("tmdb_id") and strong_anime_signal and anime_score >= 3:
            return "anime"
        if asian_dorama_signal_score(item) >= 2:
            return "dorama"
    return "regular"


def subscription_presets() -> Dict[str, Dict[str, Any]]:
    current_year = datetime.now().year
    year_from = current_year - 1
    base_exclude_keywords = "hdr,lossless,mp3,flac,fb2,epub,pdf,mobi"
    return {
        "world": {
            "name": "🌍 Новинки — мир",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 0,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords + ",ру,укр,украин",
                "content_filter": "exclude_anime_dorama",
                "country_codes": "",
                "exclude_country_codes": "TR,RU,UA,JP,KR,CN,TW,TH,HK,ID,MY,SG,PH,VN,LA,KH,MM,BD,PK,LK,NP,MN,KZ,UZ,KG,TJ,TM,AF,IR,IQ,SA,AE,QA,KW,OM,BH,YE,JO,LB,SY,IL,PS,BT,BN,MV",
            },
            "genre_ids": [],
        },
        "turkey": {
            "name": "🇹🇷 Новинки — Турция",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 1,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords,
                "content_filter": "any",
                "country_codes": "TR",
                "exclude_country_codes": "",
            },
            "genre_ids": [],
        },
        "dorama": {
            "name": "🌸 Новинки — дорамы",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 1,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords,
                "content_filter": "only_dorama",
                "country_codes": "",
                "exclude_country_codes": "",
            },
            "genre_ids": [],
        },
        "anime": {
            "name": "🍥 Новинки — аниме",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 1,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords,
                "content_filter": "only_anime",
                "country_codes": "",
                "exclude_country_codes": "",
            },
            "genre_ids": [],
        },
    }


def apply_subscription_preset(sub_id: int, preset_key: str) -> Optional[Dict[str, Any]]:
    spec = subscription_presets().get(preset_key)
    if not spec:
        return None
    fields = dict(spec["fields"])
    fields["name"] = spec["name"]
    fields["preset_key"] = preset_key
    db.update_subscription(sub_id, **fields)
    db.set_subscription_genres(sub_id, spec.get("genre_ids", []))
    return db.get_subscription(sub_id)


PRESET_ROLLOUT_VERSION = "categories_v1"


def detect_subscription_preset_key(sub: Dict[str, Any]) -> str:
    preset_key = compact_spaces(str(sub.get("preset_key") or "")).lower()
    if preset_key in subscription_presets():
        return preset_key
    name_norm = compact_spaces(str(sub.get("name") or "")).casefold()
    if not name_norm:
        return ""
    for key, spec in subscription_presets().items():
        if compact_spaces(spec.get("name") or "").casefold() == name_norm:
            return key
    return ""


class DummyCursor:
    def fetchone(self):
        return None

    def fetchall(self):
        return []


class PGCompatConnection:
    def __init__(self, dsn: str):
        self.raw = connect(dsn, autocommit=True, row_factory=dict_row)

    @staticmethod
    def _sql(sql: str) -> str:
        return sql.replace("?", "%s")

    def execute(self, sql: str, params: Optional[Sequence[Any]] = None):
        cur = self.raw.cursor()
        cur.execute(self._sql(sql), params or ())
        if cur.description is None:
            cur.close()
            return DummyCursor()
        return cur

    def executemany(self, sql: str, seq: Sequence[Sequence[Any]]):
        cur = self.raw.cursor()
        cur.executemany(self._sql(sql), seq)
        cur.close()
        return DummyCursor()

    def executescript(self, script: str) -> None:
        statements = [x.strip() for x in script.split(";") if x.strip()]
        with self.raw.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)

    def commit(self) -> None:
        return None


class DB:
    def __init__(self, dsn: str):
        self.lock = threading.RLock()
        last_error: Optional[Exception] = None
        for attempt in range(1, CFG.startup_db_retries + 1):
            try:
                self.conn = PGCompatConnection(dsn)
                self.init_schema()
                return
            except Exception as exc:
                last_error = exc
                log.warning("Postgres not ready yet (%s/%s): %s", attempt, CFG.startup_db_retries, exc)
                time.sleep(CFG.startup_db_retry_delay)
        raise RuntimeError(f"Не удалось подключиться к Postgres: {last_error}")

    def init_schema(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    tg_user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    access_granted INTEGER NOT NULL DEFAULT 0,
                    access_expires_at BIGINT,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS invites (
                    code TEXT PRIMARY KEY,
                    uses_left INTEGER NOT NULL,
                    expires_at BIGINT,
                    note TEXT,
                    created_by BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL,
                    name TEXT NOT NULL,
                    media_type TEXT NOT NULL DEFAULT 'any',
                    year_from INTEGER,
                    year_to INTEGER,
                    allow_720 INTEGER NOT NULL DEFAULT 0,
                    allow_1080 INTEGER NOT NULL DEFAULT 0,
                    allow_2160 INTEGER NOT NULL DEFAULT 0,
                    min_tmdb_rating DOUBLE PRECISION,
                    include_keywords TEXT NOT NULL DEFAULT '',
                    exclude_keywords TEXT NOT NULL DEFAULT '',
                    content_filter TEXT NOT NULL DEFAULT 'any',
                    country_codes TEXT NOT NULL DEFAULT '',
                    exclude_country_codes TEXT NOT NULL DEFAULT '',
                    preset_key TEXT NOT NULL DEFAULT '',
                    is_enabled INTEGER NOT NULL DEFAULT 1,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    CONSTRAINT fk_subscriptions_user FOREIGN KEY(tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS subscription_genres (
                    subscription_id BIGINT NOT NULL,
                    genre_id INTEGER NOT NULL,
                    PRIMARY KEY (subscription_id, genre_id),
                    CONSTRAINT fk_subscription_genres_sub FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS genres (
                    media_type TEXT NOT NULL,
                    genre_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    PRIMARY KEY (media_type, genre_id)
                );

                CREATE TABLE IF NOT EXISTS items (
                    id BIGSERIAL PRIMARY KEY,
                    source_uid TEXT NOT NULL,
                    version_signature TEXT NOT NULL,
                    source_title TEXT NOT NULL,
                    source_link TEXT,
                    kinozal_id TEXT,
                    source_published_at BIGINT,
                    source_year INTEGER,
                    source_format TEXT,
                    source_description TEXT,
                    source_episode_progress TEXT,
                    source_audio_tracks TEXT,
                    imdb_id TEXT,
                    cleaned_title TEXT,
                    source_category_id TEXT,
                    source_category_name TEXT,
                    media_type TEXT,
                    tmdb_id INTEGER,
                    tmdb_title TEXT,
                    tmdb_original_title TEXT,
                    tmdb_original_language TEXT,
                    tmdb_rating DOUBLE PRECISION,
                    tmdb_vote_count INTEGER,
                    tmdb_release_date TEXT,
                    tmdb_overview TEXT,
                    tmdb_poster_url TEXT,
                    tmdb_status TEXT,
                    tmdb_age_rating TEXT,
                    tmdb_countries TEXT,
                    tmdb_number_of_seasons INTEGER,
                    tmdb_number_of_episodes INTEGER,
                    tmdb_next_episode_name TEXT,
                    tmdb_next_episode_air_date TEXT,
                    tmdb_next_episode_season_number INTEGER,
                    tmdb_next_episode_episode_number INTEGER,
                    tmdb_last_episode_name TEXT,
                    tmdb_last_episode_air_date TEXT,
                    tmdb_last_episode_season_number INTEGER,
                    tmdb_last_episode_episode_number INTEGER,
                    manual_bucket TEXT NOT NULL DEFAULT '',
                    manual_country_codes TEXT NOT NULL DEFAULT '',
                    raw_json JSONB NOT NULL,
                    created_at BIGINT NOT NULL,
                    UNIQUE(source_uid, version_signature)
                );

                CREATE TABLE IF NOT EXISTS items_archive (
                    archive_id BIGSERIAL PRIMARY KEY,
                    original_item_id BIGINT NOT NULL,
                    kinozal_id TEXT,
                    source_uid TEXT NOT NULL,
                    version_signature TEXT NOT NULL,
                    source_title TEXT NOT NULL,
                    source_link TEXT,
                    media_type TEXT,
                    source_published_at BIGINT,
                    source_year INTEGER,
                    source_format TEXT,
                    source_description TEXT,
                    source_episode_progress TEXT,
                    source_audio_tracks TEXT,
                    imdb_id TEXT,
                    cleaned_title TEXT,
                    source_category_id TEXT,
                    source_category_name TEXT,
                    tmdb_id INTEGER,
                    tmdb_title TEXT,
                    tmdb_original_title TEXT,
                    tmdb_original_language TEXT,
                    tmdb_rating DOUBLE PRECISION,
                    tmdb_vote_count INTEGER,
                    tmdb_release_date TEXT,
                    tmdb_status TEXT,
                    tmdb_countries TEXT,
                    manual_bucket TEXT NOT NULL DEFAULT '',
                    manual_country_codes TEXT NOT NULL DEFAULT '',
                    genre_ids TEXT,
                    item_json JSONB NOT NULL,
                    original_created_at BIGINT,
                    archived_at BIGINT NOT NULL,
                    archive_reason TEXT NOT NULL,
                    merged_into_item_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS deliveries_archive (
                    archive_id BIGSERIAL PRIMARY KEY,
                    original_delivery_id BIGINT,
                    tg_user_id BIGINT NOT NULL,
                    original_item_id BIGINT NOT NULL,
                    kinozal_id TEXT,
                    source_uid TEXT,
                    media_type TEXT,
                    version_signature TEXT,
                    source_title TEXT,
                    subscription_id BIGINT,
                    matched_subscription_ids TEXT,
                    delivered_at BIGINT NOT NULL,
                    archived_at BIGINT NOT NULL,
                    archive_reason TEXT NOT NULL,
                    merged_into_item_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS item_genres (
                    item_id BIGINT NOT NULL,
                    genre_id INTEGER NOT NULL,
                    PRIMARY KEY (item_id, genre_id),
                    CONSTRAINT fk_item_genres_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS deliveries (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL,
                    item_id BIGINT NOT NULL,
                    subscription_id BIGINT,
                    delivered_at BIGINT NOT NULL,
                    UNIQUE(tg_user_id, item_id),
                    CONSTRAINT fk_deliveries_user FOREIGN KEY(tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE,
                    CONSTRAINT fk_deliveries_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
                    CONSTRAINT fk_deliveries_sub FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_items_created_at ON items(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_items_source_uid ON items(source_uid);
                CREATE INDEX IF NOT EXISTS idx_deliveries_user_item ON deliveries(tg_user_id, item_id);

                ALTER TABLE users ADD COLUMN IF NOT EXISTS access_expires_at BIGINT;
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS country_codes TEXT NOT NULL DEFAULT '';
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS exclude_country_codes TEXT NOT NULL DEFAULT '';
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS content_filter TEXT NOT NULL DEFAULT 'any';
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS preset_key TEXT NOT NULL DEFAULT '';
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_countries TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS source_category_id TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS source_category_name TEXT;
                ALTER TABLE items_archive ADD COLUMN IF NOT EXISTS source_category_id TEXT;
                ALTER TABLE items_archive ADD COLUMN IF NOT EXISTS source_category_name TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS kinozal_id TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_original_language TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_vote_count INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_status TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_age_rating TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_number_of_seasons INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_number_of_episodes INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_name TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_air_date TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_season_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_episode_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_name TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_air_date TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_season_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_episode_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS manual_bucket TEXT NOT NULL DEFAULT '';
                ALTER TABLE items ADD COLUMN IF NOT EXISTS manual_country_codes TEXT NOT NULL DEFAULT '';
                ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS matched_subscription_ids TEXT;
                CREATE INDEX IF NOT EXISTS idx_users_access_state ON users(access_granted, access_expires_at);
                CREATE INDEX IF NOT EXISTS idx_items_source_link ON items(source_link);
                CREATE INDEX IF NOT EXISTS idx_items_media_source ON items(media_type, source_uid);

                UPDATE items
                SET kinozal_id = regexp_replace(source_uid, '^kinozal:([0-9]+)$', '\1')
                WHERE (kinozal_id IS NULL OR kinozal_id = '')
                  AND source_uid ~ '^kinozal:[0-9]+$';

                UPDATE items
                SET kinozal_id = regexp_replace(source_uid, '^.*details\\.php\\?id=([0-9]+).*$','\1')
                WHERE (kinozal_id IS NULL OR kinozal_id = '')
                  AND source_uid ~ 'details\\.php\\?id=[0-9]+';

                UPDATE items
                SET kinozal_id = regexp_replace(source_link, '^.*details\\.php\\?id=([0-9]+).*$','\1')
                WHERE (kinozal_id IS NULL OR kinozal_id = '')
                  AND COALESCE(source_link, '') ~ 'details\\.php\\?id=[0-9]+';

                CREATE INDEX IF NOT EXISTS idx_items_kinozal_id ON items(kinozal_id);
                CREATE INDEX IF NOT EXISTS idx_items_archive_kinozal_id ON items_archive(kinozal_id, archived_at DESC);
                CREATE INDEX IF NOT EXISTS idx_deliveries_archive_user_kinozal ON deliveries_archive(tg_user_id, kinozal_id, delivered_at DESC);
                """
            )

    def row_to_dict(self, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        return dict(row) if row else None

    def ensure_user(self, tg_user_id: int, username: str, first_name: str, auto_grant: bool = False) -> Dict[str, Any]:
        ts = utc_ts()
        with self.lock:
            existing = self.conn.execute(
                "SELECT * FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            if existing:
                access_granted = int(existing["access_granted"])
                if auto_grant and not access_granted:
                    access_granted = 1
                self.conn.execute(
                    """
                    UPDATE users
                    SET username = ?, first_name = ?, access_granted = ?, updated_at = ?
                    WHERE tg_user_id = ?
                    """,
                    (username, first_name, access_granted, ts, tg_user_id),
                )
            else:
                self.conn.execute(
                    """
                    INSERT INTO users(tg_user_id, username, first_name, access_granted, access_expires_at, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tg_user_id, username, first_name, 1 if auto_grant else 0, None, ts, ts),
                )
            self.conn.commit()
            return self.get_user(tg_user_id)

    def get_user(self, tg_user_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            return self.row_to_dict(row)

    def user_has_access(self, tg_user_id: int) -> bool:
        if tg_user_id in CFG.admin_ids:
            return True
        user = self.get_user(tg_user_id)
        if not user or not user["is_active"] or not user["access_granted"]:
            return False
        access_expires_at = user.get("access_expires_at")
        if access_expires_at is not None and int(access_expires_at) <= utc_ts():
            return False
        return True

    def set_user_access(self, tg_user_id: int, value: bool, access_expires_at: Any = ACCESS_EXPIRY_UNSET) -> None:
        ts = utc_ts()
        with self.lock:
            assignments = ["access_granted = ?", "updated_at = ?"]
            params: List[Any] = [1 if value else 0, ts]
            if access_expires_at is not ACCESS_EXPIRY_UNSET:
                assignments.append("access_expires_at = ?")
                params.append(int(access_expires_at) if access_expires_at is not None else None)
            params.append(tg_user_id)
            self.conn.execute(
                f"UPDATE users SET {', '.join(assignments)} WHERE tg_user_id = ?",
                tuple(params),
            )
            self.conn.commit()

    def extend_user_access_days(self, tg_user_id: int, days: int) -> Optional[Dict[str, Any]]:
        days = int(days)
        if days <= 0:
            return self.get_user(tg_user_id)
        user = self.get_user(tg_user_id)
        base_ts = utc_ts()
        if user and user.get("access_expires_at") and int(user["access_expires_at"]) > base_ts:
            base_ts = int(user["access_expires_at"])
        expires_at = base_ts + days * 86400
        self.set_user_access(tg_user_id, True, access_expires_at=expires_at)
        return self.get_user(tg_user_id)

    def list_users_with_stats(self, limit: int = 25, offset: int = 0) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    u.*,
                    COUNT(s.id) AS subscriptions_total,
                    SUM(CASE WHEN s.is_enabled = 1 THEN 1 ELSE 0 END) AS subscriptions_enabled
                FROM users u
                LEFT JOIN subscriptions s ON s.tg_user_id = u.tg_user_id
                GROUP BY u.tg_user_id, u.username, u.first_name, u.is_active, u.access_granted, u.access_expires_at, u.created_at, u.updated_at
                ORDER BY u.created_at DESC, u.tg_user_id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            return [dict(x) for x in rows]

    def count_users(self) -> int:
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
            return int((row or {}).get("cnt") or 0)


    def list_broadcast_user_ids(self, active_only: bool = True, include_admins: bool = False) -> List[int]:
        conditions = []
        params: List[Any] = []
        if active_only:
            conditions.extend([
                "is_active = 1",
                "access_granted = 1",
                "(access_expires_at IS NULL OR access_expires_at > ?)",
            ])
            params.append(utc_ts())
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        with self.lock:
            rows = self.conn.execute(
                f"SELECT tg_user_id FROM users{where_sql} ORDER BY tg_user_id ASC",
                tuple(params),
            ).fetchall()
        user_ids = [int(row["tg_user_id"]) for row in rows]
        if include_admins:
            merged = {int(x) for x in user_ids}
            merged.update(int(x) for x in CFG.admin_ids)
            return sorted(merged)
        admin_ids = {int(x) for x in CFG.admin_ids}
        return [user_id for user_id in user_ids if user_id not in admin_ids]

    def get_user_with_subscriptions(self, tg_user_id: int) -> Optional[Dict[str, Any]]:
        user = self.get_user(tg_user_id)
        if not user:
            return None
        user["subscriptions"] = self.list_user_subscriptions(tg_user_id)
        return user

    def create_invite(self, created_by: int, uses_left: int, expires_days: int, note: str) -> Dict[str, Any]:
        code = "".join(random.choices(string.ascii_letters + string.digits, k=24))
        expires_at = utc_ts() + expires_days * 86400 if expires_days > 0 else None
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO invites(code, uses_left, expires_at, note, created_by, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (code, uses_left, expires_at, note, created_by, ts),
            )
            self.conn.commit()
        return self.get_invite(code)

    def get_invite(self, code: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute("SELECT * FROM invites WHERE code = ?", (code,)).fetchone()
            return self.row_to_dict(row)

    def redeem_invite(self, code: str, tg_user_id: int) -> bool:
        ts = utc_ts()
        with self.lock:
            invite = self.conn.execute("SELECT * FROM invites WHERE code = ?", (code,)).fetchone()
            if not invite:
                return False
            if invite["expires_at"] and ts > int(invite["expires_at"]):
                return False
            if int(invite["uses_left"]) <= 0:
                return False
            self.conn.execute(
                "UPDATE invites SET uses_left = uses_left - 1 WHERE code = ?",
                (code,),
            )
            self.conn.execute(
                """
                UPDATE users
                SET access_granted = 1, access_expires_at = NULL, updated_at = ?
                WHERE tg_user_id = ?
                """,
                (ts, tg_user_id),
            )
            self.conn.commit()
            return True

    def list_invites(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM invites
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(x) for x in rows]

    def create_subscription(self, tg_user_id: int, name: Optional[str] = None) -> Dict[str, Any]:
        ts = utc_ts()
        if not name:
            count = len(self.list_user_subscriptions(tg_user_id)) + 1
            name = f"Подписка {count}"
        with self.lock:
            row = self.conn.execute(
                """
                INSERT INTO subscriptions(
                    tg_user_id, name, media_type, allow_1080, is_enabled, created_at, updated_at
                )
                VALUES(?, ?, 'any', 1, 1, ?, ?)
                RETURNING id
                """,
                (tg_user_id, name, ts, ts),
            ).fetchone()
            sub_id = int(row["id"])
            self.conn.commit()
        return self.get_subscription(sub_id)

    def list_user_subscriptions(self, tg_user_id: int) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM subscriptions
                WHERE tg_user_id = ?
                ORDER BY id DESC
                """,
                (tg_user_id,),
            ).fetchall()
            return [dict(x) for x in rows]

    def list_enabled_subscriptions(self) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT s.*
                FROM subscriptions s
                JOIN users u ON u.tg_user_id = s.tg_user_id
                WHERE s.is_enabled = 1
                  AND u.is_active = 1
                  AND u.access_granted = 1
                  AND (u.access_expires_at IS NULL OR u.access_expires_at > ?)
                ORDER BY s.id ASC
                """,
                (utc_ts(),),
            ).fetchall()
            return [dict(x) for x in rows]

    def rollout_existing_preset_subscriptions(self, rollout_version: str) -> int:
        rollout_version = compact_spaces(str(rollout_version or ""))
        if not rollout_version:
            return 0
        meta_key = "preset_rollout_version"
        if self.get_meta(meta_key) == rollout_version:
            return 0

        updated = 0
        rows = self.conn.execute("SELECT * FROM subscriptions ORDER BY id ASC").fetchall()
        for row in rows:
            sub = dict(row)
            preset_key = detect_subscription_preset_key(sub)
            spec = subscription_presets().get(preset_key) if preset_key else None
            if not spec:
                continue
            fields = dict(spec["fields"])
            fields["name"] = spec["name"]
            fields["preset_key"] = preset_key
            self.update_subscription(int(sub["id"]), **fields)
            self.set_subscription_genres(int(sub["id"]), spec.get("genre_ids", []))
            updated += 1

        self.set_meta(meta_key, rollout_version)
        return updated

    def get_subscription(self, sub_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            data["genre_ids"] = self.get_subscription_genres(sub_id)
            data["country_codes_list"] = self.get_subscription_country_codes(sub_id)
            data["exclude_country_codes_list"] = self.get_subscription_exclude_country_codes(sub_id)
            return data

    def subscription_belongs_to(self, sub_id: int, tg_user_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ? AND tg_user_id = ?",
                (sub_id, tg_user_id),
            ).fetchone()
            return row is not None

    def update_subscription(self, sub_id: int, **fields: Any) -> None:
        if not fields:
            return
        allowed = {
            "name",
            "media_type",
            "year_from",
            "year_to",
            "allow_720",
            "allow_1080",
            "allow_2160",
            "min_tmdb_rating",
            "include_keywords",
            "exclude_keywords",
            "content_filter",
            "country_codes",
            "exclude_country_codes",
            "preset_key",
            "is_enabled",
        }
        cleaned = {k: v for k, v in fields.items() if k in allowed}
        if not cleaned:
            return
        cleaned["updated_at"] = utc_ts()
        assignments = ", ".join(f"{k} = ?" for k in cleaned.keys())
        values = list(cleaned.values()) + [sub_id]
        with self.lock:
            self.conn.execute(
                f"UPDATE subscriptions SET {assignments} WHERE id = ?",
                values,
            )
            self.conn.commit()

    def delete_subscription(self, sub_id: int) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
            self.conn.commit()

    def get_subscription_genres(self, sub_id: int) -> List[int]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT genre_id FROM subscription_genres WHERE subscription_id = ? ORDER BY genre_id",
                (sub_id,),
            ).fetchall()
            return [int(row["genre_id"]) for row in rows]

    def set_subscription_genres(self, sub_id: int, genre_ids: Iterable[int]) -> None:
        genre_ids = sorted({int(g) for g in genre_ids})
        with self.lock:
            self.conn.execute("DELETE FROM subscription_genres WHERE subscription_id = ?", (sub_id,))
            if genre_ids:
                self.conn.executemany(
                    "INSERT INTO subscription_genres(subscription_id, genre_id) VALUES(?, ?)",
                    [(sub_id, gid) for gid in genre_ids],
                )
            self.conn.commit()

    def toggle_subscription_genre(self, sub_id: int, genre_id: int) -> None:
        current = set(self.get_subscription_genres(sub_id))
        if genre_id in current:
            current.remove(genre_id)
        else:
            current.add(genre_id)
        self.set_subscription_genres(sub_id, current)

    def get_subscription_country_codes(self, sub_id: int) -> List[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT country_codes FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            return parse_country_codes(row["country_codes"] if row else "")

    def set_subscription_country_codes(self, sub_id: int, country_codes: Iterable[str]) -> None:
        normalized = ",".join(parse_country_codes(list(country_codes)))
        self.update_subscription(sub_id, country_codes=normalized)

    def toggle_subscription_country_code(self, sub_id: int, country_code: str) -> None:
        current = set(self.get_subscription_country_codes(sub_id))
        code = compact_spaces(str(country_code or "")).upper()
        if not code:
            return
        if code in current:
            current.remove(code)
        else:
            current.add(code)
        self.set_subscription_country_codes(sub_id, sorted(current))

    def get_subscription_exclude_country_codes(self, sub_id: int) -> List[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT exclude_country_codes FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            return parse_country_codes(row["exclude_country_codes"] if row else "")

    def set_subscription_exclude_country_codes(self, sub_id: int, country_codes: Iterable[str]) -> None:
        normalized = ",".join(parse_country_codes(list(country_codes)))
        self.update_subscription(sub_id, exclude_country_codes=normalized)

    def toggle_subscription_exclude_country_code(self, sub_id: int, country_code: str) -> None:
        current = set(self.get_subscription_exclude_country_codes(sub_id))
        code = compact_spaces(str(country_code or "")).upper()
        if not code:
            return
        if code in current:
            current.remove(code)
        else:
            current.add(code)
        self.set_subscription_exclude_country_codes(sub_id, sorted(current))

    def get_known_country_codes(self) -> List[str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT tmdb_countries FROM items WHERE tmdb_countries IS NOT NULL AND tmdb_countries <> ''"
            ).fetchall()

        codes = set(COUNTRY_NAMES_RU.keys())
        for row in rows:
            codes.update(parse_country_codes(row["tmdb_countries"]))

        if pycountry is not None:
            try:
                codes.update(str(country.alpha_2).upper() for country in pycountry.countries if getattr(country, "alpha_2", None))
            except Exception:
                pass

        return sorted(codes, key=lambda code: country_name_ru(code).lower())

    def upsert_genres(self, media_type: str, genre_map: Dict[int, str]) -> None:
        ts = utc_ts()
        with self.lock:
            for genre_id, name in genre_map.items():
                self.conn.execute(
                    """
                    INSERT INTO genres(media_type, genre_id, name, updated_at)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(media_type, genre_id) DO UPDATE SET
                        name = excluded.name,
                        updated_at = excluded.updated_at
                    """,
                    (media_type, int(genre_id), name, ts),
                )
            self.conn.commit()

    def get_genres(self, media_type: str) -> Dict[int, str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT genre_id, name FROM genres WHERE media_type = ? ORDER BY name ASC",
                (media_type,),
            ).fetchall()
            return {int(row["genre_id"]): row["name"] for row in rows}

    def get_all_genres_merged(self) -> Dict[int, str]:
        result: Dict[int, str] = {}
        for media_type in ("movie", "tv"):
            result.update(self.get_genres(media_type))
        return dict(sorted(result.items(), key=lambda x: x[1].lower()))

    def get_meta(self, key: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO meta(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, ts),
            )
            self.conn.commit()

    def _find_existing_item_for_upsert(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM items WHERE source_uid = ? AND version_signature = ?",
            (data["source_uid"], data["version_signature"]),
        ).fetchone()
        if row:
            return row

        kinozal_id = compact_spaces(str(data.get("kinozal_id") or "")) or extract_kinozal_id(data.get("source_uid")) or extract_kinozal_id(data.get("source_link"))
        target_variant_sig = build_item_variant_signature(data)

        if kinozal_id:
            source_uid_norm = f"kinozal:{kinozal_id}"
            like_pattern = f"%details.php?id={kinozal_id}%"
            rows = self.conn.execute(
                """
                SELECT * FROM items
                WHERE source_uid = ? OR source_uid LIKE ? OR source_link LIKE ?
                ORDER BY CASE WHEN source_uid = ? THEN 0 ELSE 1 END, created_at DESC
                """,
                (source_uid_norm, like_pattern, like_pattern, source_uid_norm),
            ).fetchall()
            for candidate in rows:
                if build_item_variant_signature(dict(candidate)) == target_variant_sig:
                    return candidate
            return None

        source_uid = compact_spaces(str(data.get("source_uid") or ""))
        if not source_uid:
            return None
        rows = self.conn.execute(
            """
            SELECT * FROM items
            WHERE source_uid = ?
            ORDER BY created_at DESC
            """,
            (source_uid,),
        ).fetchall()
        for candidate in rows:
            if build_item_variant_signature(dict(candidate)) == target_variant_sig:
                return candidate
        return None

    def delivered_equivalent(self, tg_user_id: int, item: Dict[str, Any]) -> bool:
        media_type = str(item.get("media_type") or "movie")
        target_variant_sig = build_item_variant_signature(item)
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
        with self.lock:
            if kinozal_id:
                source_uid_norm = f"kinozal:{kinozal_id}"
                like_pattern = f"%details.php?id={kinozal_id}%"
                rows = self.conn.execute(
                    """
                    SELECT i.*
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE d.tg_user_id = ?
                      AND (i.source_uid = ? OR i.source_uid LIKE ? OR i.source_link LIKE ?)
                    ORDER BY d.delivered_at DESC, i.id DESC
                    """,
                    (tg_user_id, source_uid_norm, like_pattern, like_pattern),
                ).fetchall()
                for row in rows:
                    if build_item_variant_signature(dict(row)) == target_variant_sig:
                        return True
                row = self.conn.execute(
                    """
                    SELECT 1
                    FROM deliveries_archive
                    WHERE tg_user_id = ?
                      AND kinozal_id = ?
                      AND COALESCE(version_signature, '') = ?
                    LIMIT 1
                    """,
                    (tg_user_id, kinozal_id, compact_spaces(str(item.get("version_signature") or ""))),
                ).fetchone()
                return row is not None

            source_uid = compact_spaces(str(item.get("source_uid") or ""))
            if not source_uid:
                return False
            rows = self.conn.execute(
                """
                SELECT i.*
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ?
                  AND i.source_uid = ?
                ORDER BY d.delivered_at DESC, i.id DESC
                """,
                (tg_user_id, source_uid),
            ).fetchall()
            for row in rows:
                if build_item_variant_signature(dict(row)) == target_variant_sig:
                    return True
            row = self.conn.execute(
                """
                SELECT 1
                FROM deliveries_archive
                WHERE tg_user_id = ?
                  AND source_uid = ?
                  AND COALESCE(version_signature, '') = ?
                LIMIT 1
                """,
                (tg_user_id, source_uid, compact_spaces(str(item.get("version_signature") or ""))),
            ).fetchone()
            return row is not None

    def get_latest_delivered_related_item(self, tg_user_id: int, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        media_type = str(item.get("media_type") or "movie")
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
        with self.lock:
            if kinozal_id:
                source_uid_norm = f"kinozal:{kinozal_id}"
                like_pattern = f"%details.php?id={kinozal_id}%"
                row = self.conn.execute(
                    """
                    SELECT i.*
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE d.tg_user_id = ?
                      AND (i.source_uid = ? OR i.source_uid LIKE ? OR i.source_link LIKE ?)
                    ORDER BY d.delivered_at DESC, i.id DESC
                    LIMIT 1
                    """,
                    (tg_user_id, source_uid_norm, like_pattern, like_pattern),
                ).fetchone()
                if row:
                    return dict(row)
                archived = self.conn.execute(
                    """
                    SELECT ia.item_json
                    FROM deliveries_archive da
                    JOIN items_archive ia ON ia.original_item_id = da.original_item_id
                    WHERE da.tg_user_id = ?
                      AND da.kinozal_id = ?
                    ORDER BY da.delivered_at DESC, ia.archived_at DESC, ia.archive_id DESC
                    LIMIT 1
                    """,
                    (tg_user_id, kinozal_id),
                ).fetchone()
                if archived and archived.get("item_json"):
                    try:
                        return json.loads(archived["item_json"])
                    except Exception:
                        pass
                return None

            source_uid = compact_spaces(str(item.get("source_uid") or ""))
            if not source_uid:
                return None
            row = self.conn.execute(
                """
                SELECT i.*
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ?
                  AND i.source_uid = ?
                ORDER BY d.delivered_at DESC, i.id DESC
                LIMIT 1
                """,
                (tg_user_id, source_uid),
            ).fetchone()
            if row:
                return dict(row)
            archived = self.conn.execute(
                """
                SELECT ia.item_json
                FROM deliveries_archive da
                JOIN items_archive ia ON ia.original_item_id = da.original_item_id
                WHERE da.tg_user_id = ?
                  AND da.source_uid = ?
                ORDER BY da.delivered_at DESC, ia.archived_at DESC, ia.archive_id DESC
                LIMIT 1
                """,
                (tg_user_id, source_uid),
            ).fetchone()
            if archived and archived.get("item_json"):
                try:
                    return json.loads(archived["item_json"])
                except Exception:
                    pass
            return None

    def save_item(self, item: Dict[str, Any]) -> Tuple[int, bool, bool]:
        data = {
            "source_uid": item["source_uid"],
            "version_signature": item["version_signature"],
            "source_title": item["source_title"],
            "source_link": item.get("source_link"),
            "kinozal_id": resolve_item_kinozal_id(item),
            "source_published_at": item.get("source_published_at"),
            "source_year": item.get("source_year"),
            "source_format": item.get("source_format"),
            "source_description": item.get("source_description"),
            "source_episode_progress": item.get("source_episode_progress"),
            "source_audio_tracks": json.dumps(item.get("source_audio_tracks", []), ensure_ascii=False),
            "imdb_id": item.get("imdb_id"),
            "cleaned_title": item.get("cleaned_title"),
            "source_category_id": item.get("source_category_id"),
            "source_category_name": item.get("source_category_name"),
            "media_type": item.get("media_type"),
            "tmdb_id": item.get("tmdb_id"),
            "tmdb_title": item.get("tmdb_title"),
            "tmdb_original_title": item.get("tmdb_original_title"),
            "tmdb_original_language": item.get("tmdb_original_language"),
            "tmdb_rating": item.get("tmdb_rating"),
            "tmdb_vote_count": item.get("tmdb_vote_count"),
            "tmdb_release_date": item.get("tmdb_release_date"),
            "tmdb_overview": item.get("tmdb_overview"),
            "tmdb_poster_url": item.get("tmdb_poster_url"),
            "tmdb_status": item.get("tmdb_status"),
            "tmdb_age_rating": item.get("tmdb_age_rating"),
            "tmdb_countries": json.dumps(parse_country_codes(item.get("tmdb_countries", [])), ensure_ascii=False),
            "tmdb_number_of_seasons": item.get("tmdb_number_of_seasons"),
            "tmdb_number_of_episodes": item.get("tmdb_number_of_episodes"),
            "tmdb_next_episode_name": item.get("tmdb_next_episode_name"),
            "tmdb_next_episode_air_date": item.get("tmdb_next_episode_air_date"),
            "tmdb_next_episode_season_number": item.get("tmdb_next_episode_season_number"),
            "tmdb_next_episode_episode_number": item.get("tmdb_next_episode_episode_number"),
            "tmdb_last_episode_name": item.get("tmdb_last_episode_name"),
            "tmdb_last_episode_air_date": item.get("tmdb_last_episode_air_date"),
            "tmdb_last_episode_season_number": item.get("tmdb_last_episode_season_number"),
            "tmdb_last_episode_episode_number": item.get("tmdb_last_episode_episode_number"),
            "manual_bucket": item.get("manual_bucket") or "",
            "manual_country_codes": ",".join(parse_country_codes(item.get("manual_country_codes"))) if item.get("manual_country_codes") is not None else "",
            "raw_json": json.dumps(item.get("raw_json", {}), ensure_ascii=False, sort_keys=True),
            "created_at": utc_ts(),
        }

        def pick_value(new_value: Any, old_value: Any) -> Any:
            if new_value is None:
                return old_value
            if isinstance(new_value, str):
                return new_value if compact_spaces(new_value) else old_value
            if isinstance(new_value, (list, tuple, set, dict)):
                return new_value if new_value else old_value
            return new_value

        with self.lock:
            existing = self._find_existing_item_for_upsert(data)
            if existing:
                existing_id = int(existing["id"])
                existing_data = dict(existing)
                merged = dict(data)
                for key in data:
                    if key == "created_at":
                        merged[key] = existing_data.get(key, data[key])
                        continue
                    merged[key] = pick_value(data.get(key), existing_data.get(key))

                fields_to_update = [
                    "source_uid",
                    "version_signature",
                    "source_title",
                    "source_link",
                    "kinozal_id",
                    "source_published_at",
                    "source_year",
                    "source_format",
                    "source_description",
                    "source_episode_progress",
                    "source_audio_tracks",
                    "imdb_id",
                    "cleaned_title",
                    "source_category_id",
                    "source_category_name",
                    "media_type",
                    "tmdb_id",
                    "tmdb_title",
                    "tmdb_original_title",
                    "tmdb_original_language",
                    "tmdb_rating",
                    "tmdb_vote_count",
                    "tmdb_release_date",
                    "tmdb_overview",
                    "tmdb_poster_url",
                    "tmdb_status",
                    "tmdb_age_rating",
                    "tmdb_countries",
                    "tmdb_number_of_seasons",
                    "tmdb_number_of_episodes",
                    "tmdb_next_episode_name",
                    "tmdb_next_episode_air_date",
                    "tmdb_next_episode_season_number",
                    "tmdb_next_episode_episode_number",
                    "tmdb_last_episode_name",
                    "tmdb_last_episode_air_date",
                    "tmdb_last_episode_season_number",
                    "tmdb_last_episode_episode_number",
                    "manual_bucket",
                    "manual_country_codes",
                    "raw_json",
                ]
                values = [merged[field] for field in fields_to_update]
                values.append(existing_id)

                current_genres = [
                    int(row["genre_id"])
                    for row in self.conn.execute(
                        "SELECT genre_id FROM item_genres WHERE item_id = ? ORDER BY genre_id",
                        (existing_id,),
                    ).fetchall()
                ]
                incoming_genres = sorted({int(x) for x in item.get("genre_ids", [])})
                final_genres = incoming_genres or current_genres

                materially_changed = False
                for field in fields_to_update:
                    if field in {"source_uid", "version_signature"}:
                        continue
                    old_value = existing_data.get(field)
                    new_value = merged.get(field)
                    if old_value != new_value:
                        materially_changed = True
                        break
                if current_genres != final_genres:
                    materially_changed = True

                self.conn.execute(
                    f"UPDATE items SET {', '.join(f'{field} = ?' for field in fields_to_update)} WHERE id = ?",
                    values,
                )

                self.conn.execute("DELETE FROM item_genres WHERE item_id = ?", (existing_id,))
                if final_genres:
                    self.conn.executemany(
                        "INSERT INTO item_genres(item_id, genre_id) VALUES(?, ?)",
                        [(existing_id, gid) for gid in final_genres],
                    )
                self.conn.commit()
                return existing_id, False, materially_changed

            fields = ", ".join(data.keys())
            marks = ", ".join("?" for _ in data)
            row = self.conn.execute(
                f"INSERT INTO items({fields}) VALUES({marks}) RETURNING id",
                list(data.values()),
            ).fetchone()
            item_id = int(row["id"])

            genre_ids = sorted({int(x) for x in item.get("genre_ids", [])})
            if genre_ids:
                self.conn.executemany(
                    "INSERT INTO item_genres(item_id, genre_id) VALUES(?, ?)",
                    [(item_id, gid) for gid in genre_ids],
                )
            self.conn.commit()
            return item_id, True, True

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE id = ?",
                (item_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            genres = self.conn.execute(
                "SELECT genre_id FROM item_genres WHERE item_id = ? ORDER BY genre_id",
                (item_id,),
            ).fetchall()
            data["genre_ids"] = [int(x["genre_id"]) for x in genres]
            data["tmdb_countries"] = parse_country_codes(data.get("tmdb_countries"))
            data["manual_country_codes"] = parse_country_codes(data.get("manual_country_codes"))
            if not compact_spaces(str(data.get("kinozal_id") or "")):
                data["kinozal_id"] = resolve_item_kinozal_id(data)
            return data

    def find_item_by_kinozal_id(self, kinozal_id: str) -> Optional[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return None
        with self.lock:
            row = self.conn.execute(
                """
                SELECT id FROM items
                WHERE kinozal_id = ? OR source_uid = ? OR source_uid LIKE ? OR source_link LIKE ?
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                LIMIT 1
                """,
                (kinozal_id, f"kinozal:{kinozal_id}", f"%details.php?id={kinozal_id}%", f"%id={kinozal_id}%"),
            ).fetchone()
            return self.get_item(int(row["id"])) if row else None

    def list_items_for_rematch(self, limit: int = 50, only_unmatched: bool = True) -> List[Dict[str, Any]]:
        fetch_limit = max(1, min(int(limit or 50), 500))
        where_clause = "WHERE COALESCE(kinozal_id, '') <> ''"
        if only_unmatched:
            where_clause += " AND tmdb_id IS NULL"
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT id FROM items
                {where_clause}
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                LIMIT ?
                """,
                (fetch_limit,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = self.get_item(int(row["id"]))
            if item:
                result.append(item)
        return result

    def set_item_manual_routing(self, item_id: int, bucket: str = "", country_codes: Optional[Iterable[str]] = None) -> None:
        bucket_norm = str(bucket or "").strip().lower()
        if bucket_norm not in {"", "anime", "dorama", "regular"}:
            bucket_norm = ""
        countries_norm = ",".join(parse_country_codes(country_codes or []))
        with self.lock:
            self.conn.execute(
                "UPDATE items SET manual_bucket = ?, manual_country_codes = ? WHERE id = ?",
                (bucket_norm, countries_norm, int(item_id)),
            )
            self.conn.commit()

    def get_last_items(self, limit: int = 10) -> List[Dict[str, Any]]:
        fetch_limit = max(limit * 50, 200)
        with self.lock:
            rows = self.conn.execute(
                "SELECT id FROM items ORDER BY COALESCE(source_published_at, 0) DESC, id DESC LIMIT ?",
                (fetch_limit,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            item = self.get_item(int(row["id"]))
            if not item:
                continue
            kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
            key = kinozal_id or compact_spaces(str(item.get("source_uid") or "")) or f"item:{int(item.get('id') or 0)}"
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
            if len(result) >= limit:
                break
        return result

    def get_last_items_for_subscription(self, sub_id: int, limit: int = 5) -> List[Dict[str, Any]]:
        items = self.get_last_items(limit=max(100, limit * 30))
        sub = self.get_subscription(sub_id)
        if not sub:
            return []
        matched = [item for item in items if item and match_subscription(sub, item)]
        matched.sort(key=lambda item: (int(item.get("source_published_at") or 0), int(item.get("id") or 0)), reverse=True)
        return matched[:limit]

    def _archive_item_locked(self, item: Dict[str, Any], reason: str, merged_into_item_id: Optional[int] = None) -> bool:
        item_id = int(item["id"])
        existing = self.conn.execute("SELECT 1 FROM items WHERE id = ?", (item_id,)).fetchone()
        if not existing:
            return False
        full_item = self.get_item(item_id) or dict(item)
        kinozal_id = compact_spaces(str(full_item.get("kinozal_id") or "")) or resolve_item_kinozal_id(full_item)
        genre_ids = sorted({int(x) for x in full_item.get("genre_ids", [])})
        archived_at = utc_ts()
        self.conn.execute(
            """
            INSERT INTO items_archive(
                original_item_id, kinozal_id, source_uid, version_signature, source_title, source_link, media_type,
                source_published_at, source_year, source_format, source_description, source_episode_progress,
                source_audio_tracks, imdb_id, cleaned_title, source_category_id, source_category_name, tmdb_id, tmdb_title, tmdb_original_title,
                tmdb_original_language, tmdb_rating, tmdb_vote_count, tmdb_release_date, tmdb_status, tmdb_countries,
                manual_bucket, manual_country_codes, genre_ids, item_json, original_created_at, archived_at,
                archive_reason, merged_into_item_id
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?)
            """,
            (
                item_id,
                kinozal_id,
                full_item.get("source_uid"),
                full_item.get("version_signature"),
                full_item.get("source_title"),
                full_item.get("source_link"),
                full_item.get("media_type"),
                full_item.get("source_published_at"),
                full_item.get("source_year"),
                full_item.get("source_format"),
                full_item.get("source_description"),
                full_item.get("source_episode_progress"),
                json.dumps(full_item.get("source_audio_tracks", []), ensure_ascii=False),
                full_item.get("imdb_id"),
                full_item.get("cleaned_title"),
                full_item.get("source_category_id"),
                full_item.get("source_category_name"),
                full_item.get("tmdb_id"),
                full_item.get("tmdb_title"),
                full_item.get("tmdb_original_title"),
                full_item.get("tmdb_original_language"),
                full_item.get("tmdb_rating"),
                full_item.get("tmdb_vote_count"),
                full_item.get("tmdb_release_date"),
                full_item.get("tmdb_status"),
                json.dumps(parse_country_codes(full_item.get("tmdb_countries")), ensure_ascii=False),
                full_item.get("manual_bucket") or "",
                ",".join(parse_country_codes(full_item.get("manual_country_codes"))),
                json.dumps(genre_ids, ensure_ascii=False),
                json.dumps(full_item, ensure_ascii=False, sort_keys=True),
                full_item.get("created_at"),
                archived_at,
                compact_spaces(reason or "archive"),
                merged_into_item_id,
            ),
        )
        delivery_rows = self.conn.execute(
            "SELECT id, tg_user_id, item_id, subscription_id, matched_subscription_ids, delivered_at FROM deliveries WHERE item_id = ? ORDER BY delivered_at ASC, id ASC",
            (item_id,),
        ).fetchall()
        for row in delivery_rows:
            delivery = dict(row)
            self.conn.execute(
                """
                INSERT INTO deliveries_archive(
                    original_delivery_id, tg_user_id, original_item_id, kinozal_id, source_uid, media_type, version_signature,
                    source_title, subscription_id, matched_subscription_ids, delivered_at, archived_at, archive_reason, merged_into_item_id
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    delivery.get("id"),
                    delivery.get("tg_user_id"),
                    item_id,
                    kinozal_id,
                    full_item.get("source_uid"),
                    full_item.get("media_type"),
                    full_item.get("version_signature"),
                    full_item.get("source_title"),
                    delivery.get("subscription_id"),
                    delivery.get("matched_subscription_ids"),
                    delivery.get("delivered_at"),
                    archived_at,
                    compact_spaces(reason or "archive"),
                    merged_into_item_id,
                ),
            )
        self.conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
        return True

    def archive_item(self, item_id: int, reason: str, merged_into_item_id: Optional[int] = None) -> bool:
        with self.lock:
            item = self.get_item(int(item_id))
            if not item:
                return False
            archived = self._archive_item_locked(item, reason=reason, merged_into_item_id=merged_into_item_id)
            self.conn.commit()
            return archived

    def get_version_timeline(self, kinozal_id: str, limit: int = 10) -> Dict[str, Any]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return {"kinozal_id": "", "active_count": 0, "archived_count": 0, "versions": []}
        with self.lock:
            active_rows = [dict(x) for x in self.conn.execute(
                """
                SELECT
                    id AS record_id,
                    kinozal_id,
                    source_uid,
                    version_signature,
                    source_title,
                    source_link,
                    media_type,
                    source_published_at,
                    source_year,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    created_at,
                    NULL::BIGINT AS archived_at,
                    'active' AS state
                FROM items
                WHERE kinozal_id = ?
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """,
                (kinozal_id,),
            ).fetchall()]
            archived_rows = [dict(x) for x in self.conn.execute(
                """
                SELECT
                    original_item_id AS record_id,
                    kinozal_id,
                    source_uid,
                    version_signature,
                    source_title,
                    source_link,
                    media_type,
                    source_published_at,
                    source_year,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    original_created_at AS created_at,
                    archived_at,
                    'archived' AS state
                FROM items_archive
                WHERE kinozal_id = ?
                ORDER BY COALESCE(source_published_at, 0) DESC, original_created_at DESC, archive_id DESC
                """,
                (kinozal_id,),
            ).fetchall()]

        def sort_key(entry: Dict[str, Any]) -> tuple[int, int, int]:
            return (
                int(entry.get("source_published_at") or 0),
                int(entry.get("created_at") or 0),
                int(entry.get("record_id") or 0),
            )

        grouped: Dict[str, Dict[str, Any]] = {}
        for row in active_rows + archived_rows:
            row["kinozal_id"] = kinozal_id
            sig = compact_spaces(str(row.get("version_signature") or "")) or build_item_variant_signature(row)
            existing = grouped.get(sig)
            if not existing or sort_key(row) > sort_key(existing):
                row["version_duplicates"] = int(existing.get("version_duplicates") or 1) + 1 if existing else 1
                grouped[sig] = row
            elif existing:
                existing["version_duplicates"] = int(existing.get("version_duplicates") or 1) + 1

        versions = sorted(grouped.values(), key=sort_key, reverse=True)
        for entry in versions:
            entry["variant_summary"] = format_variant_summary(entry)
        return {
            "kinozal_id": kinozal_id,
            "active_count": len(active_rows),
            "archived_count": len(archived_rows),
            "versions": versions[: max(1, int(limit or 10))],
        }

    def cleanup_old_versions(self, keep_last: int = 3, dry_run: bool = True, preview_limit: int = 15) -> Dict[str, Any]:
        keep_last = max(1, int(keep_last or 1))
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    id,
                    kinozal_id,
                    source_uid,
                    source_link,
                    source_title,
                    media_type,
                    version_signature,
                    source_published_at,
                    created_at,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks
                FROM items
                WHERE COALESCE(kinozal_id, '') <> ''
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """
            ).fetchall()
            by_release: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = {}
            for row in rows:
                item = dict(row)
                kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item)
                if not kinozal_id:
                    continue
                media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
                version_sig = compact_spaces(str(item.get("version_signature") or "")) or build_item_variant_signature(item)
                by_release.setdefault((kinozal_id, media_type), {}).setdefault(version_sig, []).append(item)

            groups: List[Dict[str, Any]] = []
            versions_to_archive = 0
            items_to_archive = 0

            def item_sort_key(x: Dict[str, Any]) -> tuple[int, int, int]:
                return (
                    int(x.get("source_published_at") or 0),
                    int(x.get("created_at") or 0),
                    int(x.get("id") or 0),
                )

            for (kinozal_id, media_type), versions_map in by_release.items():
                version_groups: List[Dict[str, Any]] = []
                for version_sig, items in versions_map.items():
                    ordered_items = sorted(items, key=item_sort_key, reverse=True)
                    version_groups.append({
                        "version_signature": version_sig,
                        "items": ordered_items,
                        "representative": ordered_items[0],
                    })
                version_groups.sort(key=lambda g: item_sort_key(g["representative"]), reverse=True)
                if len(version_groups) <= keep_last:
                    continue
                keep_groups = version_groups[:keep_last]
                archive_groups = version_groups[keep_last:]
                archive_items = [item for group in archive_groups for item in group["items"]]
                groups.append({
                    "kinozal_id": kinozal_id,
                    "media_type": media_type,
                    "title": compact_spaces(str((keep_groups[0]["representative"].get("source_title") if keep_groups else archive_groups[0]["representative"].get("source_title")) or "")),
                    "keep_ids": [int(g["representative"]["id"]) for g in keep_groups],
                    "archive_ids": [int(item["id"]) for item in archive_items],
                    "versions_total": len(version_groups),
                    "versions_keep": len(keep_groups),
                    "versions_archive": len(archive_groups),
                })
                versions_to_archive += len(archive_groups)
                items_to_archive += len(archive_items)
                if not dry_run:
                    for archive_group in archive_groups:
                        for item in archive_group["items"]:
                            self._archive_item_locked(item, reason=f"keep_last_versions:{keep_last}", merged_into_item_id=None)

            if not dry_run:
                self.conn.commit()

            groups.sort(key=lambda x: (x["versions_archive"], x["kinozal_id"]), reverse=True)
            summary = {
                "dry_run": dry_run,
                "groups": len(groups),
                "versions_to_archive": versions_to_archive,
                "items_to_archive": items_to_archive,
                "keep_last": keep_last,
                "sample_groups": groups[: max(1, int(preview_limit or 15))],
            }
            log.info(
                "Old versions cleanup dry_run=%s keep_last=%s groups=%s versions_to_archive=%s items_to_archive=%s",
                dry_run,
                keep_last,
                summary["groups"],
                summary["versions_to_archive"],
                summary["items_to_archive"],
            )
            return summary

    def cleanup_exact_duplicate_items(self, dry_run: bool = True, preview_limit: int = 15) -> Dict[str, Any]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    id,
                    source_uid,
                    source_link,
                    source_title,
                    media_type,
                    source_published_at,
                    created_at,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    imdb_id,
                    tmdb_id,
                    tmdb_overview,
                    tmdb_poster_url,
                    tmdb_status,
                    tmdb_vote_count,
                    tmdb_countries,
                    source_description,
                    manual_bucket
                FROM items
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """
            ).fetchall()

            groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
            for row in rows:
                item = dict(row)
                kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
                if not kinozal_id:
                    continue
                media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
                variant_sig = build_item_variant_signature(item)
                groups.setdefault((kinozal_id, media_type, variant_sig), []).append(item)

            duplicate_groups: List[Dict[str, Any]] = []
            items_to_delete = 0
            deliveries_to_migrate = 0

            for (kinozal_id, media_type, _variant_sig), items in groups.items():
                if len(items) < 2:
                    continue
                ordered = sorted(
                    items,
                    key=lambda x: (
                        item_duplicate_quality_score(x),
                        int(x.get("source_published_at") or 0),
                        int(x.get("created_at") or 0),
                        int(x.get("id") or 0),
                    ),
                    reverse=True,
                )
                keeper = ordered[0]
                losers = ordered[1:]
                moved_for_group = 0
                for loser in losers:
                    row = self.conn.execute(
                        "SELECT COUNT(*) AS cnt FROM deliveries WHERE item_id = ?",
                        (int(loser["id"]),),
                    ).fetchone()
                    moved_for_group += int((row or {}).get("cnt") or 0)

                duplicate_groups.append({
                    "kinozal_id": kinozal_id,
                    "media_type": media_type,
                    "title": compact_spaces(str(keeper.get("source_title") or losers[0].get("source_title") or "")),
                    "keep_id": int(keeper["id"]),
                    "remove_ids": [int(x["id"]) for x in losers],
                    "count": len(items),
                    "deliveries_to_migrate": moved_for_group,
                })
                items_to_delete += len(losers)
                deliveries_to_migrate += moved_for_group

                if not dry_run:
                    for loser in losers:
                        keeper_id = int(keeper["id"])
                        loser_id = int(loser["id"])
                        self.conn.execute(
                            """
                            INSERT INTO deliveries(tg_user_id, item_id, subscription_id, matched_subscription_ids, delivered_at)
                            SELECT tg_user_id, ?, subscription_id, matched_subscription_ids, delivered_at
                            FROM deliveries
                            WHERE item_id = ?
                            ON CONFLICT(tg_user_id, item_id) DO NOTHING
                            """,
                            (keeper_id, loser_id),
                        )
                        self._archive_item_locked(loser, reason="exact_duplicate_cleanup", merged_into_item_id=keeper_id)

            duplicate_groups.sort(key=lambda x: (x["count"], x["kinozal_id"]), reverse=True)
            summary = {
                "dry_run": dry_run,
                "groups": len(duplicate_groups),
                "items_to_delete": items_to_delete,
                "deliveries_to_migrate": deliveries_to_migrate,
                "sample_groups": duplicate_groups[: max(1, int(preview_limit or 15))],
            }
            log.info(
                "Duplicate cleanup dry_run=%s groups=%s items_to_delete=%s deliveries_to_migrate=%s",
                dry_run,
                summary["groups"],
                summary["items_to_delete"],
                summary["deliveries_to_migrate"],
            )
            return summary


    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM deliveries WHERE tg_user_id = ? AND item_id = ?",
                (tg_user_id, item_id),
            ).fetchone()
            return row is not None

    def record_delivery(self, tg_user_id: int, item_id: int, sub_id: Optional[int], matched_sub_ids: Optional[Iterable[int]] = None) -> None:
        matched_ids_csv = None
        if matched_sub_ids:
            normalized_ids = sorted({int(x) for x in matched_sub_ids})
            matched_ids_csv = ",".join(str(x) for x in normalized_ids) if normalized_ids else None
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO deliveries(tg_user_id, item_id, subscription_id, matched_subscription_ids, delivered_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(tg_user_id, item_id) DO NOTHING
                """,
                (tg_user_id, item_id, sub_id, matched_ids_csv, utc_ts()),
            )
            self.conn.commit()



db = DB(CFG.database_url)


class RedisCache:
    def __init__(self, url: str):
        self.url = url
        self.client = redis.from_url(url, decode_responses=True) if url else None

    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.client:
            return None
        try:
            raw = await self.client.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            log.warning("Redis get failed for key=%s", key, exc_info=True)
            return None

    async def set_json(self, key: str, value: Dict[str, Any], ex: int) -> None:
        if not self.client:
            return
        try:
            await self.client.set(key, json.dumps(value, ensure_ascii=False), ex=ex)
        except Exception:
            log.warning("Redis set failed for key=%s", key, exc_info=True)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()


cache = RedisCache(CFG.redis_url)


class TMDBClient:
    def __init__(self, token: str, language: str):
        self.token = token
        self.language = language
        self.client = httpx.AsyncClient(timeout=CFG.request_timeout)
        self.base = "https://api.themoviedb.org/3"

    async def close(self) -> None:
        await self.client.aclose()

    async def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        params = params or {}
        cache_key = None
        is_search_request = path.startswith("/search/")
        if cache.client:
            serialized = urlencode(sorted((str(k), str(v)) for k, v in params.items()))
            cache_prefix = "tmdb:v2" if is_search_request else "tmdb"
            cache_key = f"{cache_prefix}:{path}:{serialized}"
            cached = await cache.get_json(cache_key)
            if cached is not None:
                return cached

        headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.token}",
        }
        response = await self.client.get(
            f"{self.base}{path}",
            params=params,
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
        cache_ttl = CFG.tmdb_cache_ttl
        if is_search_request and not (data.get("results") or []):
            cache_ttl = max(0, int(CFG.tmdb_negative_cache_ttl))
        if cache_key and cache_ttl > 0:
            await cache.set_json(cache_key, data, ex=cache_ttl)
        return data

    async def ensure_genres(self, force: bool = False) -> None:
        if not self.token:
            return
        last_sync = db.get_meta("tmdb_genres_synced_at")
        if not force and last_sync:
            try:
                if utc_ts() - int(last_sync) < 86400:
                    return
            except Exception:
                pass

        for media_type in ("movie", "tv"):
            data = await self._get(f"/genre/{media_type}/list", {"language": self.language})
            genres = {int(g["id"]): g["name"] for g in data.get("genres", [])}
            db.upsert_genres(media_type, genres)
        db.set_meta("tmdb_genres_synced_at", str(utc_ts()))
        log.info("TMDB genres synced")

    async def find_by_imdb(self, imdb_id: str) -> Optional[Dict[str, Any]]:
        if not self.token or not imdb_id:
            return None
        data = await self._get(
            f"/find/{imdb_id}",
            {"external_source": "imdb_id", "language": self.language},
        )
        for bucket, media_type in (("movie_results", "movie"), ("tv_results", "tv")):
            results = data.get(bucket) or []
            if results:
                best = results[0]
                return await self.get_details(media_type, int(best["id"]))
        return None

    async def search(self, query: str, media_type: str, year: Optional[int]) -> Optional[Dict[str, Any]]:
        if not self.token or not query:
            return None

        raw_query = compact_spaces(str(query or "")).strip()
        if looks_like_structured_numeric_title(raw_query):
            query = normalize_structured_numeric_title(raw_query)
            cleaned_query = query
        else:
            query = compact_spaces(clean_release_title(raw_query))
            if not query or is_bad_tmdb_candidate(query):
                return None
            cleaned_query = clean_release_title(query)

        if not query or is_bad_tmdb_candidate(query):
            return None
        query_tokens = set(text_tokens(cleaned_query))
        short_common_query = is_short_or_common_tmdb_query(cleaned_query)
        acronym_query = is_short_acronym_tmdb_query(cleaned_query)
        normalized_query = normalize_match_text(cleaned_query)

        async def fetch_results(lang: str) -> List[Dict[str, Any]]:
            params: Dict[str, Any] = {
                "query": query,
                "language": lang,
                "include_adult": "false",
            }
            if media_type == "movie" and year:
                params["year"] = year
            if media_type == "tv" and year:
                params["first_air_date_year"] = year
            data = await self._get(f"/search/{media_type}", params)
            return data.get("results") or []

        async def evaluate_results(results: List[Dict[str, Any]], lang: str) -> Optional[Dict[str, Any]]:
            if not results:
                return None

            exact_matches = []
            for idx, row in enumerate(results[:12]):
                title = compact_spaces(row.get("title") or row.get("name") or "")
                original = compact_spaces(row.get("original_title") or row.get("original_name") or "")
                variants = [
                    title,
                    original,
                    clean_release_title(title),
                    clean_release_title(original),
                ]
                normalized_variants = [normalize_match_text(v) for v in variants if v]
                if normalized_query and any(normalized_query == v for v in normalized_variants):
                    row_year = parse_year(str(row.get("release_date") or row.get("first_air_date") or ""))
                    year_delta = abs(row_year - year) if year and row_year else 9999
                    exact_matches.append((year_delta, idx, row_year or 0, row))

            if exact_matches and (acronym_query or short_common_query):
                exact_matches.sort(key=lambda x: (x[0], x[1]))
                best_year_delta, best_idx, _best_item_year, best_row = exact_matches[0]
                relaxed_year_limit = 2 if acronym_query else 5
                if best_year_delta <= relaxed_year_limit or (year is None and best_idx == 0):
                    details = await self.get_details(media_type, int(best_row["id"]))
                    details["search_match_title"] = compact_spaces(best_row.get("title") or best_row.get("name") or "") or None
                    details["search_match_original_title"] = compact_spaces(best_row.get("original_title") or best_row.get("original_name") or "") or None
                    return details

            best_score = -1.0
            best_id = None
            best_rank = 999

            for idx, item in enumerate(results[:12]):
                title = compact_spaces(item.get("title") or item.get("name") or "")
                original = compact_spaces(item.get("original_title") or item.get("original_name") or "")
                title_clean = clean_release_title(title)
                original_clean = clean_release_title(original)
                score = max(
                    similarity(cleaned_query, title),
                    similarity(cleaned_query, original),
                    similarity(cleaned_query, title_clean),
                    similarity(cleaned_query, original_clean),
                )
                low_q = cleaned_query.lower()
                for cand in [title, original, title_clean, original_clean]:
                    low_c = (cand or "").lower()
                    if low_q and low_c and (low_q in low_c or low_c in low_q):
                        score += 0.12

                candidate_tokens = set()
                for cand in [title, original, title_clean, original_clean]:
                    candidate_tokens.update(text_tokens(cand or ""))
                overlap = 0.0
                if query_tokens and candidate_tokens:
                    overlap = len(query_tokens & candidate_tokens) / max(len(query_tokens), 1)
                    score += overlap * 0.22
                    if short_common_query and overlap == 0:
                        score -= 0.28

                normalized_candidates = [normalize_match_text(cand or "") for cand in [title, original, title_clean, original_clean] if cand]
                if normalized_query and any(normalized_query == cand for cand in normalized_candidates):
                    score += 0.18

                score += max(0.0, 0.10 - idx * 0.015)
                if lang != self.language:
                    score += 0.05

                date_value = item.get("release_date") or item.get("first_air_date") or ""
                item_year = parse_year(date_value)
                if year and item_year:
                    year_delta = abs(item_year - year)
                    score -= min(year_delta, 4) * (0.08 if short_common_query else 0.03)
                    if media_type == "movie":
                        score -= min(year_delta, 12) * 0.05
                        if year_delta >= 6:
                            score -= 0.20
                        if year_delta >= 10:
                            score -= 0.50
                        if year_delta >= 20:
                            score -= 1.00
                    if media_type == "tv":
                        score -= min(year_delta, 8) * (0.06 if short_common_query else 0.025)
                        if year_delta >= 5:
                            score -= 0.22
                        if year_delta >= 10:
                            score -= 0.60
                        if year_delta >= 20:
                            score -= 1.10
                    if short_common_query and year_delta >= 2:
                        score -= 0.18 if media_type == "movie" else 0.38
                    if acronym_query:
                        score -= min(year_delta, 8) * 0.12
                        if year_delta >= 2:
                            score -= 0.42
                    if media_type == "tv" and lang != self.language and year_delta <= 1:
                        score += 0.08

                if score > best_score:
                    best_score = score
                    best_id = int(item["id"])
                    best_rank = idx

            min_score = 0.42
            if media_type == "tv" and is_long_latin_tmdb_query(query):
                min_score = 0.30
            if short_common_query:
                min_score = max(min_score, 0.56)
            if acronym_query:
                min_score = max(min_score, 0.70)
            if media_type == "tv" and lang != self.language and re.search(r"[A-Za-z]", query):
                min_score = max(0.26, min_score - 0.04)

            if best_id is not None and best_score >= min_score:
                details = await self.get_details(media_type, best_id)
                matched = next((row for row in results if int(row.get("id") or 0) == int(best_id)), None)
                if matched:
                    details["search_match_title"] = compact_spaces(matched.get("title") or matched.get("name") or "") or None
                    details["search_match_original_title"] = compact_spaces(matched.get("original_title") or matched.get("original_name") or "") or None
                return details

            if media_type == "tv" and is_long_latin_tmdb_query(query) and not acronym_query:
                top = results[0]
                top_title = compact_spaces(top.get("name") or top.get("title") or "")
                top_original = compact_spaces(top.get("original_name") or top.get("original_title") or "")
                top_year = parse_year(str(top.get("first_air_date") or top.get("release_date") or ""))
                if (top_title or top_original) and not is_bad_tmdb_candidate(top_title or top_original):
                    if not year or not top_year or abs(top_year - year) <= 3:
                        log.info(
                            "TMDB relaxed match accepted for query=%s lang=%s -> %s / %s [rank=%s, score=%.3f]",
                            query,
                            lang,
                            top_title,
                            top_original,
                            best_rank,
                            best_score,
                        )
                        details = await self.get_details(media_type, int(top["id"]))
                        details["search_match_title"] = compact_spaces(top.get("name") or top.get("title") or "") or None
                        details["search_match_original_title"] = compact_spaces(top.get("original_name") or top.get("original_title") or "") or None
                        return details

            return None

        searched_languages: List[str] = [self.language]
        if re.search(r"[A-Za-z]", cleaned_query):
            extra_langs = ["en-US"]
            if media_type == "tv":
                extra_langs.append("ko-KR")
            for lang in extra_langs:
                if lang not in searched_languages:
                    searched_languages.append(lang)

        last_results: List[Dict[str, Any]] = []
        for lang in searched_languages:
            results = await fetch_results(lang)
            last_results = results or last_results
            details = await evaluate_results(results, lang)
            if details is not None:
                return details

        return None

    async def get_details(self, media_type: str, tmdb_id: int) -> Dict[str, Any]:
        append_parts = ["external_ids"]
        if media_type == "tv":
            append_parts.append("content_ratings")
        else:
            append_parts.append("release_dates")

        data = await self._get(
            f"/{media_type}/{tmdb_id}",
            {
                "language": self.language,
                "append_to_response": ",".join(append_parts),
            },
        )

        def pick_age_rating(payload: Dict[str, Any], mt: str) -> Optional[str]:
            try:
                if mt == "tv":
                    results = (payload.get("content_ratings") or {}).get("results") or []
                    for country in ("RU", "US", "GB"):
                        for row in results:
                            if row.get("iso_3166_1") == country and row.get("rating"):
                                return str(row["rating"]).strip()
                    for row in results:
                        if row.get("rating"):
                            return str(row["rating"]).strip()
                else:
                    results = (payload.get("release_dates") or {}).get("results") or []
                    for country in ("RU", "US", "GB"):
                        for row in results:
                            if row.get("iso_3166_1") == country:
                                for rel in row.get("release_dates") or []:
                                    cert = rel.get("certification")
                                    if cert:
                                        return str(cert).strip()
                    for row in results:
                        for rel in row.get("release_dates") or []:
                            cert = rel.get("certification")
                            if cert:
                                return str(cert).strip()
            except Exception:
                return None
            return None

        def unpack_episode(ep: Optional[Dict[str, Any]], prefix: str) -> Dict[str, Any]:
            if not ep:
                return {
                    f"{prefix}_name": None,
                    f"{prefix}_air_date": None,
                    f"{prefix}_season_number": None,
                    f"{prefix}_episode_number": None,
                }
            return {
                f"{prefix}_name": compact_spaces(ep.get("name") or "") or None,
                f"{prefix}_air_date": ep.get("air_date") or None,
                f"{prefix}_season_number": ep.get("season_number"),
                f"{prefix}_episode_number": ep.get("episode_number"),
            }

        genre_ids = [int(g["id"]) for g in data.get("genres", [])]
        title = data.get("title") or data.get("name") or ""
        original = data.get("original_title") or data.get("original_name") or ""
        release_date = data.get("release_date") or data.get("first_air_date") or ""
        poster_path = data.get("poster_path") or ""
        poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}" if poster_path else None
        imdb_id = None
        ext = data.get("external_ids") or {}
        if ext.get("imdb_id"):
            imdb_id = ext["imdb_id"]

        countries: List[str] = []
        if media_type == "tv":
            countries = [str(x).strip() for x in (data.get("origin_country") or []) if str(x).strip()]
        else:
            countries = [str((x or {}).get("iso_3166_1") or "").strip() for x in (data.get("production_countries") or []) if str((x or {}).get("iso_3166_1") or "").strip()]

        result = {
            "tmdb_id": int(data["id"]),
            "media_type": media_type,
            "tmdb_title": title,
            "tmdb_original_title": original,
            "tmdb_original_language": normalize_tmdb_language(data.get("original_language")),
            "tmdb_rating": float(data.get("vote_average") or 0.0),
            "tmdb_vote_count": int(data.get("vote_count") or 0),
            "tmdb_release_date": release_date or None,
            "tmdb_overview": compact_spaces(data.get("overview") or ""),
            "tmdb_poster_url": poster_url,
            "tmdb_status": compact_spaces(data.get("status") or "") or None,
            "tmdb_age_rating": pick_age_rating(data, media_type),
            "tmdb_countries": countries,
            "genre_ids": genre_ids,
            "imdb_id": imdb_id,
        }

        if media_type == "tv":
            result.update({
                "tmdb_number_of_seasons": data.get("number_of_seasons"),
                "tmdb_number_of_episodes": data.get("number_of_episodes"),
            })
            result.update(unpack_episode(data.get("next_episode_to_air"), "tmdb_next_episode"))
            result.update(unpack_episode(data.get("last_episode_to_air"), "tmdb_last_episode"))

        return result

    async def enrich_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        if not self.token:
            return item

        source_text = f"{item.get('source_title') or ''} {item.get('source_description') or ''}"
        if item.get("media_type") == "other" or is_non_video_release(source_text):
            return item
        if should_skip_tmdb_lookup(item):
            log.info(
                "TMDB skipped by source category/title heuristics for %s category=%s",
                item.get("source_title"),
                item.get("source_category_name"),
            )
            return item

        try:
            imdb_id = item.get("imdb_id")
            details = None
            override = manual_tmdb_override_for_item(item)
            if override:
                override_media_type, override_tmdb_id, override_key = override
                try:
                    override_details = await self.get_details(override_media_type, int(override_tmdb_id))
                    if override_details:
                        override_details["search_match_title"] = override_key
                        override_details["search_match_original_title"] = override_key
                        details = override_details
                        log.info(
                            "TMDB manual override matched %s -> %s [%s:%s]",
                            item.get("source_title"),
                            override_key,
                            override_media_type,
                            override_tmdb_id,
                        )
                except Exception:
                    log.warning(
                        "TMDB manual override failed for %s -> %s [%s:%s]",
                        item.get("source_title"),
                        override_key,
                        override_media_type,
                        override_tmdb_id,
                        exc_info=True,
                    )
            if imdb_id and not details:
                details = await self.find_by_imdb(imdb_id)

            if not details:
                media_type = item.get("media_type") or "movie"
                year = item.get("source_year")
                candidates = title_search_candidates(
                    item.get("source_title") or "",
                    item.get("cleaned_title") or "",
                )

                search_plan: List[Tuple[str, str, Optional[int]]] = []
                strict_tv_only = bool(item.get("source_episode_progress")) or media_type == "tv"
                if media_type == "tv":
                    for candidate in candidates:
                        search_plan.extend([
                            (candidate, "tv", year),
                            (candidate, "tv", None),
                        ])
                        if not strict_tv_only:
                            search_plan.extend([
                                (candidate, "movie", year),
                                (candidate, "movie", None),
                            ])
                else:
                    for candidate in candidates:
                        search_plan.extend([
                            (candidate, "movie", year),
                            (candidate, "movie", None),
                            (candidate, "tv", None),
                            (candidate, "tv", year),
                        ])

                seen = set()
                for candidate, mt, y in search_plan:
                    key = (candidate.lower(), mt, y)
                    if key in seen:
                        continue
                    seen.add(key)
                    details = await self.search(candidate, mt, y)
                    if details and not tmdb_match_looks_valid(item, candidate, details, mt):
                        log.info(
                            "TMDB rejected suspicious match for %s -> %s [%s / %s]",
                            item.get("source_title"),
                            candidate,
                            details.get("tmdb_title"),
                            details.get("tmdb_original_title"),
                        )
                        details = None
                    if details:
                        matched_media_type = details.get("media_type") or mt
                        matched_year = parse_year(str(details.get("tmdb_release_date") or ""))
                        log.info("TMDB matched %s -> %s [%s, tmdb_year=%s]", item.get("source_title"), candidate, matched_media_type, matched_year)
                        break

                if not details:
                    if not candidates:
                        log.info("TMDB no search candidates extracted for %s", item.get("source_title"))
                    log.info("TMDB no match for %s | candidates=%s", item.get("source_title"), candidates)

            if details:
                item.update(details)
                if not item.get("media_type"):
                    item["media_type"] = details.get("media_type")
                if not item.get("imdb_id"):
                    item["imdb_id"] = details.get("imdb_id")
            return item
        except Exception:
            log.exception("TMDB enrichment failed for %s", item.get("source_title"))
            return item


tmdb = TMDBClient(CFG.tmdb_token, CFG.language)


class KinozalSource:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.AsyncClient(timeout=CFG.request_timeout, headers={"Accept": "application/json"})

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch_latest(self) -> List[Dict[str, Any]]:
        url = f"{self.base_url}/api/get/rss/kinozal"
        response = await self.client.get(url)
        response.raise_for_status()
        payload = response.json()
        raw_items = self._extract_items(payload)
        items = [self._normalize_item(item) for item in raw_items]
        items = [item for item in items if item.get("source_title")]

        deduped: List[Dict[str, Any]] = []
        seen_keys = set()
        for item in items:
            key = (
                str(item.get("source_uid") or "").strip().lower(),
                str(item.get("version_signature") or "").strip().lower(),
            )
            if key in seen_keys:
                continue
            seen_keys.add(key)
            deduped.append(item)

        deduped.sort(
            key=lambda item: (
                int(item.get("source_published_at") or 0),
                compact_spaces(item.get("source_title") or "").lower(),
                compact_spaces(item.get("source_link") or "").lower(),
            ),
            reverse=True,
        )
        return deduped[: CFG.source_fetch_limit]

    def _extract_items(self, data: Any) -> List[Dict[str, Any]]:
        found: List[Dict[str, Any]] = []
        seen = set()

        def walk(node: Any) -> None:
            node_id = id(node)
            if node_id in seen:
                return
            seen.add(node_id)

            if isinstance(node, list):
                if node and all(isinstance(x, dict) for x in node):
                    for obj in node:
                        if {"title", "link"} & {str(k).lower() for k in obj.keys()}:
                            found.append(obj)
                for x in node:
                    walk(x)
                return

            if isinstance(node, dict):
                lowered = {str(k).lower(): v for k, v in node.items()}
                if {"title", "link", "description", "pubdate", "date"} & set(lowered.keys()):
                    found.append(node)
                for key in ("items", "item", "entries", "entry", "channel", "rss"):
                    if key in lowered:
                        walk(lowered[key])
                for value in node.values():
                    if isinstance(value, (list, dict)):
                        walk(value)

        walk(data)

        uniq = []
        fingerprints = set()
        for item in found:
            fp = json.dumps(item, sort_keys=True, ensure_ascii=False)
            if fp in fingerprints:
                continue
            fingerprints.add(fp)
            uniq.append(item)
        return uniq

    def _pick(self, item: Dict[str, Any], *keys: str) -> str:
        lowered = {str(k).lower(): v for k, v in item.items()}
        for key in keys:
            if key.lower() in lowered:
                value = lowered[key.lower()]
                if isinstance(value, dict):
                    for inner in ("#text", "text", "value", "href", "url"):
                        if inner in value and value[inner]:
                            return str(value[inner]).strip()
                    return compact_spaces(" ".join(str(v) for v in value.values()))
                if isinstance(value, list):
                    return compact_spaces(" ".join(str(v) for v in value))
                if value is not None:
                    return str(value).strip()
        return ""

    def _normalize_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        title = self._pick(item, "title", "name")
        link = self._pick(item, "link", "url")
        guid = self._pick(item, "guid", "id")
        description = strip_html(self._pick(item, "description", "summary", "content"))
        published_raw = self._pick(item, "pubDate", "published", "updated", "date")
        raw_category_id = self._pick(item, "categoryId", "category_id", "cat_id", "cid", "catid")
        raw_category_name = self._pick(item, "category", "category_name", "cat", "section")
        source_category_id = normalize_source_category_id(raw_category_id or raw_category_name)
        source_category_name = resolve_source_category_name(source_category_id, raw_category_name or raw_category_id)
        published_dt = parse_dt(published_raw)
        source_title = compact_spaces(title)
        source_text = f"{source_title} {description}"
        source_year = parse_year(source_text)
        source_format = parse_format(source_text)
        imdb_id = parse_imdb_id(source_text)
        media_type = detect_media_type(source_text)
        forced_media_type = source_category_forced_media_type(source_category_id, source_category_name)
        if forced_media_type:
            media_type = forced_media_type
        cleaned_title = clean_release_title(source_title)
        source_episode_progress = parse_episode_progress(source_text)
        source_audio_tracks = parse_audio_tracks(source_title)
        release_type = infer_release_type(source_title) or ""
        source_uid = build_source_uid(guid, link, source_title, cleaned_title)
        version_signature = build_version_signature(
            source_uid=source_uid,
            media_type=media_type,
            source_title=source_title,
            source_episode_progress=source_episode_progress,
            source_format=source_format,
            source_audio_tracks=source_audio_tracks,
        )

        return {
            "source_uid": source_uid,
            "version_signature": version_signature,
            "source_title": source_title,
            "source_link": link or None,
            "kinozal_id": extract_kinozal_id(link) or extract_kinozal_id(guid),
            "source_published_at": int(published_dt.timestamp()) if published_dt else None,
            "source_year": source_year,
            "source_format": source_format,
            "source_description": description,
            "source_episode_progress": source_episode_progress,
            "source_audio_tracks": source_audio_tracks,
            "imdb_id": imdb_id,
            "cleaned_title": cleaned_title,
            "source_category_id": source_category_id,
            "source_category_name": source_category_name,
            "media_type": media_type,
            "genre_ids": [],
            "raw_json": item,
        }


source = KinozalSource(CFG.torapi_base)


def item_genre_names(item: Dict[str, Any]) -> List[str]:
    all_genres = db.get_all_genres_merged()
    return [all_genres.get(int(gid), str(gid)) for gid in item.get("genre_ids", []) if int(gid) in all_genres]


def sub_genre_names(sub: Dict[str, Any]) -> List[str]:
    all_genres = db.get_all_genres_merged()
    return [all_genres.get(int(gid), str(gid)) for gid in sub.get("genre_ids", []) if int(gid) in all_genres]


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
        r"\b\d+\s*[-–—]\s*(\d+)\s*сезон(?:а|ов)?\b",
        r"\b\d+\s*[-–—]\s*(\d+)\s*season(?:s)?\b",
        r"\b(\d+)\s*сезон(?:а|ов)?\s*:",
        r"\b(\d+)\s*season(?:s)?\s*:",
        r"\b(\d+)\s*сезон(?:а|ов)?\b",
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
        r"\b\d+\s*[-–—]\s*(\d+)\s*сезон(?:а|ов)?\b",
        r"\b\d+\s*[-–—]\s*(\d+)\s*season(?:s)?\b",
        r"\b(\d+)\s*сезон(?:а|ов)?\s*:",
        r"\b(\d+)\s*season(?:s)?\s*:",
        r"\b(\d+)\s*сезон(?:а|ов)?\b",
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


def match_subscription(sub: Dict[str, Any], item: Dict[str, Any]) -> bool:
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


def sub_summary(sub: Dict[str, Any]) -> str:
    genres = sub_genre_names(sub)
    countries = human_country_names(sub.get("country_codes") or sub.get("country_codes_list"), limit=12)
    exclude_countries = human_country_names(sub.get("exclude_country_codes") or sub.get("exclude_country_codes_list"), limit=12)
    formats = []
    if sub.get("allow_720"):
        formats.append("720")
    if sub.get("allow_1080"):
        formats.append("1080")
    if sub.get("allow_2160"):
        formats.append("2160")
    years = "любой"
    if sub.get("year_from") or sub.get("year_to"):
        years = f"{sub.get('year_from') or '…'}–{sub.get('year_to') or '…'}"
    rating = f"{float(sub['min_tmdb_rating']):.1f}" if sub.get("min_tmdb_rating") is not None else "без фильтра"
    keywords = []
    if sub.get("include_keywords"):
        keywords.append("+" + sub["include_keywords"].replace(",", ", +"))
    if sub.get("exclude_keywords"):
        keywords.append("-" + sub["exclude_keywords"].replace(",", ", -"))
    return (
        f"#{sub['id']} {'🟢' if sub.get('is_enabled') else '⏸'} <b>{html.escape(sub['name'])}</b>\n"
        f"Тип: {human_media_type(sub.get('media_type'))}\n"
        f"Подтип: {human_content_filter(sub.get('content_filter') or 'any')}\n"
        f"Годы: {years}\n"
        f"Форматы: {', '.join(formats) if formats else 'любые'}\n"
        f"Рейтинг TMDB: {rating}\n"
        f"Жанры: {', '.join(genres) if genres else 'любые'}\n"
        f"Страны: {', '.join(countries) if countries else 'любые'}\n"
        f"Искл. страны: {', '.join(exclude_countries) if exclude_countries else 'нет'}\n"
        f"Ключи: {' '.join(keywords) if keywords else 'без фильтра'}"
    )




def item_message(item: Dict[str, Any], matched_subs: Optional[Sequence[Dict[str, Any]]] = None) -> str:
    def human_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return dt.strftime("%d.%m.%Y")
        except Exception:
            return str(value)

    def fmt_episode(prefix: str) -> Optional[str]:
        name = item.get(f"{prefix}_name")
        air_date = human_date(item.get(f"{prefix}_air_date"))
        season = item.get(f"{prefix}_season_number")
        episode = item.get(f"{prefix}_episode_number")
        parts = []
        if season and episode:
            parts.append(f"S{season}E{episode}")
        if air_date:
            parts.append(str(air_date))
        if name:
            parts.append(name)
        return " • ".join(str(x) for x in parts if x) or None

    source_title = item.get("source_title") or ""
    title = item.get("tmdb_title") or source_title or "Без названия"
    original = item.get("tmdb_original_title")
    media_type = item.get("media_type") or "movie"
    media = human_media_type(media_type)
    rating = item.get("tmdb_rating")
    votes = item.get("tmdb_vote_count")
    year = item_display_year(item)
    fmt = item.get("source_format")
    genres = item_genre_names(item)
    release_type = infer_release_type(source_title)
    audio_variants = parse_audio_variants(source_title)
    if not audio_variants:
        raw_audio_tracks = item.get("source_audio_tracks")
        if isinstance(raw_audio_tracks, str):
            try:
                raw_audio_tracks = json.loads(raw_audio_tracks)
            except Exception:
                raw_audio_tracks = parse_audio_tracks(source_title)
        raw_audio_tracks = raw_audio_tracks or parse_audio_tracks(source_title)
        audio_variants = [{"label": str(label), "count": 1} for label in raw_audio_tracks if str(label).strip()]
    release_full = format_release_full_title(source_title, item.get("tmdb_title"), item.get("tmdb_original_title"))
    countries = parse_country_codes(item.get("tmdb_countries"))
    country_names = human_country_names(countries, limit=4)

    lines = [f"🆕 <b>{html.escape(title)}</b>"]

    if original and original.lower() != title.lower():
        lines.append(f"<i>Ориг.: {html.escape(original)}</i>")

    meta = [media]
    if year:
        meta.append(str(year))
    if release_type:
        meta.append(release_type)
    if fmt:
        fmt_str = str(fmt)
        meta.append(f"{fmt_str}p" if fmt_str.isdigit() else fmt_str)
    if rating is not None and float(rating) > 0:
        if votes:
            meta.append(f"TMDB {float(rating):.1f} ({int(votes)})")
        else:
            meta.append(f"TMDB {float(rating):.1f}")
    if country_names:
        meta.append(", ".join(country_names))
    if meta:
        lines.append("🎬 " + " • ".join(meta))

    if release_full and compact_spaces(release_full).lower() != compact_spaces(title).lower():
        lines.append(f"📦 <b>Релиз:</b> {html.escape(release_full)}")

    episode_progress = item.get("source_episode_progress") or item.get("episode_progress") or item.get("source_series_update")
    if episode_progress:
        lines.append(f"📺 <b>В торренте:</b> {html.escape(str(episode_progress))}")

    if audio_variants:
        lines.append(f"🎧 <b>Озвучки:</b> {count_audio_variants(audio_variants)} • {html.escape(format_audio_variants(audio_variants))}")

    source_category_name = compact_spaces(str(item.get("source_category_name") or ""))
    if source_category_name:
        lines.append(f"🗂 <b>Категория API:</b> {html.escape(source_category_name)}")

    if genres:
        lines.append(f"🎭 <b>Жанры:</b> {html.escape(', '.join(genres[:6]))}")

    if item.get("tmdb_status"):
        lines.append(f"ℹ️ <b>Статус:</b> {html.escape(str(item['tmdb_status']))}")

    if media_type == "tv":
        seasons = item.get("tmdb_number_of_seasons")
        episodes = item.get("tmdb_number_of_episodes")
        if seasons or episodes:
            parts = []
            if seasons:
                parts.append(f"сезонов: {seasons}")
            if episodes:
                parts.append(f"эпизодов: {episodes}")
            lines.append("🧾 <b>TMDB:</b> " + ", ".join(parts))

        next_ep = fmt_episode("tmdb_next_episode")
        if next_ep:
            lines.append(f"🗓 <b>След. серия:</b> {html.escape(next_ep)}")

        last_ep = fmt_episode("tmdb_last_episode")
        if last_ep:
            lines.append(f"🕘 <b>Последняя серия:</b> {html.escape(last_ep)}")

    if item.get("tmdb_age_rating"):
        lines.append(f"🔞 <b>Возраст:</b> {html.escape(str(item['tmdb_age_rating']))}")

    if matched_subs:
        matched_names = [html.escape(str(sub.get("name") or "").strip()) for sub in matched_subs if str(sub.get("name") or "").strip()]
        matched_names = list(dict.fromkeys(matched_names))
        if matched_names:
            label = ", ".join(matched_names[:8])
            if len(matched_names) > 8:
                label += f" и ещё {len(matched_names) - 8}"
            lines.append(f"🔔 <b>Подошло под:</b> {label}")

    links = []
    if item.get("source_link"):
        links.append(f'<a href="{html.escape(item["source_link"], quote=True)}">Kinozal</a>')
    if item.get("tmdb_id"):
        tmdb_kind = "tv" if media_type == "tv" else "movie"
        links.append(f'<a href="https://www.themoviedb.org/{tmdb_kind}/{int(item["tmdb_id"])}">TMDB</a>')
    if item.get("imdb_id"):
        links.append(f'<a href="https://www.imdb.com/title/{html.escape(str(item["imdb_id"]), quote=True)}/">IMDb</a>')
    if links:
        lines.append("🔗 " + " • ".join(links))

    overview = item.get("tmdb_overview") or item.get("source_description")
    if overview:
        lines.append("")
        lines.append(html.escape(compact_spaces(overview)))

    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


async def send_item_to_user(bot: Bot, tg_user_id: int, item: Dict[str, Any], subs: Optional[Sequence[Dict[str, Any]]]) -> None:
    text = item_message(item, subs)
    plain_text = html_to_plain_text(text)
    poster = item.get("tmdb_poster_url")
    full_html_text = short(text, 3900)
    full_plain_text = short(plain_text, 3900)
    caption_html = short(text, 1000)
    caption_plain = short(plain_text, 1000)

    if poster:
        try:
            await bot.send_photo(
                tg_user_id,
                photo=poster,
                caption=caption_html,
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception:
            log.warning("send_photo failed for user=%s item=%s", tg_user_id, item.get("id"), exc_info=True)
            try:
                await bot.send_photo(
                    tg_user_id,
                    photo=poster,
                    caption=caption_plain,
                )
                return
            except Exception:
                log.warning("send_photo plain fallback failed for user=%s item=%s", tg_user_id, item.get("id"), exc_info=True)

    try:
        await bot.send_message(
            tg_user_id,
            text=full_html_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
    except Exception:
        log.warning("send_message HTML failed for user=%s item=%s", tg_user_id, item.get("id"), exc_info=True)
        await bot.send_message(
            tg_user_id,
            text=full_plain_text,
            disable_web_page_preview=CFG.disable_preview,
        )


def genres_kb(sub_id: int, page: int = 0) -> InlineKeyboardMarkup:
    all_genres = list(db.get_all_genres_merged().items())
    selected = set(db.get_subscription_genres(sub_id))
    per_page = 8
    pages = max(1, (len(all_genres) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    chunk = all_genres[page * per_page : (page + 1) * per_page]

    kb = InlineKeyboardBuilder()
    for genre_id, name in chunk:
        mark = "✅" if genre_id in selected else "⬜️"
        kb.button(text=f"{mark} {name}", callback_data=f"subgenre:{sub_id}:{page}:{genre_id}")
    if pages > 1:
        kb.button(text="⬅️", callback_data=f"subgenrespage:{sub_id}:{page-1}")
        kb.button(text=f"{page+1}/{pages}", callback_data="noop")
        kb.button(text="➡️", callback_data=f"subgenrespage:{sub_id}:{page+1}")
    kb.button(text="Очистить жанры", callback_data=f"subgenresclear:{sub_id}:{page}")
    kb.button(text="Готово", callback_data=f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()


def countries_kb(sub_id: int, page: int = 0, mode: str = "include") -> InlineKeyboardMarkup:
    all_codes = db.get_known_country_codes()
    selected = set(
        db.get_subscription_country_codes(sub_id)
        if mode == "include"
        else db.get_subscription_exclude_country_codes(sub_id)
    )
    for code in selected:
        if code not in all_codes:
            all_codes.append(code)
    all_codes = sorted(all_codes, key=lambda code: country_name_ru(code).lower())

    per_page = 8
    pages = max(1, (len(all_codes) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    chunk = all_codes[page * per_page : (page + 1) * per_page]

    kb = InlineKeyboardBuilder()
    for code in chunk:
        mark = "✅" if code in selected else "⬜️"
        kb.button(text=f"{mark} {country_name_ru(code)}", callback_data=f"subcountry:{mode}:{sub_id}:{page}:{code}")
    if pages > 1:
        kb.button(text="⬅️", callback_data=f"subcountriespage:{mode}:{sub_id}:{page-1}")
        kb.button(text=f"{page+1}/{pages}", callback_data="noop")
        kb.button(text="➡️", callback_data=f"subcountriespage:{mode}:{sub_id}:{page+1}")
    clear_text = "Очистить страны" if mode == "include" else "Очистить исключения"
    kb.button(text=clear_text, callback_data=f"subcountriesclear:{mode}:{sub_id}:{page}")
    kb.button(text="Готово", callback_data=f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()


def content_filter_kb(sub_id: int) -> InlineKeyboardMarkup:
    selected = str((db.get_subscription(sub_id) or {}).get("content_filter") or "any")
    options = [
        ("Любое", "any"),
        ("Только аниме", "only_anime"),
        ("Только дорамы", "only_dorama"),
        ("Без аниме", "exclude_anime"),
        ("Без дорам", "exclude_dorama"),
        ("Без аниме и дорам", "exclude_anime_dorama"),
    ]
    kb = InlineKeyboardBuilder()
    for text, code in options:
        mark = "✅" if code == selected else "⬜️"
        kb.button(text=f"{mark} {text}", callback_data=f"subcontent:{sub_id}:{code}")
    kb.button(text="◀️ Назад", callback_data=f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()


router = Router()
bot_instance: Optional[Bot] = None
poller_task: Optional[asyncio.Task] = None


async def safe_edit(callback: CallbackQuery, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    try:
        await callback.message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
    except TelegramBadRequest:
        await callback.message.answer(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )


def is_admin(user_id: int) -> bool:
    return user_id in CFG.admin_ids


def _meta_int(key: str, default: int = 0) -> int:
    try:
        value = db.get_meta(key)
        return int(value) if value is not None else default
    except Exception:
        return default


def _exc_brief(exc: Exception) -> str:
    text = compact_spaces(f"{type(exc).__name__}: {exc}")
    return short(text, 500)


async def send_admins_text(bot: Bot, text: str) -> None:
    if not CFG.admin_ids:
        return
    for admin_id in CFG.admin_ids:
        try:
            await bot.send_message(
                int(admin_id),
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=CFG.disable_preview,
            )
        except Exception:
            log.warning("admin alert send failed admin=%s", admin_id, exc_info=True)


async def note_source_cycle_success(bot: Bot) -> None:
    now_ts = utc_ts()
    prev_status = compact_spaces(db.get_meta("source_health_status") or "ok").lower() or "ok"
    fail_streak = _meta_int("source_fail_streak", 0)
    last_failed_at = _meta_int("source_last_failed_at", 0)
    last_error = compact_spaces(db.get_meta("source_last_error") or "")

    db.set_meta("source_last_success_at", str(now_ts))
    db.set_meta("source_fail_streak", "0")
    db.set_meta("source_health_status", "ok")

    if prev_status == "down" and fail_streak >= max(1, int(CFG.source_error_alert_threshold)):
        duration_min = max(0, (now_ts - last_failed_at) // 60) if last_failed_at else 0
        lines = [
            "✅ <b>Источник снова отвечает</b>",
            f"Восстановление: {html.escape(format_dt(now_ts))}",
        ]
        if duration_min:
            lines.append(f"Простой: ~{duration_min} мин.")
        if last_error:
            lines.append(f"Последняя ошибка: <code>{html.escape(last_error)}</code>")
        await send_admins_text(bot, "\n".join(lines))


async def note_source_cycle_failure(bot: Bot, exc: Exception) -> None:
    now_ts = utc_ts()
    fail_streak = _meta_int("source_fail_streak", 0) + 1
    last_alert_at = _meta_int("source_last_alert_at", 0)
    last_success_at = _meta_int("source_last_success_at", 0)
    repeat_seconds = max(60, int(CFG.source_error_alert_repeat_minutes) * 60)
    error_text = _exc_brief(exc)

    db.set_meta("source_fail_streak", str(fail_streak))
    db.set_meta("source_last_failed_at", str(now_ts))
    db.set_meta("source_last_error", error_text)
    db.set_meta("source_health_status", "down")

    threshold = max(1, int(CFG.source_error_alert_threshold))
    should_alert = fail_streak >= threshold and (last_alert_at <= 0 or now_ts - last_alert_at >= repeat_seconds)
    if not should_alert:
        return

    db.set_meta("source_last_alert_at", str(now_ts))
    lines = [
        "⚠️ <b>Сбой в цикле опроса источника</b>",
        f"Повторов подряд: {fail_streak}",
        f"Время: {html.escape(format_dt(now_ts))}",
        f"Ошибка: <code>{html.escape(error_text)}</code>",
    ]
    if last_success_at:
        lines.append(f"Последний успешный цикл: {html.escape(format_dt(last_success_at))}")
    await send_admins_text(bot, "\n".join(lines))


def extract_kinozal_id_from_text(text: str) -> Optional[str]:
    return extract_kinozal_id(text)


def parse_admin_route_target(raw: str) -> Tuple[Optional[str], List[str], str]:
    token = compact_spaces((raw or "").strip()).lower()
    mapping = {
        "anime": ("anime", [], "аниме"),
        "аниме": ("anime", [], "аниме"),
        "dorama": ("dorama", [], "дорамы"),
        "дорама": ("dorama", [], "дорамы"),
        "дорамы": ("dorama", [], "дорамы"),
        "world": ("regular", [], "мир"),
        "мир": ("regular", [], "мир"),
        "regular": ("regular", [], "обычное"),
        "обычное": ("regular", [], "обычное"),
        "turkey": ("regular", ["TR"], "Турция"),
        "turkish": ("regular", ["TR"], "Турция"),
        "турция": ("regular", ["TR"], "Турция"),
        "tr": ("regular", ["TR"], "Турция"),
    }
    return mapping.get(token, (None, [], ""))


ADMIN_USERS_PAGE_SIZE = 12


def format_admin_user_line(user: Dict[str, Any]) -> str:
    state = user_access_state(user)
    username = compact_spaces(str(user.get("username") or ""))
    first_name = compact_spaces(str(user.get("first_name") or ""))
    label_parts = []
    if first_name:
        label_parts.append(first_name)
    if username:
        label_parts.append(f"@{username}")
    label = " / ".join(label_parts) if label_parts else "без имени"
    total_subs = int(user.get("subscriptions_total") or 0)
    enabled_subs = int(user.get("subscriptions_enabled") or 0)
    return (
        f"• <code>{user['tg_user_id']}</code> — {html.escape(label)}\n"
        f"  Статус: {html.escape(state)} | до: {html.escape(format_access_expiry(user.get('access_expires_at')))}\n"
        f"  Подписок: {total_subs} (вкл: {enabled_subs})"
    )


def format_admin_user_details(user: Dict[str, Any]) -> str:
    state = user_access_state(user)
    username = compact_spaces(str(user.get("username") or ""))
    first_name = compact_spaces(str(user.get("first_name") or ""))
    lines = [
        f"👤 Пользователь <code>{user['tg_user_id']}</code>",
        f"Имя: {html.escape(first_name or '—')}",
        f"Username: {html.escape('@' + username if username else '—')}",
        f"Статус доступа: {html.escape(state)}",
        f"Доступ до: {html.escape(format_access_expiry(user.get('access_expires_at')))}",
        f"Создан: {html.escape(format_dt(user.get('created_at')))}",
    ]
    subs = user.get("subscriptions") or []
    if subs:
        lines.append("")
        lines.append("Подписки:")
        for sub in subs:
            lines.append(sub_summary(db.get_subscription(int(sub["id"])) or sub))
            lines.append("")
        if lines[-1] == "":
            lines.pop()
    else:
        lines.append("")
        lines.append("Подписок нет.")
    return "\n".join(lines)


def parse_command_payload(text: Optional[str]) -> str:
    raw = text or ""
    parts = raw.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""

async def ensure_access_for_message(message: Message) -> bool:
    user_id = message.from_user.id
    db.ensure_user(
        user_id,
        message.from_user.username or "",
        message.from_user.first_name or "",
        auto_grant=(CFG.allow_mode == "open" or is_admin(user_id)),
    )
    if db.user_has_access(user_id):
        return True
    await message.answer(require_access_message())
    return False


async def ensure_access_for_callback(callback: CallbackQuery) -> bool:
    user_id = callback.from_user.id
    db.ensure_user(
        user_id,
        callback.from_user.username or "",
        callback.from_user.first_name or "",
        auto_grant=(CFG.allow_mode == "open" or is_admin(user_id)),
    )
    if db.user_has_access(user_id):
        return True
    await callback.answer("Нет доступа", show_alert=True)
    return False


@router.message(CommandStart(deep_link=True))
@router.message(CommandStart())
async def cmd_start(message: Message, command: CommandObject) -> None:
    user_id = message.from_user.id
    db.ensure_user(
        user_id,
        message.from_user.username or "",
        message.from_user.first_name or "",
        auto_grant=(CFG.allow_mode == "open" or is_admin(user_id)),
    )

    code = (command.args or "").strip() if command else ""
    if code and CFG.allow_mode == "invite" and not db.user_has_access(user_id):
        if db.redeem_invite(code, user_id):
            await message.answer("✅ Доступ активирован. Добро пожаловать.", reply_markup=main_menu_kb(is_admin(user_id)))
            return
        await message.answer("❌ Инвайт не подошёл: просрочен, исчерпан или неверный.")
        return

    if not db.user_has_access(user_id):
        await message.answer(
            "Привет. Это бот для персональных новостей Kinozal.\n\n" + require_access_message()
        )
        return

    text = (
        "Привет ✨\n"
        "Тут можно настроить личные выборки новинок с Kinozal,\n"
        "дотянуть жанры/рейтинг/постер из TMDB и получать только то, что подходит тебе."
    )
    await message.answer(text, reply_markup=main_menu_kb(is_admin(user_id)))


@router.message(Command("menu"))
async def cmd_menu(message: Message) -> None:
    if not await ensure_access_for_message(message):
        return
    await message.answer("Главное меню", reply_markup=main_menu_kb(is_admin(message.from_user.id)))


@router.message(Command("whoami"))
async def cmd_whoami(message: Message) -> None:
    db.ensure_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.first_name or "",
        auto_grant=(CFG.allow_mode == "open" or is_admin(message.from_user.id)),
    )
    user = db.get_user(message.from_user.id) or {}
    await message.answer(
        f"Твой Telegram user_id: <code>{message.from_user.id}</code>\n"
        f"Статус доступа: {html.escape(user_access_state(user))}\n"
        f"Доступ до: {html.escape(format_access_expiry(user.get('access_expires_at')))}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("subs"))
async def cmd_subs(message: Message) -> None:
    if not await ensure_access_for_message(message):
        return
    subs = db.list_user_subscriptions(message.from_user.id)
    if not subs:
        await message.answer("У тебя пока нет подписок.", reply_markup=main_menu_kb(is_admin(message.from_user.id)))
        return
    text = "Твои подписки:\n\n" + "\n\n".join(sub_summary(db.get_subscription(int(x["id"]))) for x in subs[:10])
    await message.answer(text, reply_markup=subscriptions_list_kb(subs), parse_mode=ParseMode.HTML)


@router.message(Command("latest"))
async def cmd_latest(message: Message) -> None:
    if not await ensure_access_for_message(message):
        return
    items = db.get_last_items(5)
    if not items:
        await message.answer("Пока ещё нет сохранённых релизов.")
        return
    for item in items:
        await send_item_to_user(message.bot, message.chat.id, item, None)


@router.message(Command("route"))
async def cmd_route(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return
    if not message.reply_to_message:
        await message.answer("Ответь этой командой на уведомление бота. Пример: /route dorama")
        return
    parts = compact_spaces(message.text or "").split(maxsplit=1)
    target_raw = parts[1] if len(parts) > 1 else ""
    bucket, country_codes, label = parse_admin_route_target(target_raw)
    if not bucket:
        await message.answer("Используй: /route anime | dorama | turkey | world")
        return
    replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
    kinozal_id = extract_kinozal_id_from_text(replied_text)
    if not kinozal_id:
        await message.answer("Не смог найти Kinozal ID в сообщении. Ответь именно на уведомление бота.")
        return
    item = db.find_item_by_kinozal_id(kinozal_id)
    if not item:
        await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
        return
    db.set_item_manual_routing(int(item["id"]), bucket=bucket, country_codes=country_codes)
    item = db.get_item(int(item["id"])) or item

    delivered_count = 0
    matched_users = 0
    for sub in db.list_enabled_subscriptions():
        sub_full = db.get_subscription(int(sub["id"]))
        if not sub_full:
            continue
        if not match_subscription(sub_full, item):
            continue
        matched_users += 1
        tg_user_id = int(sub_full["tg_user_id"])
        if db.delivered(tg_user_id, int(item["id"])) or db.delivered_equivalent(tg_user_id, item):
            continue
        try:
            previous_item = db.get_latest_delivered_related_item(tg_user_id, item)
            if previous_item:
                log.info(
                    "Admin route delivering updated release item=%s to user=%s source_uid=%s reason=%s prev_item_id=%s",
                    item.get("id"),
                    tg_user_id,
                    item.get("source_uid"),
                    describe_variant_change(previous_item, item),
                    previous_item.get("id"),
                )
            else:
                log.info(
                    "Admin route delivering new release item=%s to user=%s source_uid=%s",
                    item.get("id"),
                    tg_user_id,
                    item.get("source_uid"),
                )
            await send_item_to_user(message.bot, tg_user_id, item, [sub_full])
            db.record_delivery(tg_user_id, int(item["id"]), int(sub_full["id"]), [int(sub_full["id"])])
            delivered_count += 1
        except Exception:
            log.exception("Admin route delivery failed item=%s user=%s", item.get("id"), tg_user_id)

    await message.answer(
        f"✅ Релиз перенаправлен как: {label}.\n"
        f"Kinozal ID: {kinozal_id}\n"
        f"Подходящих подписок: {matched_users}\n"
        f"Новых уведомлений отправлено: {delivered_count}"
    )


def build_match_explanation(item: Dict[str, Any], live_item: Optional[Dict[str, Any]] = None) -> str:
    display_item = live_item or item
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item) or "—"
    media = str(display_item.get("media_type") or "movie")
    bucket = item_content_bucket(display_item)
    category_name = compact_spaces(str(display_item.get("source_category_name") or "")) or "—"
    category_id = compact_spaces(str(display_item.get("source_category_id") or "")) or "—"
    countries = effective_item_countries(display_item)
    country_names = human_country_names(countries, limit=8)
    candidates = title_search_candidates(display_item.get("source_title") or "", display_item.get("cleaned_title") or "")
    matched_subs: List[Dict[str, Any]] = []
    for sub in db.list_enabled_subscriptions():
        sub_full = db.get_subscription(int(sub["id"]))
        if not sub_full:
            continue
        if match_subscription(sub_full, display_item):
            matched_subs.append(sub_full)

    lines = [
        f"🧭 <b>Explain match</b> — Kinozal ID <code>{html.escape(kinozal_id)}</code>",
        f"Заголовок: {html.escape(compact_spaces(str(display_item.get('source_title') or '—')))}",
        f"TMDB в БД: {html.escape('есть' if item.get('tmdb_id') else 'нет')}",
    ]
    if item.get("tmdb_id"):
        lines.append(
            f"TMDB в БД title: {html.escape(compact_spaces(str(item.get('tmdb_title') or item.get('tmdb_original_title') or '—')))} "
            f"(id={int(item.get('tmdb_id') or 0)}, year={parse_year(str(item.get('tmdb_release_date') or '')) or '—'})"
        )
    if live_item is not None:
        lines.append(f"TMDB live: {html.escape('есть' if live_item.get('tmdb_id') else 'нет')}")
        if live_item.get("tmdb_id"):
            lines.append(
                f"TMDB live title: {html.escape(compact_spaces(str(live_item.get('tmdb_title') or live_item.get('tmdb_original_title') or '—')))} "
                f"(id={int(live_item.get('tmdb_id') or 0)}, year={parse_year(str(live_item.get('tmdb_release_date') or '')) or '—'})"
            )
    lines.extend([
        f"Media: {html.escape(human_media_type(media))}",
        f"Bucket: {html.escape(bucket)}",
        f"Категория API: {html.escape(category_name)} ({html.escape(category_id)})",
        f"Страны: {html.escape(', '.join(country_names or countries or ['—']))}",
        f"Manual route: bucket={html.escape(compact_spaces(str(display_item.get('manual_bucket') or '')) or '—')} | countries={html.escape(','.join(parse_jsonish_list(display_item.get('manual_country_codes')) or [] ) or '—')}",
        f"Кандидаты TMDB: {html.escape(', '.join(candidates[:8]) if candidates else 'не извлеклись')}",
        f"Подходящих подписок сейчас: {len(matched_subs)}",
    ])
    for sub in matched_subs[:12]:
        lines.append(
            f"• <code>{int(sub['tg_user_id'])}</code> — {html.escape(sub.get('name') or 'без названия')} "
            f"[{html.escape(sub.get('preset_key') or 'custom')}]"
        )
    if len(matched_subs) > 12:
        lines.append(f"… ещё {len(matched_subs) - 12}")
    if not display_item.get("tmdb_id") and category_name != "—":
        lines.append("")
        lines.append("Фолбэк сейчас работает через source category.")
    return "\n".join(lines)


async def rematch_item_live(item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
    before = dict(item)
    try:
        enriched = await tmdb.enrich_item(dict(item))
        db.save_item(enriched)
        refreshed = db.get_item(int(item["id"])) or db.find_item_by_kinozal_id(str(item.get("kinozal_id") or ""))
        return before, refreshed, True
    except Exception:
        log.exception("Rematch failed for item_id=%s kinozal_id=%s", item.get("id"), item.get("kinozal_id"))
        return before, None, False


@router.message(Command("explainmatch"))
async def cmd_explainmatch(message: Message, command: CommandObject) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return

    kinozal_id = extract_kinozal_id(command.args or "")
    if not kinozal_id and message.reply_to_message:
        replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
        kinozal_id = extract_kinozal_id_from_text(replied_text)
    if not kinozal_id:
        await message.answer("Используй: /explainmatch <kinozal_id> или ответь этой командой на уведомление бота.")
        return

    item = db.find_item_by_kinozal_id(kinozal_id)
    if not item:
        await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
        return

    live_item = dict(item)
    try:
        live_item = await tmdb.enrich_item(dict(item))
    except Exception:
        log.exception("Live explain TMDB recompute failed for kinozal_id=%s", kinozal_id)
        live_item = dict(item)

    await message.answer(build_match_explanation(item, live_item), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)


@router.message(Command("rematch"))
async def cmd_rematch(message: Message, command: CommandObject) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return

    kinozal_id = extract_kinozal_id(command.args or "")
    if not kinozal_id and message.reply_to_message:
        replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
        kinozal_id = extract_kinozal_id_from_text(replied_text)
    if not kinozal_id:
        await message.answer("Используй: /rematch <kinozal_id> или ответь этой командой на уведомление бота.")
        return

    item = db.find_item_by_kinozal_id(kinozal_id)
    if not item:
        await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
        return

    before, after, ok = await rematch_item_live(item)
    if not ok or not after:
        await message.answer(f"Не удалось перематчить Kinozal ID {kinozal_id}. Смотри лог app.")
        return

    before_tmdb = before.get("tmdb_id")
    after_tmdb = after.get("tmdb_id")
    before_title = compact_spaces(str(before.get("tmdb_title") or before.get("tmdb_original_title") or "")) or "—"
    after_title = compact_spaces(str(after.get("tmdb_title") or after.get("tmdb_original_title") or "")) or "—"
    country_names = [COUNTRY_NAMES_RU.get(code, code) for code in parse_country_codes(after.get("tmdb_countries"))]
    lines = [
        f"♻️ Rematch — Kinozal ID {html.escape(str(kinozal_id))}",
        f"Заголовок: {html.escape(compact_spaces(str(after.get('source_title') or '—')))}",
        f"TMDB было: {html.escape(before_title)}" + (f" (id={int(before_tmdb)})" if before_tmdb else ""),
        f"TMDB стало: {html.escape(after_title)}" + (f" (id={int(after_tmdb)})" if after_tmdb else ""),
        f"Страны: {html.escape(', '.join(country_names or ['—']))}",
        "Старые доставки не переотправляются. Обновлена только карточка релиза в БД.",
    ]
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)


@router.message(Command("rematch_unmatched"))
async def cmd_rematch_unmatched(message: Message, command: CommandObject) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return

    raw = compact_spaces(str(command.args or ""))
    limit = 50
    if raw.isdigit():
        limit = max(1, min(int(raw), 500))

    items = db.list_items_for_rematch(limit=limit, only_unmatched=True)
    if not items:
        await message.answer("Для рематча ничего не найдено: unmatched items с kinozal_id закончились.")
        return

    updated = 0
    matched_now = 0
    still_unmatched = 0
    errors = 0
    samples: List[str] = []

    for item in items:
        before, after, ok = await rematch_item_live(item)
        if not ok or not after:
            errors += 1
            continue
        before_tmdb = before.get("tmdb_id")
        after_tmdb = after.get("tmdb_id")
        if before_tmdb != after_tmdb:
            updated += 1
        if after_tmdb:
            matched_now += 1
            if len(samples) < 8:
                samples.append(
                    f"• {html.escape(str(after.get('kinozal_id') or '—'))} — {html.escape(compact_spaces(str(after.get('tmdb_title') or after.get('tmdb_original_title') or after.get('source_title') or '—')))}"
                )
        else:
            still_unmatched += 1

    lines = [
        f"♻️ Batch rematch unmatched: {len(items)}",
        f"Обновлено записей: {updated}",
        f"Теперь есть TMDB: {matched_now}",
        f"Остались без TMDB: {still_unmatched}",
        f"Ошибок: {errors}",
        "Старые доставки не переотправляются.",
    ]
    if samples:
        lines.append("")
        lines.append("Примеры новых матчей:")
        lines.extend(samples)
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)


@router.message(Command("why"))
async def cmd_why(message: Message, command: CommandObject) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return

    kinozal_id = extract_kinozal_id(command.args or "")
    if not kinozal_id and message.reply_to_message:
        replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
        kinozal_id = extract_kinozal_id_from_text(replied_text)
    if not kinozal_id:
        await message.answer("Используй: /why <kinozal_id> или ответь этой командой на уведомление бота.")
        return

    report = db.get_version_timeline(kinozal_id, limit=10)
    versions = report.get("versions") or []
    if not versions:
        await message.answer(f"Не нашёл версий по Kinozal ID {kinozal_id}.")
        return

    lines = [
        f"🔎 История релиза Kinozal ID {kinozal_id}",
        f"Активных items: {report['active_count']}",
        f"В архиве: {report['archived_count']}",
        f"Показано версий: {len(versions)}",
        "",
    ]
    for idx, entry in enumerate(versions):
        icon = "🟢" if entry.get("state") == "active" else "📦"
        ts = int(entry.get("source_published_at") or entry.get("created_at") or entry.get("archived_at") or 0)
        title = short(compact_spaces(str(entry.get("source_title") or "")), 90)
        lines.append(
            f"{icon} #{entry.get('record_id')} | {format_dt(ts)} | {human_media_type(str(entry.get('media_type') or 'movie'))}\n"
            f"{html.escape(title)}\n"
            f"{html.escape(entry.get('variant_summary') or format_variant_summary(entry))}"
        )
        if idx + 1 < len(versions):
            older = versions[idx + 1]
            lines.append(f"↳ Изменение к этой версии: {html.escape(describe_variant_change(older, entry))}")
        if int(entry.get("version_duplicates") or 1) > 1:
            lines.append(f"↳ Дубликатов этой версии: {int(entry.get('version_duplicates') or 1)}")
        lines.append("")

    await message.answer("\n".join(lines).strip(), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)


@router.message(Command("cleanup_versions"))
async def cmd_cleanup_versions(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return

    parts = compact_spaces(message.text or "").split()
    confirm = any(part.lower() in {"confirm", "run", "apply", "yes"} for part in parts[1:])
    keep_last = CFG.cleanup_versions_keep_last
    for part in parts[1:]:
        if part.isdigit():
            keep_last = max(1, int(part))
            break

    summary = db.cleanup_old_versions(
        keep_last=keep_last,
        dry_run=not confirm,
        preview_limit=CFG.cleanup_versions_preview_limit,
    )

    lines = [
        ("🧪 Предпросмотр чистки старых версий" if not confirm else "🧹 Чистка старых версий выполнена"),
        f"Keep-last: {summary['keep_last']}",
        f"Групп релизов: {summary['groups']}",
        f"Версий к архивированию: {summary['versions_to_archive']}",
        f"Items к архивированию: {summary['items_to_archive']}",
    ]

    samples = summary.get("sample_groups") or []
    if samples:
        lines.append("")
        lines.append("Примеры:")
        for group in samples[: CFG.cleanup_versions_preview_limit]:
            title = short(compact_spaces(str(group.get("title") or "")), 90)
            lines.append(
                f"• {group['kinozal_id']} | {group['media_type']} | keep {group['keep_ids']} | archive {group['archive_ids']}\n  {html.escape(title)}"
            )
    else:
        lines.append("")
        lines.append("Старых версий не найдено.")

    if not confirm:
        lines.append("")
        lines.append(f"Для выполнения: /cleanup_versions {keep_last} confirm")

    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)


@router.message(Command("cleanup_duplicates"))
async def cmd_cleanup_duplicates(message: Message) -> None:
    if not is_admin(message.from_user.id):
        await message.answer("Только для администратора.")
        return

    parts = compact_spaces(message.text or "").split()
    confirm = any(part.lower() in {"confirm", "run", "apply", "yes"} for part in parts[1:])
    summary = db.cleanup_exact_duplicate_items(
        dry_run=not confirm,
        preview_limit=CFG.cleanup_duplicates_preview_limit,
    )

    lines = [
        ("🧪 Предпросмотр чистки дублей" if not confirm else "🧹 Чистка дублей выполнена"),
        f"Групп дублей: {summary['groups']}",
        f"Лишних items: {summary['items_to_delete']}",
        f"Доставок к переносу: {summary['deliveries_to_migrate']}",
    ]

    samples = summary.get("sample_groups") or []
    if samples:
        lines.append("")
        lines.append("Примеры:")
        for group in samples[: CFG.cleanup_duplicates_preview_limit]:
            title = short(compact_spaces(str(group.get("title") or "")), 90)
            lines.append(
                f"• {group['kinozal_id']} | {group['media_type']} | keep #{group['keep_id']} | del {group['remove_ids']}\n  {html.escape(title)}"
            )
    else:
        lines.append("")
        lines.append("Дублей не найдено.")

    if not confirm:
        lines.append("")
        lines.append("Для выполнения: /cleanup_duplicates confirm")

    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)


@router.message(Command("create_invite"))
async def cmd_create_invite(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = compact_spaces(message.text or "").split(maxsplit=3)
    uses = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
    days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 30
    note = parts[3] if len(parts) > 3 else ""
    invite = db.create_invite(message.from_user.id, uses, days, note)
    deep = ""
    if CFG.deep_link_bot_username:
        deep = f"\nСсылка: https://t.me/{CFG.deep_link_bot_username}?start={invite['code']}"
    expires = format_dt(invite["expires_at"]) if invite["expires_at"] else "без срока"
    await message.answer(
        f"✅ Инвайт создан\n"
        f"Код: <code>{invite['code']}</code>\n"
        f"Использований: {invite['uses_left']}\n"
        f"Истекает: {expires}\n"
        f"Примечание: {html.escape(invite.get('note') or '—')}{deep}",
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


@router.message(Command("invites"))
async def cmd_invites(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    invites = db.list_invites(15)
    if not invites:
        await message.answer("Инвайтов пока нет.")
        return
    lines = ["Последние инвайты:"]
    for inv in invites:
        expires = format_dt(inv["expires_at"]) if inv["expires_at"] else "без срока"
        lines.append(
            f"<code>{inv['code']}</code> | uses={inv['uses_left']} | exp={expires} | {html.escape(inv.get('note') or '')}"
        )
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("grant"))
async def cmd_grant(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = compact_spaces(message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /grant USER_ID [DAYS]")
        return
    target = int(parts[1])
    days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
    db.ensure_user(target, "", "", auto_grant=False)
    expires_at = utc_ts() + days * 86400 if days and days > 0 else None
    db.set_user_access(target, True, access_expires_at=expires_at)
    suffix = f" до {format_dt(expires_at)}" if expires_at is not None else " без срока"
    await message.answer(f"✅ Доступ выдан пользователю <code>{target}</code>{suffix}", parse_mode=ParseMode.HTML)


@router.message(Command("extend"))
async def cmd_extend(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = compact_spaces(message.text or "").split()
    if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
        await message.answer("Использование: /extend USER_ID DAYS")
        return
    target = int(parts[1])
    days = int(parts[2])
    db.ensure_user(target, "", "", auto_grant=False)
    user = db.extend_user_access_days(target, days)
    await message.answer(
        f"✅ Доступ пользователя <code>{target}</code> продлён на {days} дн.\n"
        f"Теперь до: {html.escape(format_access_expiry((user or {}).get('access_expires_at')))}",
        parse_mode=ParseMode.HTML,
    )


@router.message(Command("revoke"))
async def cmd_revoke(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = compact_spaces(message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /revoke USER_ID")
        return
    target = int(parts[1])
    db.set_user_access(target, False, access_expires_at=None)
    await message.answer(f"⛔ Доступ отозван у пользователя <code>{target}</code>", parse_mode=ParseMode.HTML)


@router.message(Command("users"))
async def cmd_users(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = compact_spaces(message.text or "").split()
    page = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
    page = max(1, page)
    offset = (page - 1) * ADMIN_USERS_PAGE_SIZE
    users = db.list_users_with_stats(limit=ADMIN_USERS_PAGE_SIZE, offset=offset)
    total = db.count_users()
    if not users:
        await message.answer("Пользователей пока нет.")
        return
    pages = max(1, (total + ADMIN_USERS_PAGE_SIZE - 1) // ADMIN_USERS_PAGE_SIZE)
    lines = [f"👥 Пользователи — страница {page}/{pages}", ""]
    lines.extend(format_admin_user_line(user) for user in users)
    await message.answer("\n\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("user"))
async def cmd_user(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    parts = compact_spaces(message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("Использование: /user USER_ID")
        return
    target = int(parts[1])
    user = db.get_user_with_subscriptions(target)
    if not user:
        await message.answer("Пользователь не найден.")
        return
    await message.answer(format_admin_user_details(user), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)


@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message) -> None:
    if not is_admin(message.from_user.id):
        return
    payload = parse_command_payload(message.text)
    if not payload:
        await message.answer(
            "Использование: /broadcast ТЕКСТ\n\n"
            "Пример:\n"
            "/broadcast ⚠️ Внимание! API временно недоступно. Мы уже чиним и сообщим, когда всё восстановится."
        )
        return
    user_ids = db.list_broadcast_user_ids(active_only=True, include_admins=False)
    if not user_ids:
        await message.answer("Некому отправлять: активных пользователей с доступом не найдено.")
        return

    sent = 0
    failed = 0
    failed_ids: List[int] = []
    for user_id in user_ids:
        try:
            await message.bot.send_message(
                user_id,
                payload,
                disable_web_page_preview=CFG.disable_preview,
            )
            sent += 1
        except Exception:
            failed += 1
            failed_ids.append(int(user_id))
            log.warning("broadcast send failed user=%s", user_id, exc_info=True)
        await asyncio.sleep(0.04)

    lines = [
        "📢 Рассылка завершена",
        f"Отправлено: {sent}",
        f"Ошибок: {failed}",
        f"Получателей всего: {len(user_ids)}",
    ]
    if failed_ids:
        preview = ", ".join(str(x) for x in failed_ids[:20])
        more = " …" if len(failed_ids) > 20 else ""
        lines.append(f"Не доставлено user_id: <code>{html.escape(preview + more)}</code>")
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def show_main_menu(target: Message | CallbackQuery) -> None:
    uid = target.from_user.id if isinstance(target, CallbackQuery) else target.from_user.id
    text = "Главное меню"
    kb = main_menu_kb(is_admin(uid))
    if isinstance(target, CallbackQuery):
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)


@router.callback_query(F.data == "menu:root")
async def cb_menu_root(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    await show_main_menu(callback)
    await callback.answer()


@router.callback_query(F.data == "menu:subs")
async def cb_menu_subs(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    subs = db.list_user_subscriptions(callback.from_user.id)
    if not subs:
        await safe_edit(callback, "У тебя пока нет подписок.", main_menu_kb(is_admin(callback.from_user.id)))
        await callback.answer()
        return
    text = "Твои подписки:\n\n" + "\n\n".join(sub_summary(db.get_subscription(int(x["id"]))) for x in subs[:10])
    await safe_edit(callback, text, subscriptions_list_kb(subs))
    await callback.answer()


@router.callback_query(F.data == "menu:new")
async def cb_menu_new(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub = db.create_subscription(callback.from_user.id)
    text = (
        "Создаём новую подписку ✨\n\n"
        "Выбери готовый пресет или открой свою настройку."
    )
    await safe_edit(callback, text, preset_kb(int(sub["id"]), "new"))
    await callback.answer()


@router.callback_query(F.data == "menu:latest")
async def cb_menu_latest(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    items = db.get_last_items(5)
    if not items:
        await callback.answer("Пока пусто", show_alert=True)
        return
    await callback.message.answer("Последние сохранённые релизы:")
    for item in items:
        await send_item_to_user(callback.bot, callback.message.chat.id, item, None)
    await callback.answer()


@router.callback_query(F.data == "menu:whoami")
async def cb_menu_whoami(callback: CallbackQuery) -> None:
    await callback.answer()
    user = db.get_user(callback.from_user.id) or {}
    await callback.message.answer(
        f"Твой Telegram user_id: <code>{callback.from_user.id}</code>\n"
        f"Статус доступа: {html.escape(user_access_state(user))}\n"
        f"Доступ до: {html.escape(format_access_expiry(user.get('access_expires_at')))}",
        parse_mode=ParseMode.HTML,
    )


@router.callback_query(F.data == "menu:admin_invites")
async def cb_menu_admin_invites(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    await safe_edit(
        callback,
        "Админ-раздел по доступам.\n\n"
        "Команды:\n"
        "<code>/create_invite 1 30 имя</code>\n"
        "<code>/grant USER_ID</code>\n"
        "<code>/revoke USER_ID</code>\n"
        "<code>/invites</code>",
        admin_invites_kb(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:invites")
async def cb_admin_invites(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    invites = db.list_invites(10)
    if not invites:
        await safe_edit(callback, "Инвайтов пока нет.", admin_invites_kb())
        await callback.answer()
        return
    lines = ["Последние инвайты:\n"]
    for inv in invites:
        expires = format_dt(inv["expires_at"]) if inv["expires_at"] else "без срока"
        lines.append(
            f"<code>{inv['code']}</code>\n"
            f"uses={inv['uses_left']} | exp={expires}\n"
            f"{html.escape(inv.get('note') or '—')}\n"
        )
    await safe_edit(callback, "\n".join(lines), admin_invites_kb())
    await callback.answer()


@router.callback_query(F.data == "menu:admin_users")
async def cb_menu_admin_users(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    users = db.list_users_with_stats(limit=ADMIN_USERS_PAGE_SIZE, offset=0)
    total = db.count_users()
    if not users:
        await safe_edit(callback, "Пользователей пока нет.", admin_users_kb(0, False, False))
        await callback.answer()
        return
    pages = max(1, (total + ADMIN_USERS_PAGE_SIZE - 1) // ADMIN_USERS_PAGE_SIZE)
    lines = [f"👥 Пользователи — страница 1/{pages}", ""]
    lines.extend(format_admin_user_line(user) for user in users)
    await safe_edit(callback, "\n\n".join(lines), admin_users_kb(0, False, total > ADMIN_USERS_PAGE_SIZE))
    await callback.answer()


@router.callback_query(F.data.startswith("admin:users:"))
async def cb_admin_users(callback: CallbackQuery) -> None:
    if not is_admin(callback.from_user.id):
        await callback.answer("Недоступно", show_alert=True)
        return
    try:
        page = max(0, int(callback.data.split(":")[2]))
    except Exception:
        page = 0
    offset = page * ADMIN_USERS_PAGE_SIZE
    total = db.count_users()
    users = db.list_users_with_stats(limit=ADMIN_USERS_PAGE_SIZE, offset=offset)
    if not users and page > 0:
        page = max(0, (max(1, total) - 1) // ADMIN_USERS_PAGE_SIZE)
        offset = page * ADMIN_USERS_PAGE_SIZE
        users = db.list_users_with_stats(limit=ADMIN_USERS_PAGE_SIZE, offset=offset)
    pages = max(1, (total + ADMIN_USERS_PAGE_SIZE - 1) // ADMIN_USERS_PAGE_SIZE)
    lines = [f"👥 Пользователи — страница {page + 1}/{pages}", ""]
    if users:
        lines.extend(format_admin_user_line(user) for user in users)
    else:
        lines.append("Пользователей пока нет.")
    await safe_edit(callback, "\n\n".join(lines), admin_users_kb(page, page > 0, (page + 1) * ADMIN_USERS_PAGE_SIZE < total))
    await callback.answer()


@router.callback_query(F.data.startswith("sub:view:"))
async def cb_sub_view(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
    await callback.answer()


@router.callback_query(F.data.startswith("sub:toggle:"))
async def cb_sub_toggle(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    sub = db.get_subscription(sub_id)
    db.update_subscription(sub_id, is_enabled=0 if sub.get("is_enabled") else 1)
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
    await callback.answer("Готово")


@router.callback_query(F.data.startswith("sub:delete:"))
async def cb_sub_delete(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    db.delete_subscription(sub_id)
    subs = db.list_user_subscriptions(callback.from_user.id)
    if subs:
        await safe_edit(callback, "Подписка удалена.", subscriptions_list_kb(subs))
    else:
        await safe_edit(callback, "Подписка удалена. Список пуст.", main_menu_kb(is_admin(callback.from_user.id)))
    await callback.answer("Удалено")


async def get_live_test_items_for_subscription(sub_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    sub = db.get_subscription(sub_id)
    if not sub:
        return []

    try:
        fresh_items = await source.fetch_latest()
    except Exception:
        log.warning("Live test fetch failed for sub=%s", sub_id, exc_info=True)
        return []

    matched: List[Dict[str, Any]] = []

    for raw_item in fresh_items:
        item = dict(raw_item)
        source_text = f"{item.get('source_title') or ''} {item.get('source_description') or ''}"
        if item.get("media_type") == "other" or is_non_video_release(source_text):
            continue
        try:
            item = await tmdb.enrich_item(item)
        except Exception:
            log.warning(
                "TMDB enrich failed during subscription test for sub=%s title=%s",
                sub_id,
                item.get("source_title"),
                exc_info=True,
            )

        if match_subscription(sub, item):
            matched.append(item)

        if len(matched) >= limit:
            break

    return matched


@router.callback_query(F.data.startswith("sub:test:"))
async def cb_sub_test(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return

    sub = db.get_subscription(sub_id)
    items = await get_live_test_items_for_subscription(sub_id, limit=5)

    if items:
        await callback.message.answer(
            f"Тест для <b>{html.escape(sub['name'])}</b>:\n<i>Показываю самые свежие совпадения с верха ленты.</i>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
        for item in items:
            await send_item_to_user(callback.bot, callback.message.chat.id, item, [sub])
        await callback.answer("Показал свежие")
        return

    fallback_items = db.get_last_items_for_subscription(sub_id, 5)
    if fallback_items:
        await callback.message.answer(
            f"Тест для <b>{html.escape(sub['name'])}</b>:\n<i>Свежих совпадений сверху ленты сейчас не нашлось, показываю последние совпадения из базы.</i>",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
        for item in fallback_items:
            await send_item_to_user(callback.bot, callback.message.chat.id, item, [sub])
        await callback.answer("Показал из базы")
        return

    await callback.answer("Совпадений среди свежих релизов пока нет", show_alert=True)


@router.callback_query(F.data.startswith("sub:edit_presets:"))
async def cb_sub_edit_presets(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери готовый пресет или оставь свою ручную настройку.", preset_kb(sub_id, "edit"))
    await callback.answer()


@router.callback_query(F.data.startswith("subpreset:"))
async def cb_sub_preset_apply(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, flow, preset_key = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    if preset_key == "custom":
        if flow == "new":
            db.update_subscription(sub_id, content_filter="any", country_codes="", exclude_country_codes="", preset_key="")
            await safe_edit(
                callback,
                "Создаём новую подписку ✨\n\nШаг 1/5: выбери, что ловить.",
                wizard_type_kb(sub_id),
            )
            await callback.answer("Переходим к своей настройке")
            return
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
        await callback.answer("Оставил текущую настройку")
        return

    sub = apply_subscription_preset(sub_id, preset_key)
    if not sub:
        await callback.answer("Пресет не найден", show_alert=True)
        return

    suffix = "Пресет применён. Подправь что нужно вручную." if flow == "edit" else "Пресет создан. Можно пользоваться сразу или подправить вручную."
    await safe_edit(callback, f"{sub_summary(sub)}\n\n<i>{suffix}</i>", sub_view_kb(sub_id, sub))
    await callback.answer("Пресет применён")


@router.callback_query(F.data.startswith("sub:edit_content_filter:"))
async def cb_sub_edit_content_filter(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери подтип контента:", content_filter_kb(sub_id))
    await callback.answer()


@router.callback_query(F.data.startswith("subcontent:"))
async def cb_sub_content_filter(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, code = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    db.update_subscription(sub_id, content_filter=code)
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
    await callback.answer("Подтип обновлён")


@router.callback_query(F.data.startswith("sub:edit_type:"))
async def cb_sub_edit_type(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери тип контента:", sub_type_kb(sub_id))
    await callback.answer()


@router.callback_query(F.data.startswith("subtype:"))
async def cb_subtype(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, media_type = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    db.update_subscription(sub_id, media_type=media_type)
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
    await callback.answer("Тип обновлён")


@router.callback_query(F.data.startswith("sub:edit_years:"))
async def cb_sub_edit_years(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери диапазон лет:", year_preset_kb(sub_id))
    await callback.answer()


@router.callback_query(F.data.startswith("subyear:"))
async def cb_subyear(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, code = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    if code == "any":
        db.update_subscription(sub_id, year_from=None, year_to=None)
    else:
        db.update_subscription(sub_id, year_from=int(code), year_to=2100)
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
    await callback.answer("Годы обновлены")


@router.callback_query(F.data.startswith("sub:ask_years:"))
async def cb_sub_ask_years(callback: CallbackQuery, state: FSMContext) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await state.set_state(EditInputState.waiting_years)
    await state.update_data(sub_id=sub_id)
    await callback.message.answer(
        "Пришли диапазон лет в виде:\n"
        "<code>2020 2026</code>\n"
        "или слово <code>any</code>",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.message(EditInputState.waiting_years)
async def st_waiting_years(message: Message, state: FSMContext) -> None:
    if not await ensure_access_for_message(message):
        return
    data = await state.get_data()
    sub_id = int(data["sub_id"])
    if not db.subscription_belongs_to(sub_id, message.from_user.id):
        await state.clear()
        await message.answer("Подписка не найдена.")
        return
    raw = compact_spaces(message.text or "")
    if raw.lower() == "any":
        db.update_subscription(sub_id, year_from=None, year_to=None)
        await state.clear()
        await message.answer("Годы сброшены.")
        return
    parts = raw.split()
    if len(parts) != 2 or not all(p.isdigit() for p in parts):
        await message.answer("Нужно прислать два года через пробел, например: <code>2020 2026</code>", parse_mode=ParseMode.HTML)
        return
    year_from, year_to = int(parts[0]), int(parts[1])
    if year_from > year_to:
        year_from, year_to = year_to, year_from
    db.update_subscription(sub_id, year_from=year_from, year_to=year_to)
    await state.clear()
    sub = db.get_subscription(sub_id)
    await message.answer(sub_summary(sub), parse_mode=ParseMode.HTML, reply_markup=sub_view_kb(sub_id, sub))


@router.callback_query(F.data.startswith("sub:edit_formats:"))
async def cb_sub_edit_formats(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, "Переключай нужные форматы:", format_kb(sub_id, sub, "edit"))
    await callback.answer()


@router.callback_query(F.data.startswith("subfmt:"))
async def cb_sub_format_toggle(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, fmt, mode = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    sub = db.get_subscription(sub_id)
    field = f"allow_{fmt}"
    db.update_subscription(sub_id, **{field: 0 if sub.get(field) else 1})
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, "Переключай нужные форматы:", format_kb(sub_id, sub, mode))
    await callback.answer()


@router.callback_query(F.data.startswith("sub:edit_rating:"))
async def cb_sub_edit_rating(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Минимальный рейтинг TMDB:", rating_kb(sub_id))
    await callback.answer()


@router.callback_query(F.data.startswith("subrating:"))
async def cb_sub_rating(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, code = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    db.update_subscription(sub_id, min_tmdb_rating=None if code == "none" else float(code))
    sub = db.get_subscription(sub_id)
    await safe_edit(callback, sub_summary(sub), sub_view_kb(sub_id, sub))
    await callback.answer("Рейтинг обновлён")


@router.callback_query(F.data.startswith("sub:edit_genres:"))
async def cb_sub_edit_genres(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    parts = callback.data.split(":")
    sub_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери жанры:", genres_kb(sub_id, page))
    await callback.answer()


@router.callback_query(F.data.startswith("subgenre:"))
async def cb_sub_genre_toggle(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, page_str, genre_id_str = callback.data.split(":")
    sub_id = int(sub_id_str)
    page = int(page_str)
    genre_id = int(genre_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    db.toggle_subscription_genre(sub_id, genre_id)
    await safe_edit(callback, "Выбери жанры:", genres_kb(sub_id, page))
    await callback.answer()


@router.callback_query(F.data.startswith("subgenrespage:"))
async def cb_sub_genres_page(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, page_str = callback.data.split(":")
    sub_id = int(sub_id_str)
    page = int(page_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери жанры:", genres_kb(sub_id, page))
    await callback.answer()


@router.callback_query(F.data.startswith("subgenresclear:"))
async def cb_sub_genres_clear(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, page_str = callback.data.split(":")
    sub_id = int(sub_id_str)
    page = int(page_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    db.set_subscription_genres(sub_id, [])
    await safe_edit(callback, "Выбери жанры:", genres_kb(sub_id, page))
    await callback.answer("Жанры очищены")


@router.callback_query(F.data.startswith("sub:edit_countries:"))
async def cb_sub_edit_countries(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    parts = callback.data.split(":")
    sub_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери страны, которые нужно включать:", countries_kb(sub_id, page, "include"))
    await callback.answer()


@router.callback_query(F.data.startswith("sub:edit_exclude_countries:"))
async def cb_sub_edit_exclude_countries(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    parts = callback.data.split(":")
    sub_id = int(parts[2])
    page = int(parts[3]) if len(parts) > 3 else 0
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await safe_edit(callback, "Выбери страны, которые нужно исключать:", countries_kb(sub_id, page, "exclude"))
    await callback.answer()


@router.callback_query(F.data.startswith("subcountry:"))
async def cb_sub_country_toggle(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, mode, sub_id_str, page_str, country_code = callback.data.split(":")
    sub_id = int(sub_id_str)
    page = int(page_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    if mode == "exclude":
        db.toggle_subscription_exclude_country_code(sub_id, country_code)
        title = "Выбери страны, которые нужно исключать:"
    else:
        db.toggle_subscription_country_code(sub_id, country_code)
        title = "Выбери страны, которые нужно включать:"
        mode = "include"
    await safe_edit(callback, title, countries_kb(sub_id, page, mode))
    await callback.answer()


@router.callback_query(F.data.startswith("subcountriespage:"))
async def cb_sub_countries_page(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, mode, sub_id_str, page_str = callback.data.split(":")
    sub_id = int(sub_id_str)
    page = int(page_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    title = "Выбери страны, которые нужно исключать:" if mode == "exclude" else "Выбери страны, которые нужно включать:"
    await safe_edit(callback, title, countries_kb(sub_id, page, mode))
    await callback.answer()


@router.callback_query(F.data.startswith("subcountriesclear:"))
async def cb_sub_countries_clear(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, mode, sub_id_str, page_str = callback.data.split(":")
    sub_id = int(sub_id_str)
    page = int(page_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    if mode == "exclude":
        db.set_subscription_exclude_country_codes(sub_id, [])
        title = "Выбери страны, которые нужно исключать:"
        done = "Исключаемые страны очищены"
    else:
        db.set_subscription_country_codes(sub_id, [])
        title = "Выбери страны, которые нужно включать:"
        done = "Страны очищены"
        mode = "include"
    await safe_edit(callback, title, countries_kb(sub_id, page, mode))
    await callback.answer(done)


@router.callback_query(F.data.startswith("sub:edit_keywords:"))
async def cb_sub_edit_keywords(callback: CallbackQuery, state: FSMContext) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await state.set_state(EditInputState.waiting_keywords)
    await state.update_data(sub_id=sub_id)
    await callback.message.answer(
        "Пришли ключевые слова.\n"
        "Формат:\n"
        "<code>+marvel +space -cam -ts</code>\n\n"
        "Плюс — обязательно должно встретиться,\n"
        "минус — исключить.\n"
        "Для сброса пришли: <code>clear</code>",
        parse_mode=ParseMode.HTML,
    )
    await callback.answer()


@router.message(EditInputState.waiting_keywords)
async def st_waiting_keywords(message: Message, state: FSMContext) -> None:
    if not await ensure_access_for_message(message):
        return
    data = await state.get_data()
    sub_id = int(data["sub_id"])
    if not db.subscription_belongs_to(sub_id, message.from_user.id):
        await state.clear()
        await message.answer("Подписка не найдена.")
        return
    raw = compact_spaces(message.text or "")
    if raw.lower() == "clear":
        db.update_subscription(sub_id, include_keywords="", exclude_keywords="")
    else:
        include, exclude = normalize_keywords_input(raw)
        db.update_subscription(sub_id, include_keywords=include, exclude_keywords=exclude)
    await state.clear()
    sub = db.get_subscription(sub_id)
    await message.answer(sub_summary(sub), parse_mode=ParseMode.HTML, reply_markup=sub_view_kb(sub_id, sub))


@router.callback_query(F.data.startswith("sub:rename:"))
async def cb_sub_rename(callback: CallbackQuery, state: FSMContext) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[2])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Это не твоя подписка", show_alert=True)
        return
    await state.set_state(EditInputState.waiting_name)
    await state.update_data(sub_id=sub_id)
    await callback.message.answer("Пришли новое имя подписки.")
    await callback.answer()


@router.message(EditInputState.waiting_name)
async def st_waiting_name(message: Message, state: FSMContext) -> None:
    if not await ensure_access_for_message(message):
        return
    data = await state.get_data()
    sub_id = int(data["sub_id"])
    if not db.subscription_belongs_to(sub_id, message.from_user.id):
        await state.clear()
        await message.answer("Подписка не найдена.")
        return
    new_name = short(compact_spaces(message.text or ""), 100)
    if not new_name:
        await message.answer("Имя не может быть пустым.")
        return
    db.update_subscription(sub_id, name=new_name)
    await state.clear()
    sub = db.get_subscription(sub_id)
    await message.answer(sub_summary(sub), parse_mode=ParseMode.HTML, reply_markup=sub_view_kb(sub_id, sub))


@router.callback_query(F.data.startswith("wiztype:"))
async def cb_wiz_type(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, media_type = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Подписка не найдена", show_alert=True)
        return
    db.update_subscription(sub_id, media_type=media_type)
    sub = db.get_subscription(sub_id)
    await safe_edit(
        callback,
        "Шаг 2/5: выбери форматы. Можно отметить несколько.",
        format_kb(sub_id, sub, "wiz"),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("wizfmtdone:"))
async def cb_wiz_fmt_done(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    sub_id = int(callback.data.split(":")[1])
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Подписка не найдена", show_alert=True)
        return
    await safe_edit(callback, "Шаг 3/5: выбери годы.", wizard_years_kb(sub_id))
    await callback.answer()


@router.callback_query(F.data.startswith("wizyear:"))
async def cb_wiz_year(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, code = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Подписка не найдена", show_alert=True)
        return
    if code == "any" or code == "manualskip":
        db.update_subscription(sub_id, year_from=None, year_to=None)
    else:
        db.update_subscription(sub_id, year_from=int(code), year_to=2100)
    await safe_edit(callback, "Шаг 4/5: минимальный рейтинг TMDB.", wizard_rating_kb(sub_id))
    await callback.answer()


@router.callback_query(F.data.startswith("wizrating:"))
async def cb_wiz_rating(callback: CallbackQuery) -> None:
    if not await ensure_access_for_callback(callback):
        return
    _, sub_id_str, code = callback.data.split(":")
    sub_id = int(sub_id_str)
    if not db.subscription_belongs_to(sub_id, callback.from_user.id):
        await callback.answer("Подписка не найдена", show_alert=True)
        return
    db.update_subscription(sub_id, min_tmdb_rating=None if code == "none" else float(code))
    await safe_edit(callback, "Шаг 5/5: выбери жанры или сразу жми «Готово».", genres_kb(sub_id, 0))
    await callback.answer()


@router.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery) -> None:
    await callback.answer()


async def process_new_items(bot: Bot) -> None:
    items = await source.fetch_latest()
    if not items:
        log.info("Source returned no items")
        return

    first_run_seen = db.get_meta("bootstrap_done") == "1"
    touched_item_ids: List[int] = []
    new_item_ids: List[int] = []

    for raw_item in items:
        source_text = f"{raw_item.get('source_title') or ''} {raw_item.get('source_description') or ''}"
        if raw_item.get("media_type") == "other" or is_non_video_release(source_text):
            log.info("Skip non-video item: %s", raw_item.get("source_title"))
            continue
        enriched = await tmdb.enrich_item(raw_item)
        if not enriched.get("tmdb_id") and compact_spaces(str(enriched.get("source_category_name") or "")):
            log.info(
                "TMDB no match, using source category fallback title=%s category=%s bucket=%s media=%s",
                enriched.get("source_title"),
                enriched.get("source_category_name"),
                item_content_bucket(enriched),
                enriched.get("media_type"),
            )
        item_id, is_new, materially_changed = db.save_item(enriched)
        if is_new:
            new_item_ids.append(item_id)
            touched_item_ids.append(item_id)
        elif materially_changed:
            touched_item_ids.append(item_id)

    enabled_subs = [db.get_subscription(int(sub["id"])) for sub in db.list_enabled_subscriptions()]
    enabled_subs = [sub for sub in enabled_subs if sub]

    if not first_run_seen and CFG.start_fetch_as_read:
        for item_id in new_item_ids:
            for sub in enabled_subs:
                db.record_delivery(int(sub["tg_user_id"]), item_id, int(sub["id"]), [int(sub["id"])])
        db.set_meta("bootstrap_done", "1")
        log.info("Bootstrap complete: %s items marked as delivered", len(new_item_ids))
        return

    db.set_meta("bootstrap_done", "1")
    if not touched_item_ids:
        log.info("No new or enriched item versions")
        return

    for item_id in touched_item_ids:
        item = db.get_item(item_id)
        if not item:
            continue

        matches_by_user: Dict[int, List[Dict[str, Any]]] = {}
        for sub in enabled_subs:
            tg_user_id = int(sub["tg_user_id"])
            if db.delivered(tg_user_id, item_id) or db.delivered_equivalent(tg_user_id, item):
                continue
            if not match_subscription(sub, item):
                continue
            matches_by_user.setdefault(tg_user_id, []).append(sub)

        for tg_user_id, matched_subs in matches_by_user.items():
            try:
                previous_item = db.get_latest_delivered_related_item(tg_user_id, item)
                if previous_item:
                    log.info(
                        "Delivering updated release item=%s to user=%s source_uid=%s reason=%s prev_item_id=%s",
                        item_id,
                        tg_user_id,
                        item.get("source_uid"),
                        describe_variant_change(previous_item, item),
                        previous_item.get("id"),
                    )
                else:
                    log.info(
                        "Delivering new release item=%s to user=%s source_uid=%s",
                        item_id,
                        tg_user_id,
                        item.get("source_uid"),
                    )
                await send_item_to_user(bot, tg_user_id, item, matched_subs)
                db.record_delivery(tg_user_id, item_id, int(matched_subs[0]["id"]), [int(sub["id"]) for sub in matched_subs])
                await asyncio.sleep(0.12)
            except Exception:
                log.exception("Failed to deliver item=%s to user=%s", item_id, tg_user_id)


async def poller(bot: Bot) -> None:
    while True:
        try:
            await process_new_items(bot)
            await note_source_cycle_success(bot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await note_source_cycle_failure(bot, exc)
            log.exception("Poller cycle failed")
        await asyncio.sleep(CFG.poll_seconds)


async def on_startup(bot: Bot) -> None:
    global poller_task
    if CFG.tmdb_token:
        try:
            await tmdb.ensure_genres(force=False)
        except Exception:
            log.exception("TMDB genre sync failed on startup")
    try:
        updated_preset_subs = db.rollout_existing_preset_subscriptions(PRESET_ROLLOUT_VERSION)
        if updated_preset_subs:
            log.info("Preset rollout applied to %s existing subscriptions", updated_preset_subs)
    except Exception:
        log.exception("Preset rollout failed on startup")
    poller_task = asyncio.create_task(poller(bot))
    log.info("Bot started")


async def on_shutdown(*_: Any) -> None:
    global poller_task
    if poller_task:
        poller_task.cancel()
        try:
            await poller_task
        except Exception:
            pass
    await tmdb.close()
    await cache.close()
    await source.close()
    log.info("Bot stopped")


async def main() -> None:
    global bot_instance
    bot_instance = Bot(CFG.bot_token)
    dp = Dispatcher()
    dp.include_router(router)
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot_instance)


if __name__ == "__main__":
    asyncio.run(main())
