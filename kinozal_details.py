import json
import logging
import re
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from kinozal_http import KINOZAL_BASE, fetch_kinozal_html
from parsing_basic import parse_imdb_id

log = logging.getLogger("kinozal-details")

_DETAILS_CACHE: Dict[str, Dict[str, Any]] = {}


def _compact(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _strip_tags(value: str) -> str:
    text = value or ""
    text = re.sub(r"(?is)<br\s*/?>", "\n", text)
    text = re.sub(r"(?is)</tr\s*>", "\n", text)
    text = re.sub(r"(?is)</td\s*>", " ", text)
    text = re.sub(r"(?is)</div\s*>", "\n", text)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)</li\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def _extract_kinozal_id(link: str) -> str:
    match = re.search(r"details\.php\?id=(\d+)", link or "", flags=re.I)
    return match.group(1) if match else ""


def _build_magnet_link(info_hash: str, title: str) -> str:
    clean_hash = _compact(info_hash).upper()
    clean_title = _compact(title) or "kinozal"
    if not clean_hash:
        return ""
    return f"magnet:?xt=urn:btih:{clean_hash}&dn={quote(clean_title)}"


def _extract_info_hash(html: str, text: str) -> str:
    patterns = [
        r"Инфо[\s\-]*хеш(?:</[^>]+>|<[^>]+>|\s|&nbsp;|:)*([A-Fa-f0-9]{40})",
        r"Info[\s\-]*hash(?:</[^>]+>|<[^>]+>|\s|&nbsp;|:)*([A-Fa-f0-9]{40})",
        r"Инфо[\s\-]*хеш[:\s]*([A-Fa-f0-9]{40})",
        r"Info[\s\-]*hash[:\s]*([A-Fa-f0-9]{40})",
        r"\b([A-Fa-f0-9]{40})\b",
    ]
    for pattern in patterns:
        m = re.search(pattern, html, flags=re.I | re.S)
        if m:
            return m.group(1).upper()
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I | re.S)
        if m:
            return m.group(1).upper()
    return ""


def _extract_file_count(html: str, text: str) -> Optional[int]:
    patterns = [
        r"Список\s+файлов\s+всего\s*(\d+)",
        r"Files\s+total\s*(\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, html, flags=re.I | re.S)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I | re.S)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def _extract_file_lines(html: str, text: str) -> List[str]:
    out: List[str] = []

    patterns = [
        r'>([^<>\n\r]{1,260}\.(?:mkv|mp4|avi|m2ts|ts|m4v|mov|wmv|mpg|mpeg|iso|img|flac|mp3|aac|ac3|dts|mka|srt|ass|ssa|sub|idx|exe|bin|cue|zip|rar|7z))<',
    ]
    for pattern in patterns:
        for match in re.findall(pattern, html, flags=re.I | re.S):
            line = _compact(unescape(match))
            if line and line not in out:
                out.append(line)

    for raw_line in text.splitlines():
        line = _compact(raw_line)
        if not line:
            continue
        if re.search(
            r"\.(mkv|mp4|avi|m2ts|ts|m4v|mov|wmv|mpg|mpeg|iso|img|flac|mp3|aac|ac3|dts|mka|srt|ass|ssa|sub|idx|exe|bin|cue|zip|rar|7z)\b",
            line,
            flags=re.I,
        ):
            if line not in out:
                out.append(line)

    return out[:200]


def _extract_release_tab_index(main_html: str, kinozal_id: str) -> Optional[int]:
    if not main_html or not kinozal_id:
        return None

    patterns = [
        rf'(?is)<a[^>]+onclick=["\']showtab\(\s*{re.escape(kinozal_id)}\s*,\s*(\d+)\s*\)\s*;?\s*return false;?["\'][^>]*>\s*Релиз\s*</a>',
        rf'(?is)<li[^>]*>\s*<a[^>]+onclick=["\']showtab\(\s*{re.escape(kinozal_id)}\s*,\s*(\d+)\s*\)\s*;?\s*return false;?["\'][^>]*>.*?Релиз.*?</a>\s*</li>',
    ]
    for pattern in patterns:
        m = re.search(pattern, main_html)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None


def _repair_utf8_as_cp1251_mojibake(text: str) -> str:
    text = str(text or "")
    if not text:
        return ""

    def marker_score(value: str) -> int:
        low = value.lower()
        score = 0

        good_markers = [
            "аудио",
            "субтитры",
            "перевод",
            "перевод и озвучивание",
            "озвучивание",
            "релиз:",
            "без рекламы",
            "реклама",
            "любительск",
            "профессиональ",
            "дублирован",
            "многоголос",
            "одноголос",
            "двухголос",
            "закадров",
            "роли дублировали",
            "качество:",
            "видео:",
        ]
        for marker in good_markers:
            if marker in low:
                score += 10

        score -= len(re.findall(r"[РС][Ѓ-џ]", value)) * 4
        score -= value.count("Р›С")
        score -= value.count("РЎС")
        score += len(re.findall(r"[А-Яа-яЁё]", value))
        return score

    try:
        fixed = text.encode("cp1251", errors="ignore").decode("utf-8", errors="ignore")
    except Exception:
        return text

    return fixed if marker_score(fixed) > marker_score(text) else text


def _extract_release_text_from_tab_html(tab_html: str) -> str:
    raw_html = str(tab_html or "")
    if not raw_html:
        return ""

    def normalize_lines(text: str) -> str:
        lines: List[str] = []

        for raw_line in str(text or "").splitlines():
            line = _compact(raw_line)
            if not line:
                continue

            if line in {
                "Релиз",
                "Скриншоты",
                "Техданные",
                "Интересно",
                "Награды",
                "Загрузка...",
            }:
                continue

            lines.append(line)

        # убираем только подряд идущие полные дубли, если такие внезапно есть
        collapsed: List[str] = []
        prev = None
        for line in lines:
            if line == prev:
                continue
            collapsed.append(line)
            prev = line

        return "\n".join(collapsed[:200]).strip()

    plain_text_raw = _strip_tags(raw_html)
    repaired_text_raw = _repair_utf8_as_cp1251_mojibake(plain_text_raw)

    plain_text = normalize_lines(plain_text_raw)
    repaired_text = normalize_lines(repaired_text_raw)

    return repaired_text if _score_release_text(repaired_text) > _score_release_text(plain_text) else plain_text


def _score_release_text(text: str) -> int:
    raw = str(text or "").strip()
    if not raw:
        return 0

    score = 0
    lowered = raw.lower()
    lines = [line.strip() for line in raw.splitlines() if line.strip()]

    audio_line_re = re.compile(
        r'^\s*(?:аудио\b|audio\b|оригинальная\s+аудиодорожка\b|original(?:\s+audio)?\b)',
        flags=re.I,
    )
    translate_line_re = re.compile(
        r'^\s*(?:перевод(?:\s+и\s+озвучивание)?|озвучивание|озвучка)\s*:',
        flags=re.I,
    )
    numbered_voice_line_re = re.compile(
        r'^\s*\d+\.\s*(?:любительск|профессиональ|дублирован|авторск)',
        flags=re.I,
    )

    audio_count = sum(1 for line in lines if audio_line_re.match(line))
    translate_count = sum(1 for line in lines if translate_line_re.match(line))
    numbered_voice_count = sum(1 for line in lines if numbered_voice_line_re.match(line))

    score += audio_count * 6
    score += translate_count * 5
    score += numbered_voice_count * 5

    extra_markers = [
        "субтитры",
        "без рекламы",
        "реклама",
        "реклама отсутствует",
        "релиз:",
        "автор релиза",
        "роли дублировали",
        "контейнер",
        "видео:",
        "формат:",
        "качество:",
        "источник:",
        "перевод добавлен отдельным файлом",
        "сериал озвучен",
        "размер:",
        "продолжительность:",
        "любительск",
        "профессиональ",
        "дублирован",
        "многоголос",
        "одноголос",
        "двухголос",
        "закадров",
    ]
    for marker in extra_markers:
        if marker in lowered:
            score += 2

    bad_markers = [
        "оскар",
        "золотой глобус",
        "британская академия",
        "номинац",
        "премия",
        "фильм снят",
        "снят по",
        "интересные факты",
        "награды",
    ]
    for marker in bad_markers:
        if marker in lowered:
            score -= 8

    if audio_count == 0 and translate_count == 0 and numbered_voice_count == 0 and not any(
        marker in lowered
        for marker in (
            "субтитры",
            "без рекламы",
            "реклама",
            "релиз:",
            "роли дублировали",
            "перевод добавлен отдельным файлом",
            "сериал озвучен",
            "качество:",
            "видео:",
            "перевод:",
            "размер:",
            "продолжительность:",
            "любительск",
            "профессиональ",
            "дублирован",
        )
    ):
        score -= 12

    return score


async def _fetch_best_release_text(kinozal_id: str, source_link: str, main_html: str) -> str:
    def useful(text: str) -> bool:
        return _score_release_text(text) > 0

    try:
        body = await fetch_kinozal_html(f"{KINOZAL_BASE}/get_srv_details.php?id={kinozal_id}&pagesd=0")
        parsed = _extract_release_text_from_tab_html(body)
        if useful(parsed):
            log.info("Selected release source for %s mode=pagesd idx=0 score=%s", source_link, _score_release_text(parsed))
            return parsed
    except Exception:
        log.warning("Failed release fetch for %s mode=pagesd idx=0", source_link, exc_info=True)

    release_tab_index = _extract_release_tab_index(main_html, kinozal_id)
    if release_tab_index is not None and release_tab_index != 0:
        try:
            body = await fetch_kinozal_html(f"{KINOZAL_BASE}/get_srv_details.php?id={kinozal_id}&pagesd={release_tab_index}")
            parsed = _extract_release_text_from_tab_html(body)
            if useful(parsed):
                log.info(
                    "Selected release source for %s mode=pagesd idx=%s score=%s",
                    source_link,
                    release_tab_index,
                    _score_release_text(parsed),
                )
                return parsed
        except Exception:
            log.warning(
                "Failed release fetch for %s mode=pagesd idx=%s",
                source_link,
                release_tab_index,
                exc_info=True,
            )

    for idx in range(1, 8):
        if idx == release_tab_index:
            continue
        try:
            body = await fetch_kinozal_html(f"{KINOZAL_BASE}/get_srv_details.php?id={kinozal_id}&pagesd={idx}")
            parsed = _extract_release_text_from_tab_html(body)
            if useful(parsed):
                log.info("Selected release source for %s mode=pagesd idx=%s score=%s", source_link, idx, _score_release_text(parsed))
                return parsed
        except Exception:
            log.warning(
                "Failed release fetch for %s mode=pagesd idx=%s",
                source_link,
                idx,
                exc_info=True,
            )

    return ""


async def enrich_kinozal_item_with_details(item: Dict[str, Any]) -> Dict[str, Any]:
    source_link = _compact(item.get("source_link") or "")
    if not source_link:
        return item

    cached = _DETAILS_CACHE.get(source_link)
    if cached:
        merged = dict(item)
        merged.update(cached)
        return merged

    kinozal_id = _extract_kinozal_id(source_link)
    if not kinozal_id:
        return item

    main_html = ""
    ajax_html = ""
    main_text = ""
    ajax_text = ""
    source_imdb_id = ""
    release_text = ""

    try:
        main_html = await fetch_kinozal_html(source_link)
        main_text = _strip_tags(main_html)
        source_imdb_id = parse_imdb_id(main_html) or ""
    except Exception:
        log.warning("Failed to fetch kinozal details page for %s", source_link, exc_info=True)

    try:
        ajax_html = await fetch_kinozal_html(f"{KINOZAL_BASE}/get_srv_details.php?id={kinozal_id}&action=2")
        ajax_text = _strip_tags(ajax_html)
    except Exception:
        log.warning("Failed to fetch kinozal ajax file list for %s", source_link, exc_info=True)

    try:
        release_text = await _fetch_best_release_text(kinozal_id, source_link, main_html)
    except Exception:
        log.warning("Failed to fetch best release text for %s", source_link, exc_info=True)
        release_text = ""

    info_hash = _extract_info_hash(ajax_html, ajax_text) or _extract_info_hash(main_html, main_text)
    file_count = _extract_file_count(main_html, main_text) or _extract_file_count(ajax_html, ajax_text)
    file_lines = _extract_file_lines(ajax_html, ajax_text) or _extract_file_lines(main_html, main_text)

    if not file_count and file_lines:
        file_count = len(file_lines)

    extra: Dict[str, Any] = {
        "source_imdb_id": source_imdb_id or "",
        "source_info_hash": info_hash or "",
        "source_file_count": file_count,
        "source_file_list": file_lines,
        "source_file_list_json": json.dumps(file_lines, ensure_ascii=False) if file_lines else "",
        "source_magnet": _build_magnet_link(info_hash, item.get("source_title") or "") if info_hash else "",
        "source_release_text": release_text or "",
    }

    _DETAILS_CACHE[source_link] = dict(extra)
    if len(_DETAILS_CACHE) > 512:
        try:
            _DETAILS_CACHE.pop(next(iter(_DETAILS_CACHE)))
        except Exception:
            pass

    merged = dict(item)
    merged.update(extra)
    return merged
