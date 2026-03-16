import json
import logging
import re
from html import unescape
from typing import Any, Dict, List
from urllib.parse import quote

from kinozal_http import KINOZAL_BASE, fetch_kinozal_html

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
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def _build_magnet_link(info_hash: str, title: str) -> str:
    clean_hash = _compact(info_hash).upper()
    clean_title = _compact(title) or "kinozal"
    if not clean_hash:
        return ""
    return f"magnet:?xt=urn:btih:{clean_hash}&dn={quote(clean_title)}"


def _extract_kinozal_id(link: str) -> str:
    m = re.search(r"details\.php\?id=(\d+)", link or "", flags=re.I)
    return m.group(1) if m else ""


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


def _extract_file_count(html: str, text: str) -> int | None:
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

    blob = text
    split_pattern = re.compile(
        r'([A-Za-zА-Яа-я0-9_ .\-/\[\]\(\)]+?\.(?:mkv|mp4|avi|m2ts|ts|m4v|mov|wmv|mpg|mpeg|iso|img|flac|mp3|aac|ac3|dts|mka|srt|ass|ssa|sub|idx|exe|bin|cue|zip|rar|7z))(?:\s+[0-9.,]+\s+[КМГТkMGT][Бb])?(?:\s+\([0-9]+\))?',
        flags=re.I,
    )
    for match in split_pattern.findall(blob):
        line = _compact(match)
        if line and line not in out:
            out.append(line)

    for raw_line in text.splitlines():
        line = _compact(raw_line)
        if not line:
            continue
        if re.search(r"\.(mkv|mp4|avi|m2ts|ts|m4v|mov|wmv|mpg|mpeg|iso|img|flac|mp3|aac|ac3|dts|mka|srt|ass|ssa|sub|idx|exe|bin|cue|zip|rar|7z)\b", line, flags=re.I):
            if line not in out:
                out.append(line)


    # Убираем склеенные строки, где оказалось несколько имён файлов подряд,
    # если у нас уже есть нормальные отдельные элементы.
    file_name_re = re.compile(
        r'[^\s]+\.(?:mkv|mp4|avi|m2ts|ts|m4v|mov|wmv|mpg|mpeg|iso|img|flac|mp3|aac|ac3|dts|mka|srt|ass|ssa|sub|idx|exe|bin|cue|zip|rar|7z)\b',
        flags=re.I,
    )
    cleaned = []
    for line in out:
        names = file_name_re.findall(line)
        if len(names) >= 2:
            continue
        cleaned.append(line)

    if cleaned:
        out = cleaned

    return out[:200]


async def enrich_kinozal_item_with_details(item: Dict[str, Any]) -> Dict[str, Any]:
    source_link = _compact(item.get("source_link") or "")
    if not source_link:
        return item

    if item.get("source_info_hash") and item.get("source_magnet"):
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

    try:
        main_html = await fetch_kinozal_html(source_link)
        main_text = _strip_tags(main_html)
    except Exception:
        log.warning("Failed to fetch kinozal details page for %s", source_link, exc_info=True)

    try:
        ajax_url = f"{KINOZAL_BASE}/get_srv_details.php?id={kinozal_id}&action=2"
        ajax_html = await fetch_kinozal_html(ajax_url)
        ajax_text = _strip_tags(ajax_html)
    except Exception:
        log.warning("Failed to fetch kinozal ajax file list for %s", source_link, exc_info=True)

    info_hash = _extract_info_hash(ajax_html, ajax_text) or _extract_info_hash(main_html, main_text)
    file_count = _extract_file_count(main_html, main_text) or _extract_file_count(ajax_html, ajax_text)
    file_lines = _extract_file_lines(ajax_html, ajax_text) or _extract_file_lines(main_html, main_text)

    if not file_count and file_lines:
        file_count = len(file_lines)

    if not info_hash:
        log.info(
            "Kinozal ajax/details without info-hash link=%s ajax_len=%s files=%s",
            source_link,
            len(ajax_html or ""),
            len(file_lines),
        )

    extra: Dict[str, Any] = {
        "source_info_hash": info_hash or "",
        "source_file_count": file_count,
        "source_file_list": file_lines,
        "source_file_list_json": json.dumps(file_lines, ensure_ascii=False) if file_lines else "",
        "source_magnet": _build_magnet_link(info_hash, item.get("source_title") or "") if info_hash else "",
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
