import logging
import re
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from kinozal_http import close_kinozal_http, fetch_kinozal_html
from episode_progress import parse_episode_progress
from parsing_audio import infer_release_type, parse_audio_tracks
from parsing_basic import parse_format, parse_year
from parsed_release import parse_release_title
from source_categories import normalize_source_category_id, resolve_source_category_name

log = logging.getLogger("kinozal-source")


def _compact(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _safe_int(value: Any) -> int:
    raw = _compact(value)
    if not raw:
        return 0
    m = re.search(r"\d+", raw)
    return int(m.group(0)) if m else 0


def _strip_tags(value: str) -> str:
    text = re.sub(r"(?is)<br\s*/?>", " ", value or "")
    text = re.sub(r"(?is)<[^>]+>", " ", text)
    text = unescape(text)
    return _compact(text)


def _normalize_category_fields(category_id: Any) -> Dict[str, Any]:
    normalized_id = normalize_source_category_id(category_id)
    if normalized_id is None:
        return {"source_category_id": "", "source_category_name": ""}
    return {
        "source_category_id": str(normalized_id),
        "source_category_name": resolve_source_category_name(normalized_id, "") or "",
    }


def _enrich_title_fields(title: str) -> Dict[str, Any]:
    title = _compact(title)
    parsed = parse_release_title(title)
    return {
        "source_year": parsed.year or parse_year(title),
        "source_format": parsed.resolution or parse_format(title) or "",
        "source_audio_tracks": parsed.audio_tracks or parse_audio_tracks(title),
        "source_episode_progress": parsed.episode_progress_text or parse_episode_progress(title) or "",
        "source_release_type": parsed.release_type or infer_release_type(title) or "",
        "parsed_release_json": parsed.to_json(),
    }


class KinozalSource:
    def __init__(self, base_url: str = ""):
        self.base_url = _compact(base_url).rstrip("/")
        self.direct_url = "https://kinozal.tv/browse.php?s=&page=0&c=0&d=0&v=0"

    async def close(self) -> None:
        await close_kinozal_http()

    async def fetch_latest(self) -> List[Dict[str, Any]]:
        try:
            html = await fetch_kinozal_html(self.direct_url)
        except Exception:
            log.exception("KinozalSource: direct fetch failed")
            return []

        if "details.php?id=" not in html:
            title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
            title = _strip_tags(title_match.group(1)) if title_match else ""
            log.warning(
                "KinozalSource: html contains no details links title=%s has_t_peer=%s",
                title,
                bool(re.search(r"t_peer", html, flags=re.I)),
            )
            return []

        row_matches = re.findall(r"(?is)<tr\b[^>]*>.*?</tr>", html)
        items: List[Dict[str, Any]] = []

        for row_html in row_matches:
            if "details.php?id=" not in row_html:
                continue
            item = self._parse_direct_row(row_html)
            if item:
                items.append(item)

        log.info("KinozalSource: using direct html parser items=%s", len(items))
        return items

    def _parse_direct_row(self, row_html: str) -> Optional[Dict[str, Any]]:
        link_match = re.search(
            r'href=["\'](/details\.php\?id=(\d+))["\'][^>]*>(.*?)</a>',
            row_html,
            flags=re.I | re.S,
        )
        if not link_match:
            return None

        rel_link = link_match.group(1)
        kinozal_id = link_match.group(2)
        title = _strip_tags(link_match.group(3))
        if not title or not kinozal_id:
            return None

        s_cells = [
            _strip_tags(x)
            for x in re.findall(r'(?is)<td[^>]*class=["\']s["\'][^>]*>(.*?)</td>', row_html)
        ]
        comments = _safe_int(s_cells[0]) if len(s_cells) > 0 else 0
        size = s_cells[1] if len(s_cells) > 1 else ""
        date_raw = s_cells[2] if len(s_cells) > 2 else ""

        seeds_match = re.search(r'(?is)<td[^>]*class=["\']sl_s["\'][^>]*>(.*?)</td>', row_html)
        peers_match = re.search(r'(?is)<td[^>]*class=["\']sl_p["\'][^>]*>(.*?)</td>', row_html)
        uploader_match = re.search(
            r'(?is)<a[^>]*href=["\']/userdetails\.php\?id=\d+["\'][^>]*>(.*?)</a>',
            row_html,
        )
        cat_match = re.search(r'onclick=["\']cat\((\d+)\);["\']', row_html, flags=re.I)

        seeds = _safe_int(_strip_tags(seeds_match.group(1)) if seeds_match else "")
        peers = _safe_int(_strip_tags(peers_match.group(1)) if peers_match else "")
        uploader = _strip_tags(uploader_match.group(1)) if uploader_match else ""
        category_id = int(cat_match.group(1)) if cat_match else None
        category_fields = _normalize_category_fields(category_id)

        item = {
            "source_id": str(kinozal_id),
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": title,
            "source_link": urljoin("https://kinozal.tv/", rel_link),
            "source_description": "",
            "source_category_id": category_fields["source_category_id"],
            "source_category_name": category_fields["source_category_name"],
            "source_date_raw": date_raw,
            "source_size": size,
            "source_comments": comments,
            "source_seeds": seeds,
            "source_peers": peers,
            "source_uploader": uploader,
        }
        item.update(_enrich_title_fields(title))
        return item
