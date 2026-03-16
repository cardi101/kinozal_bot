import json
import logging
import re
from html import unescape
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import httpx

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


def _decode_html_bytes(raw: bytes) -> str:
    for enc in ("cp1251", "windows-1251", "utf-8"):
        try:
            return raw.decode(enc)
        except Exception:
            pass
    return raw.decode("utf-8", errors="replace")


def _extract_kinozal_id_from_link(link: str) -> Optional[str]:
    m = re.search(r"details\.php\?id=(\d+)", link or "", flags=re.I)
    return m.group(1) if m else None


class KinozalSource:
    def __init__(self, base_url: str):
        self.base_url = _compact(base_url).rstrip("/")
        self.direct_url = "https://kinozal.tv/browse.php?s=&page=0&c=0&d=0&v=0"
        self.user_agent = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/145.0.0.0 Safari/537.36"
        )
        self.client = httpx.AsyncClient(
            timeout=60,
            follow_redirects=True,
            headers={"User-Agent": self.user_agent},
        )

    async def close(self) -> None:
        await self.client.aclose()

    async def fetch_latest(self) -> List[Dict[str, Any]]:
        torapi_items = await self._fetch_via_torapi()
        if torapi_items:
            log.info("KinozalSource: using torapi result items=%s", len(torapi_items))
            return torapi_items

        direct_items = await self._fetch_direct()
        log.info("KinozalSource: using direct html parser items=%s", len(direct_items))
        return direct_items

    async def _fetch_via_torapi(self) -> List[Dict[str, Any]]:
        if not self.base_url:
            return []

        url = f"{self.base_url}/api/get/rss/kinozal"
        try:
            r = await self.client.get(url)
            r.raise_for_status()

            payload: Any
            try:
                payload = r.json()
            except Exception:
                payload = json.loads(r.text)

            if isinstance(payload, dict):
                if _compact(payload.get("Result")) == "Server is not available":
                    log.warning("KinozalSource: torapi unavailable response")
                    return []
                log.warning("KinozalSource: unexpected torapi dict payload keys=%s", list(payload.keys())[:20])
                return []

            if not isinstance(payload, list):
                log.warning("KinozalSource: unexpected torapi payload type=%s", type(payload).__name__)
                return []

            items: List[Dict[str, Any]] = []
            for raw_item in payload:
                item = self._normalize_torapi_item(raw_item)
                if item:
                    items.append(item)

            return items
        except Exception:
            log.exception("KinozalSource: torapi fetch failed")
            return []

    async def _fetch_direct(self) -> List[Dict[str, Any]]:
        try:
            r = await self.client.get(self.direct_url)
            r.raise_for_status()
            html = _decode_html_bytes(r.content)
        except Exception:
            log.exception("KinozalSource: direct fetch failed")
            return []

        if "details.php?id=" not in html:
            title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.I | re.S)
            title = _strip_tags(title_match.group(1)) if title_match else ""
            log.warning(
                "KinozalSource: direct html contains no details links title=%s has_t_peer=%s",
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

        return items

    def _normalize_torapi_item(self, raw_item: Any) -> Optional[Dict[str, Any]]:
        if not isinstance(raw_item, dict):
            return None

        title = (
            _compact(raw_item.get("Name"))
            or _compact(raw_item.get("Title"))
            or _compact(raw_item.get("title"))
            or _compact(raw_item.get("name"))
        )
        if not title:
            return None

        link = (
            _compact(raw_item.get("Url"))
            or _compact(raw_item.get("URL"))
            or _compact(raw_item.get("Link"))
            or _compact(raw_item.get("link"))
        )
        if link and link.startswith("/"):
            link = urljoin("https://kinozal.tv/", link)
        if not link:
            maybe_id = _compact(raw_item.get("Id") or raw_item.get("ID") or raw_item.get("id"))
            if maybe_id.isdigit():
                link = f"https://kinozal.tv/details.php?id={maybe_id}"

        kinozal_id = _extract_kinozal_id_from_link(link) or _compact(raw_item.get("Id") or raw_item.get("id"))
        if not kinozal_id:
            return None

        category_id = _safe_int(
            raw_item.get("Category_Id")
            or raw_item.get("CategoryID")
            or raw_item.get("CatId")
            or raw_item.get("Category")
        )

        category_name = _compact(
            raw_item.get("Category_Name")
            or raw_item.get("CategoryName")
            or raw_item.get("Category")
        )

        uploader = _compact(
            raw_item.get("Upload_By")
            or raw_item.get("Uploader")
            or raw_item.get("Author")
        )

        return {
            "source_id": str(kinozal_id),
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": title,
            "source_link": link,
            "source_description": "",
            "source_category_id": category_id or None,
            "source_category_name": category_name or "",
            "source_date_raw": _compact(raw_item.get("Date") or raw_item.get("Published") or raw_item.get("PubDate")),
            "source_size": _compact(raw_item.get("Size")),
            "source_comments": _safe_int(raw_item.get("Comments")),
            "source_seeds": _safe_int(raw_item.get("Seeds")),
            "source_peers": _safe_int(raw_item.get("Peers")),
            "source_uploader": uploader,
        }

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

        return {
            "source_id": str(kinozal_id),
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": title,
            "source_link": urljoin("https://kinozal.tv/", rel_link),
            "source_description": "",
            "source_category_id": category_id,
            "source_category_name": "",
            "source_date_raw": date_raw,
            "source_size": size,
            "source_comments": comments,
            "source_seeds": seeds,
            "source_peers": peers,
            "source_uploader": uploader,
        }
