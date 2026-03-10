import json
import logging
from typing import Any, Dict, List, Optional

import httpx

from config import CFG
from utils import parse_dt, compact_spaces, strip_html
from parsing_basic import parse_year, parse_format, parse_imdb_id
from source_categories import normalize_source_category_id, resolve_source_category_name, source_category_forced_media_type
from release_versioning import parse_episode_progress, extract_kinozal_id, build_source_uid, build_version_signature
from media_detection import detect_media_type
from title_prep import clean_release_title
from parsing_audio import parse_audio_tracks, infer_release_type


log = logging.getLogger(__name__)


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
