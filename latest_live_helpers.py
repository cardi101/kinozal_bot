import logging
from typing import Any, Dict, List

from kinozal_details import enrich_kinozal_item_with_details

log = logging.getLogger("latest-live")


async def get_live_latest_items(source: Any, tmdb: Any, limit: int = 5) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    seen = set()

    try:
        fresh_items = await source.fetch_latest()
    except Exception:
        log.exception("latest-live: source.fetch_latest failed")
        return []

    for raw_item in fresh_items:
        source_id = str(raw_item.get("source_id") or raw_item.get("source_uid") or "")
        if source_id and source_id in seen:
            continue
        if source_id:
            seen.add(source_id)

        try:
            enriched = await tmdb.enrich_item(dict(raw_item))
        except Exception:
            log.exception("latest-live: tmdb.enrich_item failed for source_id=%s", source_id)
            enriched = dict(raw_item)

        try:
            enriched = await enrich_kinozal_item_with_details(dict(enriched))
        except Exception:
            log.warning("latest-live: details enrich failed for source_id=%s", source_id, exc_info=True)

        items.append(enriched)
        if len(items) >= limit:
            break

    return items
