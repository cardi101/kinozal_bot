import logging
from typing import Any, Dict, List

from media_detection import is_non_video_release, is_russian_release
from subscription_matching import match_subscription
from kinozal_details import enrich_kinozal_item_with_details


log = logging.getLogger(__name__)


async def get_live_test_items_for_subscription(db: Any, source: Any, tmdb: Any, sub_id: int, limit: int = 5) -> List[Dict[str, Any]]:
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
        if is_russian_release(raw_item):
            continue
        source_text = f"{raw_item.get('source_title') or ''} {raw_item.get('source_description') or ''}"
        if raw_item.get("media_type") == "other" or is_non_video_release(source_text):
            continue
        item = dict(raw_item)
        item = await enrich_kinozal_item_with_details(dict(item))
        try:
            item = await tmdb.enrich_item(item)
        except Exception:
            log.warning(
                "TMDB enrich failed during subscription test for sub=%s title=%s",
                sub_id,
                item.get("source_title"),
                exc_info=True,
            )

        if match_subscription(db, sub, item):
            matched.append(item)

        if len(matched) >= limit:
            break

    return matched
