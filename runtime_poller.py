import asyncio
import logging
from typing import Any, Dict, List

from aiogram import Bot

from config import CFG
from content_buckets import item_content_bucket
from delivery_sender import send_item_to_user
from kinozal_details import enrich_kinozal_item_with_details
from media_detection import is_non_video_release
from release_versioning import describe_variant_change
from source_health import note_source_cycle_failure, note_source_cycle_success
from subscription_matching import match_subscription
from utils import compact_spaces

log = logging.getLogger(__name__)


async def process_new_items(db: Any, source: Any, tmdb: Any, bot: Bot) -> None:
    items = await source.fetch_latest()
    if not items:
        log.info("Source returned no items")
        return

    first_run_seen = db.get_meta("bootstrap_done") == "1"
    touched_item_ids: List[int] = []
    new_item_ids: List[int] = []
    live_items_by_id: Dict[int, Dict[str, Any]] = {}

    for raw_item in items:
        source_text = f"{raw_item.get('source_title') or ''} {raw_item.get('source_description') or ''}"
        if raw_item.get("media_type") == "other" or is_non_video_release(source_text):
            log.info("Skip non-video item: %s", raw_item.get("source_title"))
            continue

        enriched = await tmdb.enrich_item(dict(raw_item))
        if not enriched.get("tmdb_id") and compact_spaces(str(enriched.get("source_category_name") or "")):
            log.info(
                "TMDB no match, using source category fallback title=%s category=%s bucket=%s media=%s",
                enriched.get("source_title"),
                enriched.get("source_category_name"),
                item_content_bucket(enriched),
                enriched.get("media_type"),
            )

        item_id, is_new, materially_changed = db.save_item(enriched)
        enriched["id"] = item_id
        live_items_by_id[item_id] = enriched

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
        item = live_items_by_id.get(item_id) or db.get_item(item_id)
        if not item:
            continue

        matches_by_user: Dict[int, List[Dict[str, Any]]] = {}
        for sub in enabled_subs:
            tg_user_id = int(sub["tg_user_id"])
            if db.delivered(tg_user_id, item_id) or db.delivered_equivalent(tg_user_id, item):
                continue
            if not match_subscription(db, sub, item):
                continue
            matches_by_user.setdefault(tg_user_id, []).append(sub)

        if not matches_by_user:
            continue

        try:
            item = await enrich_kinozal_item_with_details(dict(item))
        except Exception:
            log.warning("Failed to enrich item with kinozal details item_id=%s", item_id, exc_info=True)

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

                await send_item_to_user(db, bot, tg_user_id, item, matched_subs)
                db.record_delivery(
                    tg_user_id,
                    item_id,
                    int(matched_subs[0]["id"]),
                    [int(sub["id"]) for sub in matched_subs],
                )
                await asyncio.sleep(0.12)
            except Exception:
                log.exception("Failed to deliver item=%s to user=%s", item_id, tg_user_id)


async def poller(db: Any, source: Any, tmdb: Any, bot: Bot) -> None:
    while True:
        try:
            await process_new_items(db, source, tmdb, bot)
            await note_source_cycle_success(db, bot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await note_source_cycle_failure(db, bot, exc)
            log.exception("Poller cycle failed")
        await asyncio.sleep(CFG.poll_seconds)
