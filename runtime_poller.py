import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

from aiogram import Bot

from config import CFG
from content_buckets import item_content_bucket
from delivery_sender import send_grouped_items_to_user, send_item_to_user
from kinozal_details import enrich_kinozal_item_with_details
from media_detection import is_non_video_release
from release_versioning import describe_variant_change, extract_kinozal_id, parse_episode_progress
from source_health import note_source_cycle_failure, note_source_cycle_success
from subscription_matching import match_subscription
from utils import compact_spaces

log = logging.getLogger(__name__)


def _quiet_active(start_h: int, end_h: int, current_h: int) -> bool:
    if start_h < end_h:
        return start_h <= current_h < end_h
    return current_h >= start_h or current_h < end_h


async def _send_single(db: Any, bot: Bot, tg_user_id: int, d: Dict[str, Any]) -> None:
    item = d["item"]
    item_id = d["item_id"]
    matched_subs = d["subs"]
    previous_item = db.get_latest_delivered_related_item(tg_user_id, item)
    if previous_item:
        log.info(
            "Delivering updated release item=%s to user=%s source_uid=%s reason=%s prev_item_id=%s",
            item_id, tg_user_id, item.get("source_uid"),
            describe_variant_change(previous_item, item), previous_item.get("id"),
        )
    else:
        log.info("Delivering new release item=%s to user=%s source_uid=%s",
                 item_id, tg_user_id, item.get("source_uid"))
    await send_item_to_user(db, bot, tg_user_id, item, matched_subs)
    db.record_delivery(tg_user_id, item_id,
                       int(matched_subs[0]["id"]), [int(s["id"]) for s in matched_subs])
    await asyncio.sleep(0.12)


async def process_new_items(db: Any, source: Any, tmdb: Any, bot: Bot) -> None:
    items = await source.fetch_latest()
    if not items:
        log.info("Source returned no items")
        return

    first_run_seen = db.get_meta("bootstrap_done") == "1"
    touched_item_ids: List[int] = []
    new_item_ids: List[int] = []
    live_items_by_id: Dict[int, Dict[str, Any]] = {}
    release_text_changed_ids: Set[int] = set()
    old_release_texts: Dict[int, str] = {}

    for raw_item in items:
        source_text = f"{raw_item.get('source_title') or ''} {raw_item.get('source_description') or ''}"
        if raw_item.get("media_type") == "other" or is_non_video_release(source_text):
            log.info("Skip non-video item: %s", raw_item.get("source_title"))
            continue
        category = str(raw_item.get("source_category_name") or "")
        title = str(raw_item.get("source_title") or "")
        if any(kw in category for kw in ("Русский", "Русская", "Русское", "Наше Кино")) or "/ РУ /" in title:
            log.info("Skip Russian item: %s [%s]", title, category)
            continue

        cached = db.find_existing_enriched(raw_item.get("source_uid"), raw_item.get("source_title"))
        if cached:
            enriched = dict(raw_item)
            for key, value in cached.items():
                if key.startswith("tmdb_") or key in ("imdb_id", "mal_id", "media_type", "cleaned_title"):
                    if value is not None and not enriched.get(key):
                        enriched[key] = value
        else:
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

        if not is_new and first_run_seen and db.was_delivered_to_anyone(item_id):
            stored_item = db.get_item(item_id)
            stored_release_text = (stored_item.get("source_release_text") or "") if stored_item else ""
            try:
                detail_enriched = await enrich_kinozal_item_with_details(dict(enriched), force_refresh=True)
                fresh_release_text = detail_enriched.get("source_release_text") or ""
                if fresh_release_text and fresh_release_text != stored_release_text:
                    db.update_item_release_text(item_id, fresh_release_text)
                    enriched["source_release_text"] = fresh_release_text
                    live_items_by_id[item_id] = enriched
                    if stored_release_text:
                        old_release_texts[item_id] = stored_release_text
                        release_text_changed_ids.add(item_id)
                        if item_id not in touched_item_ids:
                            touched_item_ids.append(item_id)
                        log.info("Release text changed for item=%s source_uid=%s",
                                 item_id, enriched.get("source_uid"))
                    else:
                        release_text_changed_ids.add(item_id)
                        if item_id not in touched_item_ids:
                            touched_item_ids.append(item_id)
                        log.info("Initialized release text baseline for item=%s source_uid=%s",
                                 item_id, enriched.get("source_uid"))

                # Check if the details page title has a newer episode progress
                # than what the browse page reported (browse page can be stale).
                details_title = detail_enriched.get("details_title") or ""
                if details_title:
                    details_progress = parse_episode_progress(details_title)
                    stored_progress = enriched.get("source_episode_progress") or ""
                    if details_progress and details_progress != stored_progress:
                        log.info(
                            "Details page episode progress differs for item=%s: browse=%s details=%s, creating new version",
                            item_id, stored_progress, details_progress,
                        )
                        enriched["source_title"] = details_title
                        enriched["source_episode_progress"] = details_progress
                        new_item_id, new_is_new, _ = db.save_item(enriched)
                        if new_is_new:
                            enriched["id"] = new_item_id
                            live_items_by_id[new_item_id] = enriched
                            new_item_ids.append(new_item_id)
                            if new_item_id not in touched_item_ids:
                                touched_item_ids.append(new_item_id)
            except Exception:
                log.warning("Failed to check release text for item=%s", item_id, exc_info=True)

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

    # Phase 1: collect all planned deliveries
    all_pending: Dict[int, List[Dict[str, Any]]] = {}

    for item_id in touched_item_ids:
        item = live_items_by_id.get(item_id) or db.get_item(item_id)
        if not item:
            continue

        is_release_text_change = item_id in release_text_changed_ids
        item_tmdb_id = int(item["tmdb_id"]) if item.get("tmdb_id") else None
        kinozal_id = item.get("kinozal_id") or extract_kinozal_id(item.get("source_uid"))
        matches_by_user: Dict[int, List[Dict[str, Any]]] = {}
        for sub in enabled_subs:
            tg_user_id = int(sub["tg_user_id"])
            if item_tmdb_id and db.is_title_muted(tg_user_id, item_tmdb_id):
                continue
            if not is_release_text_change:
                if db.delivered(tg_user_id, item_id) or db.delivered_equivalent(tg_user_id, item):
                    continue
                if kinozal_id and db.recently_delivered_kinozal_id(tg_user_id, kinozal_id, cooldown_seconds=420):
                    continue
            else:
                if not db.delivered(tg_user_id, item_id):
                    continue
                if db.recently_delivered(tg_user_id, item_id, cooldown_seconds=420):
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

        if not is_release_text_change and item.get("source_release_text"):
            db.update_item_release_text(item_id, item["source_release_text"])

        for tg_user_id, matched_subs in matches_by_user.items():
            if kinozal_id and not is_release_text_change:
                sub_ids_str = ",".join(str(s["id"]) for s in matched_subs)
                db.upsert_debounce(tg_user_id, kinozal_id, item_id, sub_ids_str, delay_seconds=120)
                log.info("Debounce queued item=%s kinozal_id=%s to user=%s", item_id, kinozal_id, tg_user_id)
            else:
                all_pending.setdefault(tg_user_id, []).append({
                    "item": item,
                    "item_id": item_id,
                    "subs": matched_subs,
                    "old_release_text": old_release_texts.get(item_id, ""),
                    "is_release_text_change": is_release_text_change,
                })

    # Phase 2: flush due pending deliveries from previous quiet periods
    current_hour = datetime.now(timezone.utc).hour
    due_pending = db.pop_due_pending_deliveries(current_hour)
    for flush_uid, pd_list in due_pending.items():
        for pd in pd_list:
            pd_item_id = int(pd["item_id"])
            if db.delivered(flush_uid, pd_item_id):
                db.delete_pending_delivery(flush_uid, pd_item_id)
                continue
            pd_item = db.get_item(pd_item_id)
            if not pd_item:
                db.delete_pending_delivery(flush_uid, pd_item_id)
                continue
            try:
                pd_item = await enrich_kinozal_item_with_details(dict(pd_item))
            except Exception:
                log.warning("Failed to enrich pending item=%s", pd_item_id, exc_info=True)
            sub_ids = [s.strip() for s in str(pd.get("matched_sub_ids") or "").split(",") if s.strip()]
            pd_subs = [db.get_subscription(int(sid)) for sid in sub_ids if sid.isdigit()]
            pd_subs = [s for s in pd_subs if s]
            try:
                log.info("Flushing pending delivery item=%s to user=%s", pd_item_id, flush_uid)
                await send_item_to_user(db, bot, flush_uid, pd_item, pd_subs,
                                        old_release_text=str(pd.get("old_release_text") or ""))
                db.record_delivery(flush_uid, pd_item_id,
                                   int(pd_subs[0]["id"]) if pd_subs else 0,
                                   [int(s["id"]) for s in pd_subs])
                db.delete_pending_delivery(flush_uid, pd_item_id)
                await asyncio.sleep(0.12)
            except Exception:
                log.exception("Failed to flush pending delivery user=%s item=%s", flush_uid, pd_item_id)

    # Phase 2.5: flush due debounced deliveries into all_pending
    _enriched_cache: Dict[int, Dict[str, Any]] = {}
    for entry in db.pop_due_debounce():
        db_uid = int(entry["tg_user_id"])
        db_item_id = int(entry["item_id"])
        if db.delivered(db_uid, db_item_id):
            continue
        if db_item_id not in _enriched_cache:
            raw = db.get_item(db_item_id)
            if not raw:
                continue
            try:
                _enriched_cache[db_item_id] = await enrich_kinozal_item_with_details(dict(raw), force_refresh=True)
            except Exception:
                log.warning("Failed to enrich debounced item=%s", db_item_id, exc_info=True)
                _enriched_cache[db_item_id] = raw
        db_item = _enriched_cache[db_item_id]
        if db.delivered_equivalent(db_uid, db_item):
            continue
        sub_ids = [s.strip() for s in str(entry.get("matched_sub_ids") or "").split(",") if s.strip()]
        subs = [db.get_subscription(int(sid)) for sid in sub_ids if sid.isdigit()]
        subs = [s for s in subs if s]
        if not subs:
            continue
        log.info("Debounce ready item=%s kinozal_id=%s to user=%s", db_item_id, entry["kinozal_id"], db_uid)
        all_pending.setdefault(db_uid, []).append({
            "item": db_item,
            "item_id": db_item_id,
            "subs": subs,
            "old_release_text": "",
            "is_release_text_change": False,
        })

    if not all_pending:
        return

    # Phase 3: deliver current cycle items per user
    for tg_user_id, deliveries in all_pending.items():
        try:
            q_start, q_end = db.get_user_quiet_hours(tg_user_id)
            if q_start is not None and q_end is not None and _quiet_active(q_start, q_end, current_hour):
                for d in deliveries:
                    sub_ids_str = ",".join(str(s["id"]) for s in d["subs"])
                    db.queue_pending_delivery(tg_user_id, d["item_id"], sub_ids_str,
                                              d["old_release_text"], d["is_release_text_change"])
                log.info("Queued %d deliveries for user=%s (quiet %02d:00-%02d:00 UTC)",
                         len(deliveries), tg_user_id, q_start, q_end)
                continue

            rtc = [d for d in deliveries if d["is_release_text_change"]]
            regular = [d for d in deliveries if not d["is_release_text_change"]]

            tmdb_groups: Dict[int, List[Dict[str, Any]]] = {}
            no_tmdb: List[Dict[str, Any]] = []
            for d in regular:
                tmdb_id = d["item"].get("tmdb_id")
                if tmdb_id:
                    tmdb_groups.setdefault(int(tmdb_id), []).append(d)
                else:
                    no_tmdb.append(d)

            for d in rtc:
                try:
                    log.info("Delivering release text update item=%s to user=%s source_uid=%s",
                             d["item_id"], tg_user_id, d["item"].get("source_uid"))
                    await send_item_to_user(db, bot, tg_user_id, d["item"], d["subs"],
                                            old_release_text=d["old_release_text"])
                    db.record_delivery(tg_user_id, d["item_id"],
                                       int(d["subs"][0]["id"]), [int(s["id"]) for s in d["subs"]])
                    await asyncio.sleep(0.12)
                except Exception:
                    log.exception("Error delivering rtc item=%s to user=%s", d["item_id"], tg_user_id)

            for d in no_tmdb:
                if db.delivered(tg_user_id, d["item_id"]):
                    continue
                try:
                    await _send_single(db, bot, tg_user_id, d)
                except Exception:
                    log.exception("Error delivering item=%s to user=%s", d["item_id"], tg_user_id)

            for tmdb_id, group in tmdb_groups.items():
                try:
                    group = [d for d in group if not db.delivered(tg_user_id, d["item_id"])]
                    if not group:
                        continue
                    if len(group) >= 2:
                        all_subs = list({s["id"]: s for d in group for s in d["subs"]}.values())
                        log.info("Delivering grouped %d items tmdb=%s to user=%s",
                                 len(group), tmdb_id, tg_user_id)
                        await send_grouped_items_to_user(db, bot, tg_user_id,
                                                         [d["item"] for d in group], all_subs)
                        for d in group:
                            db.record_delivery(tg_user_id, d["item_id"],
                                               int(d["subs"][0]["id"]),
                                               [int(s["id"]) for s in d["subs"]])
                            await asyncio.sleep(0.12)
                    else:
                        await _send_single(db, bot, tg_user_id, group[0])
                except Exception:
                    log.exception("Error delivering tmdb_group tmdb=%s to user=%s", tmdb_id, tg_user_id)
        except Exception:
            log.exception("Unexpected error processing deliveries for user=%s", tg_user_id)


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
