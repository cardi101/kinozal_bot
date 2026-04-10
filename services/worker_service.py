import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

from config import CFG
from content_buckets import item_content_bucket
from media_detection import is_non_video_release
from release_versioning import extract_kinozal_id, parse_episode_progress
from source_health import note_source_cycle_failure, note_source_cycle_success
from utils import compact_spaces

log = logging.getLogger(__name__)


class WorkerService:
    def __init__(
        self,
        repository: Any,
        kinozal_service: Any,
        tmdb_service: Any,
        subscription_service: Any,
        delivery_service: Any,
        bot: Any,
    ) -> None:
        self.repository = repository
        self.kinozal_service = kinozal_service
        self.tmdb_service = tmdb_service
        self.subscription_service = subscription_service
        self.delivery_service = delivery_service
        self.bot = bot

    @staticmethod
    def _quiet_active(start_h: int, end_h: int, current_h: int) -> bool:
        if start_h < end_h:
            return start_h <= current_h < end_h
        return current_h >= start_h or current_h < end_h

    async def process_new_items(self) -> None:
        items = await self.kinozal_service.fetch_latest()
        if not items:
            log.info("Source returned no items")
            return

        first_run_seen = self.repository.get_meta("bootstrap_done") == "1"
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

            cached = self.repository.find_existing_enriched(raw_item.get("source_uid"), raw_item.get("source_title"))
            if cached:
                enriched = dict(raw_item)
                for key, value in cached.items():
                    if key.startswith("tmdb_") or key in ("imdb_id", "mal_id", "media_type", "cleaned_title"):
                        if value is not None and not enriched.get(key):
                            enriched[key] = value
            else:
                enriched = await self.tmdb_service.enrich_item(dict(raw_item))
                if not enriched.get("tmdb_id") and compact_spaces(str(enriched.get("source_category_name") or "")):
                    log.info(
                        "TMDB no match, using source category fallback title=%s category=%s bucket=%s media=%s",
                        enriched.get("source_title"),
                        enriched.get("source_category_name"),
                        item_content_bucket(enriched),
                        enriched.get("media_type"),
                    )

            item_id, is_new, materially_changed = self.repository.save_item(enriched)
            enriched["id"] = item_id
            live_items_by_id[item_id] = enriched

            if is_new:
                new_item_ids.append(item_id)
                touched_item_ids.append(item_id)
            elif materially_changed:
                touched_item_ids.append(item_id)

            if not is_new and first_run_seen and self.repository.was_delivered_to_anyone(item_id):
                stored_item = self.repository.get_item(item_id)
                stored_release_text = (stored_item.get("source_release_text") or "") if stored_item else ""
                try:
                    detail_enriched = await self.kinozal_service.enrich_item_with_details(
                        dict(enriched),
                        force_refresh=True,
                    )
                    fresh_release_text = detail_enriched.get("source_release_text") or ""
                    if fresh_release_text and fresh_release_text != stored_release_text:
                        self.repository.update_item_release_text(item_id, fresh_release_text)
                        enriched["source_release_text"] = fresh_release_text
                        live_items_by_id[item_id] = enriched
                        if stored_release_text:
                            old_release_texts[item_id] = stored_release_text
                            release_text_changed_ids.add(item_id)
                            if item_id not in touched_item_ids:
                                touched_item_ids.append(item_id)
                            log.info("Release text changed for item=%s source_uid=%s", item_id, enriched.get("source_uid"))
                        else:
                            release_text_changed_ids.add(item_id)
                            if item_id not in touched_item_ids:
                                touched_item_ids.append(item_id)
                            log.info("Initialized release text baseline for item=%s source_uid=%s", item_id, enriched.get("source_uid"))

                    details_title = detail_enriched.get("details_title") or ""
                    if details_title:
                        details_progress = parse_episode_progress(details_title)
                        stored_progress = enriched.get("source_episode_progress") or ""
                        if details_progress and details_progress != stored_progress:
                            log.info(
                                "Details page episode progress differs for item=%s: browse=%s details=%s, creating new version",
                                item_id,
                                stored_progress,
                                details_progress,
                            )
                            enriched["source_title"] = details_title
                            enriched["source_episode_progress"] = details_progress
                            new_item_id, new_is_new, _ = self.repository.save_item(enriched)
                            if new_is_new:
                                enriched["id"] = new_item_id
                                live_items_by_id[new_item_id] = enriched
                                new_item_ids.append(new_item_id)
                                if new_item_id not in touched_item_ids:
                                    touched_item_ids.append(new_item_id)
                except Exception:
                    log.warning("Failed to check release text for item=%s", item_id, exc_info=True)

        enabled_subs = self.subscription_service.list_enabled()

        if not first_run_seen and CFG.start_fetch_as_read:
            for item_id in new_item_ids:
                for sub in enabled_subs:
                    self.delivery_service.record_delivery(int(sub["tg_user_id"]), item_id, [sub])
            self.repository.set_meta("bootstrap_done", "1")
            log.info("Bootstrap complete: %s items marked as delivered", len(new_item_ids))
            return

        self.repository.set_meta("bootstrap_done", "1")

        if not touched_item_ids:
            log.info("No new or enriched item versions")

        all_pending: Dict[int, List[Dict[str, Any]]] = {}

        for item_id in touched_item_ids:
            item = live_items_by_id.get(item_id) or self.repository.get_item(item_id)
            if not item:
                continue

            is_release_text_change = item_id in release_text_changed_ids
            item_tmdb_id = int(item["tmdb_id"]) if item.get("tmdb_id") else None
            kinozal_id = item.get("kinozal_id") or extract_kinozal_id(item.get("source_uid"))
            matches_by_user: Dict[int, List[Dict[str, Any]]] = {}
            for sub in enabled_subs:
                tg_user_id = int(sub["tg_user_id"])
                if item_tmdb_id and self.repository.is_title_muted(tg_user_id, item_tmdb_id):
                    continue
                if not is_release_text_change:
                    if self.repository.delivered(tg_user_id, item_id) or self.repository.delivered_equivalent(tg_user_id, item):
                        continue
                    if kinozal_id and self.repository.recently_delivered_kinozal_id(
                        tg_user_id,
                        kinozal_id,
                        cooldown_seconds=420,
                    ):
                        continue
                else:
                    if not self.repository.delivered(tg_user_id, item_id):
                        continue
                    if self.repository.recently_delivered(tg_user_id, item_id, cooldown_seconds=420):
                        continue
                if not self.subscription_service.matches(sub, item):
                    continue
                matches_by_user.setdefault(tg_user_id, []).append(sub)

            if not matches_by_user:
                continue

            try:
                item = await self.kinozal_service.enrich_item_with_details(dict(item))
            except Exception:
                log.warning("Failed to enrich item with kinozal details item_id=%s", item_id, exc_info=True)

            if not is_release_text_change and item.get("source_release_text"):
                self.repository.update_item_release_text(item_id, item["source_release_text"])

            for tg_user_id, matched_subs in matches_by_user.items():
                if kinozal_id and not is_release_text_change:
                    sub_ids_str = ",".join(str(sub["id"]) for sub in matched_subs)
                    self.repository.upsert_debounce(
                        tg_user_id,
                        kinozal_id,
                        item_id,
                        sub_ids_str,
                        delay_seconds=120,
                    )
                    log.info("Debounce queued item=%s kinozal_id=%s to user=%s", item_id, kinozal_id, tg_user_id)
                else:
                    all_pending.setdefault(tg_user_id, []).append({
                        "item": item,
                        "item_id": item_id,
                        "subs": matched_subs,
                        "old_release_text": old_release_texts.get(item_id, ""),
                        "is_release_text_change": is_release_text_change,
                    })

        current_hour = datetime.now(timezone.utc).hour
        await self._flush_due_pending_deliveries(current_hour)
        await self._flush_due_debounce(all_pending)

        if not all_pending:
            return

        await self._deliver_current_cycle(all_pending, current_hour)

    async def _flush_due_pending_deliveries(self, current_hour: int) -> None:
        due_pending = self.repository.pop_due_pending_deliveries(current_hour)
        for flush_uid, pending_deliveries in due_pending.items():
            for pending_delivery in pending_deliveries:
                pending_item_id = int(pending_delivery["item_id"])
                if self.repository.delivered(flush_uid, pending_item_id):
                    self.repository.delete_pending_delivery(flush_uid, pending_item_id)
                    continue

                pending_item = self.repository.get_item(pending_item_id)
                if not pending_item:
                    self.repository.delete_pending_delivery(flush_uid, pending_item_id)
                    continue

                try:
                    pending_item = await self.kinozal_service.enrich_item_with_details(dict(pending_item))
                except Exception:
                    log.warning("Failed to enrich pending item=%s", pending_item_id, exc_info=True)

                sub_ids = [
                    sub_id.strip()
                    for sub_id in str(pending_delivery.get("matched_sub_ids") or "").split(",")
                    if sub_id.strip()
                ]
                pending_subs = [self.repository.get_subscription(int(sub_id)) for sub_id in sub_ids if sub_id.isdigit()]
                pending_subs = [sub for sub in pending_subs if sub]
                try:
                    log.info("Flushing pending delivery item=%s to user=%s", pending_item_id, flush_uid)
                    await self.delivery_service.send_item(
                        flush_uid,
                        pending_item,
                        pending_subs,
                        old_release_text=str(pending_delivery.get("old_release_text") or ""),
                    )
                    self.delivery_service.record_delivery(flush_uid, pending_item_id, pending_subs)
                    self.repository.delete_pending_delivery(flush_uid, pending_item_id)
                    await asyncio.sleep(0.12)
                except Exception:
                    log.exception("Failed to flush pending delivery user=%s item=%s", flush_uid, pending_item_id)

    async def _flush_due_debounce(self, all_pending: Dict[int, List[Dict[str, Any]]]) -> None:
        enriched_cache: Dict[int, Dict[str, Any]] = {}
        for entry in self.repository.pop_due_debounce():
            tg_user_id = int(entry["tg_user_id"])
            item_id = int(entry["item_id"])
            if self.repository.delivered(tg_user_id, item_id):
                continue

            if item_id not in enriched_cache:
                raw_item = self.repository.get_item(item_id)
                if not raw_item:
                    continue
                try:
                    enriched_cache[item_id] = await self.kinozal_service.enrich_item_with_details(
                        dict(raw_item),
                        force_refresh=True,
                    )
                except Exception:
                    log.warning("Failed to enrich debounced item=%s", item_id, exc_info=True)
                    enriched_cache[item_id] = raw_item

            item = enriched_cache[item_id]
            if self.repository.delivered_equivalent(tg_user_id, item):
                continue

            sub_ids = [sub_id.strip() for sub_id in str(entry.get("matched_sub_ids") or "").split(",") if sub_id.strip()]
            subs = [self.repository.get_subscription(int(sub_id)) for sub_id in sub_ids if sub_id.isdigit()]
            subs = [sub for sub in subs if sub]
            if not subs:
                continue

            log.info("Debounce ready item=%s kinozal_id=%s to user=%s", item_id, entry["kinozal_id"], tg_user_id)
            all_pending.setdefault(tg_user_id, []).append({
                "item": item,
                "item_id": item_id,
                "subs": subs,
                "old_release_text": "",
                "is_release_text_change": False,
            })

    async def _deliver_current_cycle(
        self,
        all_pending: Dict[int, List[Dict[str, Any]]],
        current_hour: int,
    ) -> None:
        for tg_user_id, deliveries in all_pending.items():
            try:
                quiet_start, quiet_end = self.repository.get_user_quiet_hours(tg_user_id)
                if (
                    quiet_start is not None
                    and quiet_end is not None
                    and self._quiet_active(quiet_start, quiet_end, current_hour)
                ):
                    for delivery in deliveries:
                        sub_ids_str = ",".join(str(sub["id"]) for sub in delivery["subs"])
                        self.repository.queue_pending_delivery(
                            tg_user_id,
                            delivery["item_id"],
                            sub_ids_str,
                            delivery["old_release_text"],
                            delivery["is_release_text_change"],
                        )
                    log.info(
                        "Queued %d deliveries for user=%s (quiet %02d:00-%02d:00 UTC)",
                        len(deliveries),
                        tg_user_id,
                        quiet_start,
                        quiet_end,
                    )
                    continue

                release_text_updates = [delivery for delivery in deliveries if delivery["is_release_text_change"]]
                regular_deliveries = [delivery for delivery in deliveries if not delivery["is_release_text_change"]]

                tmdb_groups: Dict[int, List[Dict[str, Any]]] = {}
                without_tmdb: List[Dict[str, Any]] = []
                for delivery in regular_deliveries:
                    tmdb_id = delivery["item"].get("tmdb_id")
                    if tmdb_id:
                        tmdb_groups.setdefault(int(tmdb_id), []).append(delivery)
                    else:
                        without_tmdb.append(delivery)

                for delivery in release_text_updates:
                    try:
                        log.info(
                            "Delivering release text update item=%s to user=%s source_uid=%s",
                            delivery["item_id"],
                            tg_user_id,
                            delivery["item"].get("source_uid"),
                        )
                        await self.delivery_service.send_item(
                            tg_user_id,
                            delivery["item"],
                            delivery["subs"],
                            old_release_text=delivery["old_release_text"],
                        )
                        self.delivery_service.record_delivery(tg_user_id, delivery["item_id"], delivery["subs"])
                        await asyncio.sleep(0.12)
                    except Exception:
                        log.exception("Error delivering rtc item=%s to user=%s", delivery["item_id"], tg_user_id)

                for delivery in without_tmdb:
                    if self.repository.delivered(tg_user_id, delivery["item_id"]):
                        continue
                    try:
                        await self.delivery_service.send_single(tg_user_id, delivery)
                    except Exception:
                        log.exception("Error delivering item=%s to user=%s", delivery["item_id"], tg_user_id)

                for tmdb_id, group in tmdb_groups.items():
                    try:
                        group = [delivery for delivery in group if not self.repository.delivered(tg_user_id, delivery["item_id"])]
                        if not group:
                            continue
                        if len(group) >= 2:
                            all_subs = list({sub["id"]: sub for delivery in group for sub in delivery["subs"]}.values())
                            log.info("Delivering grouped %d items tmdb=%s to user=%s", len(group), tmdb_id, tg_user_id)
                            await self.delivery_service.send_grouped_items(
                                tg_user_id,
                                [delivery["item"] for delivery in group],
                                all_subs,
                            )
                            for delivery in group:
                                self.delivery_service.record_delivery(tg_user_id, delivery["item_id"], delivery["subs"])
                                await asyncio.sleep(0.12)
                        else:
                            await self.delivery_service.send_single(tg_user_id, group[0])
                    except Exception:
                        log.exception("Error delivering tmdb_group tmdb=%s to user=%s", tmdb_id, tg_user_id)
            except Exception:
                log.exception("Unexpected error processing deliveries for user=%s", tg_user_id)

    async def poll_forever(self) -> None:
        while True:
            try:
                await self.process_new_items()
                await note_source_cycle_success(self.repository.db, self.bot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await note_source_cycle_failure(self.repository.db, self.bot, exc)
                log.exception("Poller cycle failed")
            await asyncio.sleep(CFG.poll_seconds)
