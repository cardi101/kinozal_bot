import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Set

from admin_match_review_helpers import item_requires_match_review, notify_admins_about_match_review
from config import CFG
from content_buckets import item_content_bucket
from domain import DeliveryCandidate, ReleaseItem, SubscriptionRecord
from media_detection import is_non_video_release, is_russian_release
from release_versioning import extract_kinozal_id, parse_episode_progress
from source_health import note_source_cycle_failure, note_source_cycle_success
from utils import compact_spaces, utc_ts

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
    def _new_cycle_metrics() -> Dict[str, int]:
        return {
            "items_fetched_total": 0,
            "items_filtered_non_video_total": 0,
            "items_filtered_russian_total": 0,
            "items_tmdb_enriched_total": 0,
            "items_saved_new_total": 0,
            "items_saved_updated_total": 0,
            "release_text_changes_total": 0,
            "debounce_queued_total": 0,
            "pending_queued_total": 0,
            "deliveries_sent_total": 0,
            "grouped_messages_total": 0,
            "bootstrap_marked_read_total": 0,
        }

    def _meta_int(self, key: str, default: int = 0) -> int:
        try:
            value = self.repository.get_meta(key)
            return int(value) if value is not None else default
        except Exception:
            return default

    def _set_metric(self, key: str, value: Any) -> None:
        self.repository.set_meta(key, str(value))

    def _increment_metric(self, key: str, delta: int) -> None:
        if delta <= 0:
            return
        self._set_metric(key, self._meta_int(key) + delta)

    def _record_cycle_metrics(
        self,
        cycle_started_at: int,
        cycle_metrics: Dict[str, int],
        duration_seconds: float,
        failed: bool,
    ) -> None:
        self._increment_metric("metrics_worker_cycles_total", 1)
        if failed:
            self._increment_metric("metrics_worker_cycle_failures_total", 1)

        self._set_metric("metrics_worker_cycle_last_started_at", cycle_started_at)
        self._set_metric("metrics_worker_cycle_last_finished_at", utc_ts())
        self._set_metric("metrics_worker_cycle_duration_seconds", f"{duration_seconds:.6f}")
        self._set_metric("metrics_worker_last_cycle_items_fetched", cycle_metrics["items_fetched_total"])
        self._set_metric("metrics_worker_last_cycle_new_items", cycle_metrics["items_saved_new_total"])
        self._set_metric("metrics_worker_last_cycle_updated_items", cycle_metrics["items_saved_updated_total"])
        self._set_metric("metrics_worker_last_cycle_deliveries_sent", cycle_metrics["deliveries_sent_total"])

        for metric_name in (
            "items_fetched_total",
            "items_filtered_non_video_total",
            "items_filtered_russian_total",
            "items_tmdb_enriched_total",
            "items_saved_new_total",
            "items_saved_updated_total",
            "release_text_changes_total",
            "debounce_queued_total",
            "pending_queued_total",
            "deliveries_sent_total",
            "grouped_messages_total",
            "bootstrap_marked_read_total",
        ):
            self._increment_metric(f"metrics_worker_{metric_name}", cycle_metrics[metric_name])

    @staticmethod
    def _quiet_active(start_h: int, end_h: int, current_h: int) -> bool:
        if start_h < end_h:
            return start_h <= current_h < end_h
        return current_h >= start_h or current_h < end_h

    async def process_new_items(self, cycle_metrics: Dict[str, int] | None = None) -> None:
        if cycle_metrics is None:
            cycle_metrics = self._new_cycle_metrics()
        items = await self.kinozal_service.fetch_latest()
        cycle_metrics["items_fetched_total"] += len(items)
        if not items:
            log.info("Source returned no items")
            return

        first_run_seen = self.repository.get_meta("bootstrap_done") == "1"
        touched_item_ids: List[int] = []
        new_item_ids: List[int] = []
        live_items_by_id: Dict[int, ReleaseItem] = {}
        release_text_changed_ids: Set[int] = set()
        old_release_texts: Dict[int, str] = {}

        for raw_item in items:
            source_text = f"{raw_item.get('source_title') or ''} {raw_item.get('source_description') or ''}"
            if raw_item.get("media_type") == "other" or is_non_video_release(source_text):
                cycle_metrics["items_filtered_non_video_total"] += 1
                log.info("Skip non-video item: %s", raw_item.get("source_title"))
                continue

            if is_russian_release(raw_item.to_dict()):
                cycle_metrics["items_filtered_russian_total"] += 1
                log.info("Skip Russian item: %s [%s]", raw_item.get("source_title"), raw_item.get("source_category_name"))
                continue

            cached = self.repository.find_existing_enriched(raw_item.get("source_uid"), raw_item.get("source_title"))
            if cached:
                enriched = raw_item.clone()
                for key, value in cached.items():
                    if key.startswith("tmdb_") or key in ("imdb_id", "mal_id", "media_type", "cleaned_title"):
                        if value is not None and not enriched.get(key):
                            enriched.set(key, value)
                if enriched.get("tmdb_id") and not compact_spaces(str(enriched.get("tmdb_match_confidence") or "")):
                    enriched.set(
                        "tmdb_match_path",
                        compact_spaces(str(enriched.get("tmdb_match_path") or "")) or "cached_existing_match",
                    )
                    enriched.set("tmdb_match_confidence", "high")
                    enriched.set(
                        "tmdb_match_evidence",
                        compact_spaces(str(enriched.get("tmdb_match_evidence") or "")) or "reused cached TMDB match",
                    )
            else:
                cycle_metrics["items_tmdb_enriched_total"] += 1
                enriched = await self.tmdb_service.enrich_item(raw_item.clone())
                if not enriched.get("tmdb_id") and compact_spaces(str(enriched.get("source_category_name") or "")):
                    log.info(
                        "TMDB no match, using source category fallback title=%s category=%s bucket=%s media=%s",
                        enriched.get("source_title"),
                        enriched.get("source_category_name"),
                        item_content_bucket(enriched.to_dict()),
                        enriched.get("media_type"),
                    )

            item_id, is_new, materially_changed = self.repository.save_item(enriched.to_dict())
            enriched.set("id", item_id)
            live_items_by_id[item_id] = enriched

            if is_new:
                cycle_metrics["items_saved_new_total"] += 1
                new_item_ids.append(item_id)
                touched_item_ids.append(item_id)
            elif materially_changed:
                cycle_metrics["items_saved_updated_total"] += 1
                touched_item_ids.append(item_id)

            if not is_new and first_run_seen and self.repository.was_delivered_to_anyone(item_id):
                stored_item = self.repository.get_item(item_id)
                stored_release_text = (stored_item.get("source_release_text") or "") if stored_item else ""
                try:
                    detail_enriched = await self.kinozal_service.enrich_item_with_details(
                        enriched.clone(),
                        force_refresh=True,
                    )
                    fresh_release_text = detail_enriched.get("source_release_text") or ""
                    if fresh_release_text and fresh_release_text != stored_release_text:
                        self.repository.update_item_release_text(item_id, fresh_release_text)
                        enriched.set("source_release_text", fresh_release_text)
                        live_items_by_id[item_id] = enriched
                        if stored_release_text:
                            old_release_texts[item_id] = stored_release_text
                            release_text_changed_ids.add(item_id)
                            cycle_metrics["release_text_changes_total"] += 1
                            if item_id not in touched_item_ids:
                                touched_item_ids.append(item_id)
                            log.info("Release text changed for item=%s source_uid=%s", item_id, enriched.source_uid)
                        else:
                            release_text_changed_ids.add(item_id)
                            if item_id not in touched_item_ids:
                                touched_item_ids.append(item_id)
                            log.info("Initialized release text baseline for item=%s source_uid=%s", item_id, enriched.source_uid)

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
                            enriched.set("source_title", details_title)
                            enriched.set("source_episode_progress", details_progress)
                            new_item_id, new_is_new, _ = self.repository.save_item(enriched.to_dict())
                            if new_is_new:
                                cycle_metrics["items_saved_new_total"] += 1
                                enriched.set("id", new_item_id)
                                live_items_by_id[new_item_id] = enriched.clone()
                                new_item_ids.append(new_item_id)
                                if new_item_id not in touched_item_ids:
                                    touched_item_ids.append(new_item_id)
                except Exception:
                    log.warning("Failed to check release text for item=%s", item_id, exc_info=True)

        enabled_subs = self.subscription_service.list_enabled()

        if not first_run_seen and CFG.start_fetch_as_read:
            for item_id in new_item_ids:
                for sub in enabled_subs:
                    self.delivery_service.record_delivery(sub.tg_user_id, item_id, [sub])
                    cycle_metrics["bootstrap_marked_read_total"] += 1
            self.repository.set_meta("bootstrap_done", "1")
            log.info("Bootstrap complete: %s items marked as delivered", len(new_item_ids))
            return

        self.repository.set_meta("bootstrap_done", "1")

        if not touched_item_ids:
            log.info("No new or enriched item versions")

        all_pending: Dict[int, List[DeliveryCandidate]] = {}

        for item_id in touched_item_ids:
            item = live_items_by_id.get(item_id)
            if not item:
                payload = self.repository.get_item(item_id)
                item = ReleaseItem.from_payload(payload) if payload else None
            if not item:
                continue

            is_release_text_change = item_id in release_text_changed_ids
            item_tmdb_id = item.tmdb_id
            kinozal_id = item.kinozal_id or extract_kinozal_id(item.source_uid)
            matches_by_user: Dict[int, List[SubscriptionRecord]] = {}
            for sub in enabled_subs:
                tg_user_id = sub.tg_user_id
                if item_tmdb_id and self.repository.is_title_muted(tg_user_id, item_tmdb_id):
                    continue
                if not is_release_text_change:
                    if self.repository.delivered(tg_user_id, item_id) or self.repository.delivered_equivalent(tg_user_id, item.to_dict()):
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

            needs_review = item_requires_match_review(item.to_dict())

            if not matches_by_user and not needs_review:
                continue

            try:
                item = await self.kinozal_service.enrich_item_with_details(item.clone())
            except Exception:
                log.warning("Failed to enrich item with kinozal details item_id=%s", item_id, exc_info=True)

            if not is_release_text_change and item.get("source_release_text"):
                self.repository.update_item_release_text(item_id, item.get("source_release_text"))

            if item_requires_match_review(item.to_dict()):
                if not matches_by_user:
                    log.info(
                        "Skip match review without affected users item=%s kinozal_id=%s confidence=%s",
                        item_id,
                        kinozal_id,
                        item.get("tmdb_match_confidence"),
                    )
                    continue
                review_reason = compact_spaces(
                    str(item.get("tmdb_match_evidence") or item.get("tmdb_match_confidence") or "low_confidence")
                )
                self.repository.queue_match_review(item_id, kinozal_id or str(item_id), reason=review_reason)
                review = self.repository.get_pending_match_review_by_item_id(item_id)
                if review and not review.get("notified_at"):
                    try:
                        sent_count = await notify_admins_about_match_review(
                            self.bot,
                            item.to_dict(),
                            affected_users=len(matches_by_user),
                        )
                        if sent_count > 0:
                            self.repository.mark_match_review_notified(item_id)
                            log.info(
                                "Auto-sent match review item=%s kinozal_id=%s sent_count=%s",
                                item_id,
                                kinozal_id,
                                sent_count,
                            )
                        else:
                            log.warning(
                                "Match review not marked notified item=%s kinozal_id=%s sent_count=0",
                                item_id,
                                kinozal_id,
                            )
                    except Exception:
                        log.exception("Failed to notify admins about match review item=%s", item_id)
                log.info(
                    "Queued match review item=%s kinozal_id=%s confidence=%s users=%s",
                    item_id,
                    kinozal_id,
                    item.get("tmdb_match_confidence"),
                    len(matches_by_user),
                )
                continue

            for tg_user_id, matched_subs in matches_by_user.items():
                if kinozal_id and not is_release_text_change:
                    sub_ids_str = ",".join(str(sub.id) for sub in matched_subs)
                    self.repository.upsert_debounce(
                        tg_user_id,
                        kinozal_id,
                        item_id,
                        sub_ids_str,
                        delay_seconds=120,
                    )
                    cycle_metrics["debounce_queued_total"] += 1
                    log.info("Debounce queued item=%s kinozal_id=%s to user=%s", item_id, kinozal_id, tg_user_id)
                else:
                    all_pending.setdefault(tg_user_id, []).append(
                        DeliveryCandidate(
                            item=item.clone(),
                            subs=list(matched_subs),
                            old_release_text=old_release_texts.get(item_id, ""),
                            is_release_text_change=is_release_text_change,
                        )
                    )

        current_hour = datetime.now(timezone.utc).hour
        await self._flush_due_pending_deliveries(current_hour, cycle_metrics)
        await self._flush_due_debounce(all_pending)

        if not all_pending:
            return

        await self._deliver_current_cycle(all_pending, current_hour, cycle_metrics)

    async def _flush_due_pending_deliveries(self, current_hour: int, cycle_metrics: Dict[str, int]) -> None:
        due_pending = self.repository.pop_due_pending_deliveries(current_hour)
        for flush_uid, pending_deliveries in due_pending.items():
            for pending_delivery in pending_deliveries:
                pending_item_id = int(pending_delivery["item_id"])
                if self.repository.delivered(flush_uid, pending_item_id):
                    self.repository.delete_pending_delivery(flush_uid, pending_item_id)
                    continue

                pending_item_payload = self.repository.get_item(pending_item_id)
                if not pending_item_payload:
                    self.repository.delete_pending_delivery(flush_uid, pending_item_id)
                    continue

                pending_item = ReleaseItem.from_payload(pending_item_payload)
                try:
                    pending_item = await self.kinozal_service.enrich_item_with_details(pending_item)
                except Exception:
                    log.warning("Failed to enrich pending item=%s", pending_item_id, exc_info=True)

                sub_ids = [
                    sub_id.strip()
                    for sub_id in str(pending_delivery.get("matched_sub_ids") or "").split(",")
                    if sub_id.strip()
                ]
                pending_subs = [
                    SubscriptionRecord.from_payload(sub)
                    for sub in [self.repository.get_subscription(int(sub_id)) for sub_id in sub_ids if sub_id.isdigit()]
                    if sub
                ]
                try:
                    log.info("Flushing pending delivery item=%s to user=%s", pending_item_id, flush_uid)
                    await self.delivery_service.send_item(
                        flush_uid,
                        pending_item,
                        pending_subs,
                        old_release_text=str(pending_delivery.get("old_release_text") or ""),
                    )
                    self.delivery_service.record_delivery(flush_uid, pending_item_id, pending_subs)
                    cycle_metrics["deliveries_sent_total"] += 1
                    self.repository.delete_pending_delivery(flush_uid, pending_item_id)
                    await asyncio.sleep(0.12)
                except Exception:
                    log.exception("Failed to flush pending delivery user=%s item=%s", flush_uid, pending_item_id)

    async def _flush_due_debounce(self, all_pending: Dict[int, List[DeliveryCandidate]]) -> None:
        enriched_cache: Dict[int, ReleaseItem] = {}
        for entry in self.repository.pop_due_debounce():
            tg_user_id = int(entry["tg_user_id"])
            item_id = int(entry["item_id"])
            if self.repository.delivered(tg_user_id, item_id):
                continue

            if item_id not in enriched_cache:
                raw_item = self.repository.get_item(item_id)
                if not raw_item:
                    continue
                item = ReleaseItem.from_payload(raw_item)
                try:
                    enriched_cache[item_id] = await self.kinozal_service.enrich_item_with_details(
                        item,
                        force_refresh=True,
                    )
                except Exception:
                    log.warning("Failed to enrich debounced item=%s", item_id, exc_info=True)
                    enriched_cache[item_id] = item

            item = enriched_cache[item_id]
            if self.repository.delivered_equivalent(tg_user_id, item.to_dict()):
                continue

            sub_ids = [sub_id.strip() for sub_id in str(entry.get("matched_sub_ids") or "").split(",") if sub_id.strip()]
            subs = [
                SubscriptionRecord.from_payload(sub)
                for sub in [self.repository.get_subscription(int(sub_id)) for sub_id in sub_ids if sub_id.isdigit()]
                if sub
            ]
            if not subs:
                continue

            log.info("Debounce ready item=%s kinozal_id=%s to user=%s", item_id, entry["kinozal_id"], tg_user_id)
            all_pending.setdefault(tg_user_id, []).append(
                DeliveryCandidate(
                    item=item.clone(),
                    subs=subs,
                    old_release_text="",
                    is_release_text_change=False,
                )
            )

    async def _deliver_current_cycle(
        self,
        all_pending: Dict[int, List[DeliveryCandidate]],
        current_hour: int,
        cycle_metrics: Dict[str, int],
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
                        sub_ids_str = ",".join(str(sub.id) for sub in delivery.subs)
                        self.repository.queue_pending_delivery(
                            tg_user_id,
                            delivery.item_id,
                            sub_ids_str,
                            delivery.old_release_text,
                            delivery.is_release_text_change,
                        )
                    cycle_metrics["pending_queued_total"] += len(deliveries)
                    log.info(
                        "Queued %d deliveries for user=%s (quiet %02d:00-%02d:00 UTC)",
                        len(deliveries),
                        tg_user_id,
                        quiet_start,
                        quiet_end,
                    )
                    continue

                release_text_updates = [delivery for delivery in deliveries if delivery.is_release_text_change]
                regular_deliveries = [delivery for delivery in deliveries if not delivery.is_release_text_change]

                tmdb_groups: Dict[int, List[DeliveryCandidate]] = {}
                without_tmdb: List[DeliveryCandidate] = []
                for delivery in regular_deliveries:
                    tmdb_id = delivery.item.tmdb_id
                    if tmdb_id:
                        tmdb_groups.setdefault(tmdb_id, []).append(delivery)
                    else:
                        without_tmdb.append(delivery)

                for delivery in release_text_updates:
                    try:
                        log.info(
                            "Delivering release text update item=%s to user=%s source_uid=%s",
                            delivery.item_id,
                            tg_user_id,
                            delivery.item.source_uid,
                        )
                        await self.delivery_service.send_item(
                            tg_user_id,
                            delivery.item,
                            delivery.subs,
                            old_release_text=delivery.old_release_text,
                        )
                        self.delivery_service.record_delivery(tg_user_id, delivery.item_id, delivery.subs)
                        cycle_metrics["deliveries_sent_total"] += 1
                        await asyncio.sleep(0.12)
                    except Exception:
                        log.exception("Error delivering rtc item=%s to user=%s", delivery.item_id, tg_user_id)

                for delivery in without_tmdb:
                    if self.repository.delivered(tg_user_id, delivery.item_id):
                        continue
                    try:
                        await self.delivery_service.send_single(tg_user_id, delivery)
                        cycle_metrics["deliveries_sent_total"] += 1
                    except Exception:
                        log.exception("Error delivering item=%s to user=%s", delivery.item_id, tg_user_id)

                for tmdb_id, group in tmdb_groups.items():
                    try:
                        group = [delivery for delivery in group if not self.repository.delivered(tg_user_id, delivery.item_id)]
                        if not group:
                            continue
                        if len(group) >= 2:
                            all_subs = list({sub.id: sub for delivery in group for sub in delivery.subs}.values())
                            log.info("Delivering grouped %d items tmdb=%s to user=%s", len(group), tmdb_id, tg_user_id)
                            await self.delivery_service.send_grouped_items(
                                tg_user_id,
                                [delivery.item for delivery in group],
                                all_subs,
                            )
                            cycle_metrics["grouped_messages_total"] += 1
                            for delivery in group:
                                self.delivery_service.record_delivery(tg_user_id, delivery.item_id, delivery.subs)
                                cycle_metrics["deliveries_sent_total"] += 1
                                await asyncio.sleep(0.12)
                        else:
                            await self.delivery_service.send_single(tg_user_id, group[0])
                            cycle_metrics["deliveries_sent_total"] += 1
                    except Exception:
                        log.exception("Error delivering tmdb_group tmdb=%s to user=%s", tmdb_id, tg_user_id)
            except Exception:
                log.exception("Unexpected error processing deliveries for user=%s", tg_user_id)

    async def poll_forever(self) -> None:
        while True:
            cycle_started_at = utc_ts()
            cycle_started_monotonic = time.monotonic()
            cycle_metrics = self._new_cycle_metrics()
            cycle_failed = False
            try:
                await self.process_new_items(cycle_metrics)
                await note_source_cycle_success(self.repository.db, self.bot)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                cycle_failed = True
                await note_source_cycle_failure(self.repository.db, self.bot, exc)
                log.exception("Poller cycle failed")
            finally:
                self._record_cycle_metrics(
                    cycle_started_at,
                    cycle_metrics,
                    time.monotonic() - cycle_started_monotonic,
                    failed=cycle_failed,
                )
            await asyncio.sleep(CFG.poll_seconds)
