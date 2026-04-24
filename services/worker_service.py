import asyncio
import html
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Sequence, Set

from aiogram.enums import ParseMode

from admin_match_review_helpers import item_requires_match_review, notify_admins_about_match_review
from config import CFG
from content_buckets import item_content_bucket
from delivery_events import build_delivery_event_key, resolve_delivery_event_type
from domain import DeliveryCandidate, ReleaseItem, SubscriptionRecord
from episode_progress import parse_episode_progress
from keyboards import anomaly_alert_kb
from media_detection import is_non_video_release, is_russian_release
from quiet_hours import next_quiet_window_end_ts, quiet_window_status
from release_versioning import extract_kinozal_id, format_variant_summary
from service_helpers import ops_alert_chat_ids
from subscription_matching import match_subscription
from source_categories import source_category_is_non_video
from source_health import note_source_cycle_failure, note_source_cycle_success
from utils import compact_spaces, utc_ts

log = logging.getLogger(__name__)


def _grouped_event_envelope(event_key: str) -> str:
    normalized = compact_spaces(str(event_key or ""))
    if ":item:" in normalized:
        return normalized.rsplit(":item:", 1)[0]
    return normalized


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
            "items_filtered_non_video_by_category_total": 0,
            "items_filtered_russian_total": 0,
            "items_tmdb_enriched_total": 0,
            "items_saved_new_total": 0,
            "items_saved_updated_total": 0,
            "release_text_changes_total": 0,
            "observations_recorded_total": 0,
            "progress_regressions_total": 0,
            "anomaly_holds_total": 0,
            "debounce_queued_total": 0,
            "pending_queued_total": 0,
            "deliveries_sent_total": 0,
            "grouped_messages_total": 0,
            "bootstrap_marked_read_total": 0,
        }

    @staticmethod
    def _should_refresh_cached_tmdb_match(cached: Dict[str, Any] | None) -> bool:
        if not cached or not cached.get("tmdb_id"):
            return False
        path = compact_spaces(str(cached.get("tmdb_match_path") or "")).lower()
        confidence = compact_spaces(str(cached.get("tmdb_match_confidence") or "")).lower()

        # Explicit/manual matches should remain sticky until an operator changes them.
        if path in {"stored_override", "manual_override", "admin_override"}:
            return False

        # Older rows may have tmdb_id but no confidence because they were matched before
        # current validation existed; re-enrich those rows instead of freezing them forever.
        if confidence in {"", "low"}:
            return True
        return False

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
            "items_filtered_non_video_by_category_total",
            "items_filtered_russian_total",
            "items_tmdb_enriched_total",
            "items_saved_new_total",
            "items_saved_updated_total",
            "release_text_changes_total",
            "observations_recorded_total",
            "progress_regressions_total",
            "anomaly_holds_total",
            "debounce_queued_total",
            "pending_queued_total",
            "deliveries_sent_total",
            "grouped_messages_total",
            "bootstrap_marked_read_total",
        ):
            self._increment_metric(f"metrics_worker_{metric_name}", cycle_metrics[metric_name])

    async def _notify_admins_about_anomaly(
        self,
        kinozal_id: str,
        item: ReleaseItem,
        anomaly_type: str,
        previous_value: str,
        current_value: str,
        details: str = "",
    ) -> int:
        targets = ops_alert_chat_ids()
        if not targets:
            return 0
        lines = [
            "🚨 <b>Release anomaly detected</b>",
            f"Kinozal ID: <code>{html.escape(kinozal_id)}</code>",
            f"Type: <code>{html.escape(anomaly_type)}</code>",
            f"Title: {html.escape(item.source_title)}",
            f"Old: <code>{html.escape(previous_value)}</code>",
            f"New: <code>{html.escape(current_value)}</code>",
            f"Variant: <code>{html.escape(format_variant_summary(item.to_dict()))}</code>",
        ]
        if details:
            lines.append(f"Details: {html.escape(details)}")
        lines.extend(
            [
                f"Route: <code>{html.escape(','.join(str(target) for target in targets))}</code>",
                "Action: проверь timeline/explain и реши, нужен ли ручной replay.",
            ]
        )
        text = "\n".join(lines)
        sent_count = 0
        for admin_id in targets:
            try:
                await self.bot.send_message(
                    int(admin_id),
                    text,
                    parse_mode=ParseMode.HTML,
                    disable_web_page_preview=True,
                    reply_markup=anomaly_alert_kb(kinozal_id),
                )
                sent_count += 1
            except Exception:
                log.exception("Failed to send anomaly alert chat_id=%s kinozal_id=%s", admin_id, kinozal_id)
        return sent_count

    def _record_observation(
        self,
        item: ReleaseItem,
        cycle_metrics: Dict[str, int],
        source_kind: str,
        poll_ts: int,
        item_id: int | None = None,
        details_title: str = "",
    ) -> None:
        kinozal_id = item.kinozal_id or extract_kinozal_id(item.source_uid) or extract_kinozal_id(item.get("source_link"))
        if not kinozal_id:
            return
        observation_id = self.repository.record_source_observation(
            kinozal_id=kinozal_id,
            source_kind=source_kind,
            poll_ts=poll_ts,
            item_id=item_id,
            source_title=str(item.get("source_title") or ""),
            details_title=details_title or str(item.get("details_title") or ""),
            episode_progress=str(item.get("source_episode_progress") or ""),
            release_text=str(item.get("source_release_text") or ""),
            source_format=str(item.get("source_format") or ""),
            source_audio_tracks=item.get("source_audio_tracks"),
            raw_payload=item.to_dict(),
        )
        if observation_id:
            cycle_metrics["observations_recorded_total"] += 1

    @staticmethod
    def _quiet_active(start_h: int, end_h: int, current_h: int) -> bool:
        if start_h < end_h:
            return start_h <= current_h < end_h
        return current_h >= start_h or current_h < end_h

    @staticmethod
    def _should_emit_anomaly_alert(
        matches_by_user: Dict[int, List[SubscriptionRecord]],
        existing_anomaly: Dict[str, Any] | None,
    ) -> bool:
        return bool(matches_by_user) and not existing_anomaly

    def _resolve_delivery_subscriptions(self, tg_user_id: int, item: ReleaseItem, matched_sub_ids_csv: str) -> List[SubscriptionRecord]:
        sub_ids = [sub_id.strip() for sub_id in str(matched_sub_ids_csv or "").split(",") if sub_id.strip()]
        matcher_db = getattr(self.repository, "db", self.repository)
        subs: List[SubscriptionRecord] = []
        user_subscriptions = {
            int(sub["id"]): sub
            for sub in self.repository.list_user_subscriptions(int(tg_user_id))
            if sub and sub.get("id") is not None
        }
        for sub in [user_subscriptions.get(int(sub_id)) for sub_id in sub_ids if sub_id.isdigit()]:
            if not sub or not int(sub.get("is_enabled") or 0):
                continue
            if self.subscription_service and hasattr(self.subscription_service, "matches"):
                matched = bool(self.subscription_service.matches(SubscriptionRecord.from_payload(sub), item))
            else:
                matched = match_subscription(matcher_db, sub, item.to_dict())
            if matched:
                subs.append(SubscriptionRecord.from_payload(sub))
        if subs:
            return subs
        fallback_subs: List[SubscriptionRecord] = []
        for sub_full in user_subscriptions.values():
            if not sub_full or not int(sub_full.get("is_enabled") or 0):
                continue
            if self.subscription_service and hasattr(self.subscription_service, "matches"):
                matched = bool(self.subscription_service.matches(SubscriptionRecord.from_payload(sub_full), item))
            else:
                matched = match_subscription(matcher_db, sub_full, item.to_dict())
            if matched:
                fallback_subs.append(SubscriptionRecord.from_payload(sub_full))
        return fallback_subs

    def _user_matches_item(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        *,
        item_id: int,
        kinozal_id: str,
        is_release_text_change: bool,
    ) -> List[SubscriptionRecord]:
        matched_subs, _ = self._user_match_result(
            tg_user_id,
            item,
            item_id=item_id,
            kinozal_id=kinozal_id,
            is_release_text_change=is_release_text_change,
        )
        return matched_subs

    def _user_match_result(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        *,
        item_id: int,
        kinozal_id: str,
        is_release_text_change: bool,
    ) -> tuple[List[SubscriptionRecord], str]:
        access_checker = getattr(self.repository, "user_has_access", None)
        if callable(access_checker) and not access_checker(tg_user_id):
            return [], "user_access_denied"

        if item.tmdb_id and self.repository.is_title_muted(tg_user_id, item.tmdb_id):
            return [], "title_muted"

        if not is_release_text_change:
            if self.repository.delivered(tg_user_id, item_id):
                return [], "already_delivered_item"
            if self.repository.delivered_equivalent(tg_user_id, item.to_dict()):
                return [], "delivered_equivalent"
            if kinozal_id and self.repository.recently_delivered_kinozal_id(
                tg_user_id,
                kinozal_id,
                cooldown_seconds=420,
            ):
                return [], "recently_delivered_kinozal_id"
        else:
            if not self.repository.delivered(tg_user_id, item_id):
                return [], "release_text_without_base_delivery"
            if self.repository.recently_delivered(tg_user_id, item_id, cooldown_seconds=420):
                return [], "recently_delivered_item"

        matched_subs: List[SubscriptionRecord] = []
        enabled_seen = False
        for sub in self.repository.list_user_subscriptions(int(tg_user_id)):
            if not sub or not int(sub.get("is_enabled") or 0):
                continue
            enabled_seen = True
            subscription = SubscriptionRecord.from_payload(sub)
            if self.subscription_service and hasattr(self.subscription_service, "matches"):
                matched = bool(self.subscription_service.matches(subscription, item))
            else:
                matched = match_subscription(getattr(self.repository, "db", self.repository), sub, item.to_dict())
            if matched:
                matched_subs.append(subscription)
        if matched_subs:
            return matched_subs, "matched"
        if enabled_seen:
            return [], "subscription_mismatch"
        return [], "no_enabled_subscriptions"

    def _recover_followup_matches_for_delivered_kinozal(
        self,
        item: ReleaseItem,
        *,
        item_id: int,
        kinozal_id: str,
        is_release_text_change: bool,
        existing_user_ids: Set[int] | None = None,
    ) -> Dict[int, List[SubscriptionRecord]]:
        if not kinozal_id or is_release_text_change:
            return {}
        getter = getattr(self.repository, "list_recent_delivery_users_for_kinozal_id", None)
        if not callable(getter):
            return {}

        existing = {int(user_id) for user_id in (existing_user_ids or set())}
        recovered: Dict[int, List[SubscriptionRecord]] = {}
        skipped_by_reason: Dict[str, List[int]] = {}
        checked_missing_users: List[int] = []
        for tg_user_id in getter(kinozal_id, limit=200, exclude_user_ids=sorted(existing)):
            resolved_user_id = int(tg_user_id)
            checked_missing_users.append(resolved_user_id)
            matched_subs, reason = self._user_match_result(
                resolved_user_id,
                item,
                item_id=item_id,
                kinozal_id=kinozal_id,
                is_release_text_change=False,
            )
            if matched_subs:
                recovered[resolved_user_id] = matched_subs
            else:
                skipped_by_reason.setdefault(reason, []).append(resolved_user_id)
        if recovered:
            log.warning(
                "Recovered follow-up delivery matches item=%s kinozal_id=%s users=%s recovered_user_ids=%s",
                item_id,
                kinozal_id,
                len(recovered),
                ",".join(str(user_id) for user_id in sorted(recovered)),
            )
        if checked_missing_users and (recovered or skipped_by_reason):
            reason_summary = ", ".join(
                f"{reason}:{len(user_ids)}[{','.join(str(user_id) for user_id in user_ids[:5])}{'...' if len(user_ids) > 5 else ''}]"
                for reason, user_ids in sorted(skipped_by_reason.items())
            )
            log.info(
                "Follow-up recovery audit item=%s kinozal_id=%s existing_users=%s checked_missing=%s recovered=%s skipped=%s",
                item_id,
                kinozal_id,
                len(existing),
                len(checked_missing_users),
                len(recovered),
                reason_summary or "-",
            )
        return recovered

    def _latest_payload_for_queued_item(self, payload: Dict[str, Any] | None, kinozal_id: str = "") -> Dict[str, Any] | None:
        if not payload and not compact_spaces(kinozal_id):
            return payload

        resolved_kinozal_id = compact_spaces(kinozal_id)
        if not resolved_kinozal_id and payload:
            resolved_kinozal_id = (
                compact_spaces(str(payload.get("kinozal_id") or ""))
                or extract_kinozal_id(payload.get("source_uid"))
                or extract_kinozal_id(payload.get("source_link"))
            )
        if not resolved_kinozal_id:
            return payload

        finder = getattr(self.repository, "find_item_any_by_kinozal_id", None)
        if not callable(finder):
            return payload

        latest_payload = finder(resolved_kinozal_id)
        return latest_payload or payload

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
        poll_ts = utc_ts()

        for raw_item in items:
            self._record_observation(raw_item, cycle_metrics, source_kind="browse", poll_ts=poll_ts)
            source_text = f"{raw_item.get('source_title') or ''} {raw_item.get('source_description') or ''}"
            category_non_video = source_category_is_non_video(
                raw_item.get("source_category_id"),
                raw_item.get("source_category_name"),
            )
            text_non_video = is_non_video_release(source_text)
            if raw_item.get("media_type") == "other" or category_non_video or text_non_video:
                cycle_metrics["items_filtered_non_video_total"] += 1
                if category_non_video:
                    cycle_metrics["items_filtered_non_video_by_category_total"] += 1
                log.debug(
                    "Skip non-video item: %s [%s] media=%s category_non_video=%s text_non_video=%s",
                    raw_item.get("source_title"),
                    raw_item.get("source_category_name"),
                    raw_item.get("media_type"),
                    category_non_video,
                    text_non_video,
                )
                continue

            if is_russian_release(raw_item.to_dict()):
                cycle_metrics["items_filtered_russian_total"] += 1
                log.info("Skip Russian item: %s [%s]", raw_item.get("source_title"), raw_item.get("source_category_name"))
                continue

            cached = self.repository.find_existing_enriched(raw_item.get("source_uid"), raw_item.get("source_title"))
            if cached and not self._should_refresh_cached_tmdb_match(cached):
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
                enrich_input = raw_item.clone()
                if cached and self._should_refresh_cached_tmdb_match(cached):
                    enrich_input.set("_clear_tmdb_match", True)
                enriched = await self.tmdb_service.enrich_item(enrich_input)
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
                    self._record_observation(
                        detail_enriched,
                        cycle_metrics,
                        source_kind="details",
                        poll_ts=poll_ts,
                        item_id=item_id,
                        details_title=str(detail_enriched.get("details_title") or ""),
                    )
                except Exception:
                    log.warning("Failed to check release text for item=%s", item_id, exc_info=True)

        if self.subscription_service and hasattr(self.subscription_service, "list_enabled_compiled"):
            enabled_subs = self.subscription_service.list_enabled_compiled()
        else:
            enabled_subs = self.subscription_service.list_enabled()

        if not first_run_seen and CFG.start_fetch_as_read:
            for item_id in new_item_ids:
                for sub in enabled_subs:
                    item = live_items_by_id.get(item_id)
                    if item:
                        self.delivery_service.record_delivery(sub.tg_user_id, item, [sub], context="bootstrap")
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
            kinozal_id = item.kinozal_id or extract_kinozal_id(item.source_uid)
            progress = compact_spaces(str(item.get("source_episode_progress") or ""))
            matches_by_user: Dict[int, List[SubscriptionRecord]] = {}
            for sub in enabled_subs:
                tg_user_id = int(sub.tg_user_id)
                if tg_user_id in matches_by_user:
                    continue
                matched_subs = self._user_matches_item(
                    tg_user_id,
                    item,
                    item_id=item_id,
                    kinozal_id=kinozal_id or "",
                    is_release_text_change=is_release_text_change,
                )
                if matched_subs:
                    matches_by_user[tg_user_id] = matched_subs

            if not is_release_text_change and kinozal_id:
                recovered_matches = self._recover_followup_matches_for_delivered_kinozal(
                    item,
                    item_id=item_id,
                    kinozal_id=kinozal_id,
                    is_release_text_change=False,
                    existing_user_ids=set(matches_by_user),
                )
                if recovered_matches:
                    matches_by_user.update(recovered_matches)

            if kinozal_id and not is_release_text_change and progress:
                higher_progress_item = self.repository.find_higher_progress_reference(
                    kinozal_id,
                    progress,
                    item_id=item_id,
                )
                if higher_progress_item:
                    previous_progress = compact_spaces(str(higher_progress_item.get("source_episode_progress") or ""))
                    existing_anomaly = self.repository.get_open_release_anomaly(
                        kinozal_id,
                        "progress_regression",
                        old_value=previous_progress,
                        new_value=progress,
                    )
                    cycle_metrics["progress_regressions_total"] += 1
                    if matches_by_user:
                        item.set("anomaly_flags", ["progress_regression"])
                        cycle_metrics["anomaly_holds_total"] += 1
                        anomaly_details = (
                            f"higher={previous_progress} state={higher_progress_item.get('state')} "
                            f"item_id={higher_progress_item.get('item_id')}"
                        )
                        if not existing_anomaly:
                            self.repository.record_release_anomaly(
                                kinozal_id,
                                "progress_regression",
                                item_id=item_id,
                                old_value=previous_progress,
                                new_value=progress,
                                details=anomaly_details,
                            )
                        log.warning(
                            "Held delivery for item=%s kinozal_id=%s due to progress regression old=%s new=%s users=%s duplicate=%s",
                            item_id,
                            kinozal_id,
                            previous_progress,
                            progress,
                            len(matches_by_user),
                            bool(existing_anomaly),
                        )
                        if self._should_emit_anomaly_alert(matches_by_user, existing_anomaly):
                            await self._notify_admins_about_anomaly(
                                kinozal_id,
                                item,
                                "progress_regression",
                                previous_progress,
                                progress,
                                details=anomaly_details,
                            )
                        continue

            needs_review = item_requires_match_review(item.to_dict())

            if not matches_by_user and not needs_review:
                continue

            try:
                item = await self.kinozal_service.enrich_item_with_details(item.clone())
                self._record_observation(
                    item,
                    cycle_metrics,
                    source_kind="details",
                    poll_ts=utc_ts(),
                    item_id=item_id,
                    details_title=str(item.get("details_title") or ""),
                )
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
                        event_key=build_delivery_event_key(
                            tg_user_id,
                            item,
                            context="worker",
                            is_release_text_change=False,
                            release_text=str(item.get("source_release_text") or ""),
                        ),
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
                            delivery_context="release_text_update" if is_release_text_change else "worker",
                            event_type=resolve_delivery_event_type(
                                "release_text_update" if is_release_text_change else "worker",
                                is_release_text_change=is_release_text_change,
                            ),
                            event_key=build_delivery_event_key(
                                tg_user_id,
                                item,
                                context="release_text_update" if is_release_text_change else "worker",
                                is_release_text_change=is_release_text_change,
                                release_text=str(item.get("source_release_text") or ""),
                            ),
                        )
                    )

        current_hour = datetime.now(timezone.utc).hour
        await self._flush_due_pending_deliveries(current_hour, cycle_metrics)
        await self._flush_due_debounce(all_pending)

        if not all_pending:
            return

        await self._deliver_current_cycle(all_pending, current_hour, cycle_metrics)

    async def _flush_due_pending_deliveries(self, current_hour: int, cycle_metrics: Dict[str, int]) -> None:
        del current_hour
        leased_rows = list(self.repository.lease_due_pending_deliveries(utc_ts()))
        grouped_pending: Dict[tuple[int, str], List[Dict[str, Any]]] = {}
        singles: List[Dict[str, Any]] = []
        for pending_delivery in leased_rows:
            event_type = compact_spaces(str(pending_delivery.get("event_type") or ""))
            event_key = compact_spaces(str(pending_delivery.get("event_key") or ""))
            if event_type == "grouped" and event_key:
                grouped_pending.setdefault(
                    (int(pending_delivery["tg_user_id"]), _grouped_event_envelope(event_key)),
                    [],
                ).append(pending_delivery)
            else:
                singles.append(pending_delivery)

        for pending_delivery in singles:
            flush_uid = int(pending_delivery["tg_user_id"])
            pending_item_id = int(pending_delivery["item_id"])
            lease_token = str(pending_delivery.get("lease_token") or "")
            event_key = str(pending_delivery.get("event_key") or "")
            pending_is_release_text = bool(int(pending_delivery.get("is_release_text_change") or 0))
            pending_event_type = compact_spaces(str(pending_delivery.get("event_type") or "")) or resolve_delivery_event_type(
                "release_text_update" if pending_is_release_text else "worker",
                is_release_text_change=pending_is_release_text,
            )
            if self.repository.delivery_event_persisted(
                flush_uid,
                pending_item_id,
                event_type=pending_event_type,
                event_key=event_key,
            ):
                self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
                continue

            pending_item_payload = self.repository.get_item_any(pending_item_id)
            pending_item_payload = self._latest_payload_for_queued_item(pending_item_payload)
            if not pending_item_payload:
                self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
                continue

            start_h, end_h, quiet_timezone = self.repository.get_user_quiet_profile(flush_uid)
            quiet_status = quiet_window_status(start_h, end_h, quiet_timezone)
            if quiet_status["active"]:
                self.repository.release_pending_delivery_lease(
                    int(pending_delivery["id"]),
                    lease_token=lease_token,
                    deliver_not_before_ts=next_quiet_window_end_ts(start_h, end_h, quiet_timezone),
                )
                continue

            pending_item = ReleaseItem.from_payload(pending_item_payload)
            try:
                pending_item = await self.kinozal_service.enrich_item_with_details(pending_item)
            except Exception:
                log.warning("Failed to enrich pending item=%s", pending_item_id, exc_info=True)

            pending_subs = self._resolve_delivery_subscriptions(
                flush_uid,
                pending_item,
                str(pending_delivery.get("matched_sub_ids") or ""),
            )
            if not pending_subs:
                self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
                continue
            try:
                log.info("Flushing pending delivery item=%s to user=%s", pending_item_id, flush_uid)
                pending_event_key = compact_spaces(str(pending_delivery.get("event_key") or ""))
                if not self.delivery_service.begin_delivery_claim(
                    flush_uid,
                    pending_item,
                    pending_subs,
                    context="pending_flush",
                    event_type=pending_event_type,
                    event_key=pending_event_key,
                    old_release_text=str(pending_delivery.get("old_release_text") or ""),
                ):
                    self.repository.release_pending_delivery_lease(int(pending_delivery["id"]), lease_token=lease_token)
                    continue
                await self.delivery_service.deliver_claimed_item(
                    flush_uid,
                    pending_item,
                    pending_subs,
                    context="pending_flush",
                    old_release_text=str(pending_delivery.get("old_release_text") or ""),
                    event_type=pending_event_type,
                    event_key=pending_event_key,
                )
                cycle_metrics["deliveries_sent_total"] += 1
                self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
            except Exception as exc:
                self.repository.release_pending_delivery_lease(
                    int(pending_delivery["id"]),
                    lease_token=lease_token,
                    error=str(exc),
                )
                log.exception("Failed to flush pending delivery user=%s item=%s", flush_uid, pending_item_id)

        for (flush_uid, group_envelope), pending_group in grouped_pending.items():
            claimed_group: List[tuple[Dict[str, Any], ReleaseItem, Sequence[SubscriptionRecord], str]] = []
            try:
                for pending_delivery in pending_group:
                    pending_item_id = int(pending_delivery["item_id"])
                    lease_token = str(pending_delivery.get("lease_token") or "")
                    event_key = compact_spaces(str(pending_delivery.get("event_key") or ""))
                    if self.repository.delivery_event_persisted(
                        flush_uid,
                        pending_item_id,
                        event_type="grouped",
                        event_key=event_key,
                    ):
                        self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
                        continue
                    pending_item_payload = self.repository.get_item_any(pending_item_id)
                    pending_item_payload = self._latest_payload_for_queued_item(pending_item_payload)
                    if not pending_item_payload:
                        self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
                        continue
                    pending_item = ReleaseItem.from_payload(pending_item_payload)
                    try:
                        pending_item = await self.kinozal_service.enrich_item_with_details(pending_item)
                    except Exception:
                        log.warning("Failed to enrich grouped pending item=%s", pending_item_id, exc_info=True)
                    pending_subs = self._resolve_delivery_subscriptions(
                        flush_uid,
                        pending_item,
                        str(pending_delivery.get("matched_sub_ids") or ""),
                    )
                    if not pending_subs:
                        self.repository.delete_pending_delivery(flush_uid, pending_item_id, event_key=event_key)
                        continue
                    if not self.delivery_service.begin_delivery_claim(
                        flush_uid,
                        pending_item,
                        pending_subs,
                        context="pending_flush",
                        event_type="grouped",
                        event_key=event_key,
                        grouped_event_key=group_envelope,
                    ):
                        self.repository.release_pending_delivery_lease(int(pending_delivery["id"]), lease_token=lease_token)
                        continue
                    claimed_group.append((pending_delivery, pending_item, pending_subs, event_key))

                if not claimed_group:
                    continue
                if len(claimed_group) == 1:
                    pending_delivery, pending_item, pending_subs, event_key = claimed_group[0]
                    await self.delivery_service.deliver_claimed_item(
                        flush_uid,
                        pending_item,
                        pending_subs,
                        context="grouped_singleton",
                        old_release_text=str(pending_delivery.get("old_release_text") or ""),
                        event_type="grouped",
                        event_key=event_key,
                        grouped_event_key=group_envelope,
                    )
                    self.repository.delete_pending_delivery(flush_uid, pending_item.id, event_key=event_key)
                    cycle_metrics["deliveries_sent_total"] += 1
                    continue

                all_subs = list({sub.id: sub for _, _, subs, _ in claimed_group for sub in subs}.values())
                await self.delivery_service.send_grouped_items(
                    flush_uid,
                    [pending_item for _, pending_item, _, _ in claimed_group],
                    all_subs,
                )
                cycle_metrics["grouped_messages_total"] += 1
                for _pending_delivery, pending_item, pending_subs, event_key in claimed_group:
                    self.delivery_service.record_delivery(
                        flush_uid,
                        pending_item,
                        pending_subs,
                        context="grouped",
                        event_type="grouped",
                        event_key=event_key,
                        grouped_event_key=group_envelope,
                    )
                    self.repository.delete_pending_delivery(flush_uid, pending_item.id, event_key=event_key)
                    cycle_metrics["deliveries_sent_total"] += 1
                    await asyncio.sleep(0.12)
            except Exception as exc:
                for pending_delivery, pending_item, _, event_key in claimed_group:
                    self.delivery_service.mark_delivery_claim_failed(
                        flush_uid,
                        pending_item,
                        error=str(exc),
                        event_key=event_key,
                    )
                    self.repository.release_pending_delivery_lease(
                        int(pending_delivery["id"]),
                        lease_token=str(pending_delivery.get("lease_token") or ""),
                        error=str(exc),
                    )
                log.exception("Failed to flush grouped pending deliveries user=%s envelope=%s", flush_uid, group_envelope)

    async def _flush_due_debounce(self, all_pending: Dict[int, List[DeliveryCandidate]]) -> None:
        enriched_cache: Dict[int, ReleaseItem] = {}
        for entry in self.repository.lease_due_debounce_entries(utc_ts()):
            tg_user_id = int(entry["tg_user_id"])
            item_id = int(entry["item_id"])
            debounce_kinozal_id = str(entry.get("kinozal_id") or "")
            lease_token = str(entry.get("lease_token") or "")
            if self.repository.delivered_persisted(tg_user_id, item_id):
                self.repository.delete_debounce_entry(tg_user_id, debounce_kinozal_id)
                continue

            if item_id not in enriched_cache:
                raw_item = self.repository.get_item_any(item_id)
                raw_item = self._latest_payload_for_queued_item(raw_item, str(entry.get("kinozal_id") or ""))
                if not raw_item:
                    self.repository.delete_debounce_entry(tg_user_id, debounce_kinozal_id)
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
            if self.repository.delivered_equivalent_persisted(tg_user_id, item.to_dict()):
                self.repository.delete_debounce_entry(tg_user_id, debounce_kinozal_id)
                continue

            subs = self._resolve_delivery_subscriptions(
                tg_user_id,
                item,
                str(entry.get("matched_sub_ids") or ""),
            )
            if not subs:
                self.repository.delete_debounce_entry(tg_user_id, debounce_kinozal_id)
                continue

            log.info("Debounce ready item=%s kinozal_id=%s to user=%s", item_id, entry["kinozal_id"], tg_user_id)
            all_pending.setdefault(tg_user_id, []).append(
                DeliveryCandidate(
                    item=item.clone(),
                    subs=subs,
                    old_release_text="",
                    is_release_text_change=False,
                    debounce_kinozal_id=debounce_kinozal_id,
                    delivery_context="worker",
                    event_type=resolve_delivery_event_type("worker"),
                    event_key=build_delivery_event_key(
                        tg_user_id,
                        item,
                        context="worker",
                        is_release_text_change=False,
                        release_text=str(item.get("source_release_text") or ""),
                    ),
                    queue_lease_token=lease_token,
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
                quiet_start, quiet_end, quiet_timezone = self.repository.get_user_quiet_profile(tg_user_id)
                quiet_status = quiet_window_status(quiet_start, quiet_end, quiet_timezone)
                if quiet_status["active"]:
                    deliver_not_before_ts = next_quiet_window_end_ts(quiet_start, quiet_end, quiet_timezone)
                    release_text_updates = [delivery for delivery in deliveries if delivery.is_release_text_change]
                    regular_deliveries = [delivery for delivery in deliveries if not delivery.is_release_text_change]
                    tmdb_groups: Dict[int, List[DeliveryCandidate]] = {}
                    passthrough_deliveries: List[tuple[DeliveryCandidate, str, str]] = []
                    for delivery in regular_deliveries:
                        tmdb_id = delivery.item.tmdb_id
                        if tmdb_id:
                            tmdb_groups.setdefault(tmdb_id, []).append(delivery)
                        else:
                            event_type, event_key = self.delivery_service.build_candidate_delivery_event(tg_user_id, delivery, context=delivery.delivery_context or "worker")
                            passthrough_deliveries.append((delivery, event_type, event_key))
                    for delivery in release_text_updates:
                        event_type, event_key = self.delivery_service.build_candidate_delivery_event(tg_user_id, delivery, context="release_text_update")
                        passthrough_deliveries.append((delivery, event_type, event_key))
                    for tmdb_id, group in tmdb_groups.items():
                        if len(group) >= 2:
                            _, group_envelope = self.delivery_service.build_group_delivery_event(
                                tg_user_id,
                                [delivery.item for delivery in group],
                                group_key=f"tmdb:{tmdb_id}",
                            )
                            for delivery in group:
                                passthrough_deliveries.append(
                                    (
                                        delivery,
                                        "grouped",
                                        f"{group_envelope}:item:{delivery.item_id}",
                                    )
                                )
                        else:
                            event_type, event_key = self.delivery_service.build_candidate_delivery_event(tg_user_id, group[0], context=group[0].delivery_context or "worker")
                            passthrough_deliveries.append((group[0], event_type, event_key))

                    for delivery, queued_event_type, queued_event_key in passthrough_deliveries:
                        sub_ids_str = ",".join(str(sub.id) for sub in delivery.subs)
                        self.repository.queue_pending_delivery(
                            tg_user_id,
                            delivery.item_id,
                            sub_ids_str,
                            delivery.old_release_text,
                            delivery.is_release_text_change,
                            event_type=queued_event_type,
                            event_key=queued_event_key,
                            deliver_not_before_ts=deliver_not_before_ts,
                        )
                        if delivery.debounce_kinozal_id:
                            self.repository.delete_debounce_entry(tg_user_id, delivery.debounce_kinozal_id)
                    cycle_metrics["pending_queued_total"] += len(deliveries)
                    log.info(
                        "Queued %d deliveries for user=%s (quiet %02d:00-%02d:00 %s local_hour=%02d)",
                        len(deliveries),
                        tg_user_id,
                        quiet_start,
                        quiet_end,
                        quiet_status["timezone"],
                        quiet_status["local_hour"],
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
                        event_type, event_key = self.delivery_service.build_candidate_delivery_event(
                            tg_user_id,
                            delivery,
                            context="release_text_update",
                        )
                        log.info(
                            "Delivering release text update item=%s to user=%s source_uid=%s",
                            delivery.item_id,
                            tg_user_id,
                            delivery.item.source_uid,
                        )
                        if not self.delivery_service.begin_delivery_claim(
                            tg_user_id,
                            delivery.item,
                            delivery.subs,
                            context="release_text_update",
                            event_type=event_type,
                            event_key=event_key,
                            old_release_text=delivery.old_release_text,
                        ):
                            continue
                        await self.delivery_service.deliver_claimed_item(
                            tg_user_id,
                            delivery.item,
                            delivery.subs,
                            context="release_text_update",
                            old_release_text=delivery.old_release_text,
                            event_type=event_type,
                            event_key=event_key,
                        )
                        if delivery.debounce_kinozal_id:
                            self.repository.delete_debounce_entry(tg_user_id, delivery.debounce_kinozal_id)
                        cycle_metrics["deliveries_sent_total"] += 1
                    except Exception:
                        if delivery.debounce_kinozal_id and delivery.queue_lease_token:
                            self.repository.release_debounce_lease(
                                tg_user_id,
                                delivery.debounce_kinozal_id,
                                lease_token=delivery.queue_lease_token,
                            )
                        log.exception("Error delivering rtc item=%s to user=%s", delivery.item_id, tg_user_id)

                for delivery in without_tmdb:
                    if self.repository.delivered(tg_user_id, delivery.item_id):
                        if delivery.debounce_kinozal_id:
                            self.repository.delete_debounce_entry(tg_user_id, delivery.debounce_kinozal_id)
                        continue
                    try:
                        await self.delivery_service.send_single(tg_user_id, delivery)
                        if delivery.debounce_kinozal_id:
                            self.repository.delete_debounce_entry(tg_user_id, delivery.debounce_kinozal_id)
                        cycle_metrics["deliveries_sent_total"] += 1
                    except Exception:
                        if delivery.debounce_kinozal_id and delivery.queue_lease_token:
                            self.repository.release_debounce_lease(
                                tg_user_id,
                                delivery.debounce_kinozal_id,
                                lease_token=delivery.queue_lease_token,
                            )
                        log.exception("Error delivering item=%s to user=%s", delivery.item_id, tg_user_id)

                for tmdb_id, group in tmdb_groups.items():
                    try:
                        group = [delivery for delivery in group if not self.repository.delivered(tg_user_id, delivery.item_id)]
                        if not group:
                            continue
                        if len(group) >= 2:
                            group_event_type, group_event_envelope = self.delivery_service.build_group_delivery_event(
                                tg_user_id,
                                [delivery.item for delivery in group],
                                group_key=f"tmdb:{tmdb_id}",
                            )
                            claimed_group: List[tuple[DeliveryCandidate, str]] = []
                            for delivery in group:
                                member_event_key = f"{group_event_envelope}:item:{delivery.item_id}"
                                if self.delivery_service.begin_delivery_claim(
                                    tg_user_id,
                                    delivery.item,
                                    delivery.subs,
                                    context="grouped",
                                    event_type=group_event_type,
                                    event_key=member_event_key,
                                    grouped_event_key=group_event_envelope,
                                ):
                                    claimed_group.append((delivery, member_event_key))
                            if not claimed_group:
                                continue
                            if len(claimed_group) == 1:
                                await self.delivery_service.deliver_claimed_item(
                                    tg_user_id,
                                    claimed_group[0][0].item,
                                    claimed_group[0][0].subs,
                                    context="grouped_singleton",
                                    old_release_text=claimed_group[0][0].old_release_text,
                                    event_type=group_event_type,
                                    event_key=claimed_group[0][1],
                                    grouped_event_key=group_event_envelope,
                                )
                                if claimed_group[0][0].debounce_kinozal_id:
                                    self.repository.delete_debounce_entry(tg_user_id, claimed_group[0][0].debounce_kinozal_id)
                                cycle_metrics["deliveries_sent_total"] += 1
                                continue
                            all_subs = list({sub.id: sub for delivery, _ in claimed_group for sub in delivery.subs}.values())
                            log.info("Delivering grouped %d items tmdb=%s to user=%s", len(claimed_group), tmdb_id, tg_user_id)
                            try:
                                await self.delivery_service.send_grouped_items(
                                    tg_user_id,
                                    [delivery.item for delivery, _ in claimed_group],
                                    all_subs,
                                )
                                cycle_metrics["grouped_messages_total"] += 1
                                for delivery, member_event_key in claimed_group:
                                    self.delivery_service.record_delivery(
                                        tg_user_id,
                                        delivery.item,
                                        delivery.subs,
                                        context="grouped",
                                        event_type=group_event_type,
                                        event_key=member_event_key,
                                        grouped_event_key=group_event_envelope,
                                    )
                                    if delivery.debounce_kinozal_id:
                                        self.repository.delete_debounce_entry(tg_user_id, delivery.debounce_kinozal_id)
                                    cycle_metrics["deliveries_sent_total"] += 1
                                    await asyncio.sleep(0.12)
                            except Exception as exc:
                                for delivery, member_event_key in claimed_group:
                                    self.delivery_service.mark_delivery_claim_failed(
                                        tg_user_id,
                                        delivery.item,
                                        error=str(exc),
                                        event_key=member_event_key,
                                    )
                                    if delivery.debounce_kinozal_id and delivery.queue_lease_token:
                                        self.repository.release_debounce_lease(
                                            tg_user_id,
                                            delivery.debounce_kinozal_id,
                                            lease_token=delivery.queue_lease_token,
                                            error=str(exc),
                                        )
                                raise
                        else:
                            await self.delivery_service.send_single(tg_user_id, group[0])
                            if group[0].debounce_kinozal_id:
                                self.repository.delete_debounce_entry(tg_user_id, group[0].debounce_kinozal_id)
                            cycle_metrics["deliveries_sent_total"] += 1
                    except Exception:
                        for delivery in group:
                            if delivery.debounce_kinozal_id and delivery.queue_lease_token:
                                self.repository.release_debounce_lease(
                                    tg_user_id,
                                    delivery.debounce_kinozal_id,
                                    lease_token=delivery.queue_lease_token,
                                )
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
