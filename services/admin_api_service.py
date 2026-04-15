from datetime import datetime, timezone
from typing import Any, Dict, List

from delivery_audit import build_delivery_audit
from delivery_sender import send_item_to_user
from domain import ReleaseItem
from match_debug_helpers import _strip_existing_match_fields, build_match_explanation
from metrics_registry import build_metrics_payload
from release_versioning import parse_episode_progress
from subscription_matching import explain_subscription_match, match_subscription


class AdminApiService:
    def __init__(self, db: Any, tmdb_service: Any, kinozal_service: Any, bot: Any = None) -> None:
        self.db = db
        self.tmdb_service = tmdb_service
        self.kinozal_service = kinozal_service
        self.bot = bot

    def get_health(self) -> Dict[str, Any]:
        db_ok = True
        try:
            self.db.conn.execute("SELECT 1").fetchone()
        except Exception:
            db_ok = False

        return {
            "status": "ok" if db_ok else "degraded",
            "database_ok": db_ok,
            "source_health_status": self.db.get_meta("source_health_status") or "unknown",
            "source_fail_streak": int(self.db.get_meta("source_fail_streak") or 0),
            "source_last_success_at": int(self.db.get_meta("source_last_success_at") or 0),
            "source_last_failed_at": int(self.db.get_meta("source_last_failed_at") or 0),
        }

    def _meta_int(self, key: str, default: int = 0) -> int:
        try:
            value = self.db.get_meta(key)
            return int(value) if value is not None else default
        except Exception:
            return default

    def _meta_float(self, key: str, default: float = 0.0) -> float:
        try:
            value = self.db.get_meta(key)
            return float(value) if value is not None else default
        except Exception:
            return default

    def get_metrics_payload(self) -> bytes:
        db_ok = True
        try:
            self.db.conn.execute("SELECT 1").fetchone()
        except Exception:
            db_ok = False

        users_total = self.db.count_users()
        subscriptions_enabled = len(self.db.list_enabled_subscriptions())
        source_fail_streak = self._meta_int("source_fail_streak")
        source_last_success_at = self._meta_int("source_last_success_at")
        source_last_failed_at = self._meta_int("source_last_failed_at")
        source_status = self.db.get_meta("source_health_status") or "unknown"
        return build_metrics_payload(
            database_up=db_ok,
            users_total=users_total,
            subscriptions_enabled=subscriptions_enabled,
            source_fail_streak=source_fail_streak,
            source_last_success_at=source_last_success_at,
            source_last_failed_at=source_last_failed_at,
            source_status=source_status,
            extra_counters={
                "kinozal_bot_worker_cycles_total": (
                    "Total completed worker cycles",
                    self._meta_int("metrics_worker_cycles_total"),
                ),
                "kinozal_bot_worker_cycle_failures_total": (
                    "Total failed worker cycles",
                    self._meta_int("metrics_worker_cycle_failures_total"),
                ),
                "kinozal_bot_worker_items_fetched_total": (
                    "Total items fetched from source",
                    self._meta_int("metrics_worker_items_fetched_total"),
                ),
                "kinozal_bot_worker_items_filtered_non_video_total": (
                    "Total source items skipped as non-video",
                    self._meta_int("metrics_worker_items_filtered_non_video_total"),
                ),
                "kinozal_bot_worker_items_filtered_non_video_by_category_total": (
                    "Total source items skipped as non-video by source category",
                    self._meta_int("metrics_worker_items_filtered_non_video_by_category_total"),
                ),
                "kinozal_bot_worker_items_filtered_russian_total": (
                    "Total source items skipped as Russian content",
                    self._meta_int("metrics_worker_items_filtered_russian_total"),
                ),
                "kinozal_bot_worker_items_tmdb_enriched_total": (
                    "Total items passed through TMDB enrichment",
                    self._meta_int("metrics_worker_items_tmdb_enriched_total"),
                ),
                "kinozal_bot_worker_items_saved_new_total": (
                    "Total new items inserted into database",
                    self._meta_int("metrics_worker_items_saved_new_total"),
                ),
                "kinozal_bot_worker_items_saved_updated_total": (
                    "Total existing items materially updated",
                    self._meta_int("metrics_worker_items_saved_updated_total"),
                ),
                "kinozal_bot_worker_release_text_changes_total": (
                    "Total detected release text changes",
                    self._meta_int("metrics_worker_release_text_changes_total"),
                ),
                "kinozal_bot_worker_debounce_queued_total": (
                    "Total deliveries queued into debounce",
                    self._meta_int("metrics_worker_debounce_queued_total"),
                ),
                "kinozal_bot_worker_pending_queued_total": (
                    "Total deliveries queued due to quiet hours",
                    self._meta_int("metrics_worker_pending_queued_total"),
                ),
                "kinozal_bot_worker_deliveries_sent_total": (
                    "Total delivered item notifications",
                    self._meta_int("metrics_worker_deliveries_sent_total"),
                ),
                "kinozal_bot_worker_grouped_messages_total": (
                    "Total grouped delivery messages sent",
                    self._meta_int("metrics_worker_grouped_messages_total"),
                ),
                "kinozal_bot_worker_bootstrap_marked_read_total": (
                    "Total deliveries marked as read during bootstrap",
                    self._meta_int("metrics_worker_bootstrap_marked_read_total"),
                ),
                "kinozal_bot_worker_observations_recorded_total": (
                    "Total stored source observations",
                    self._meta_int("metrics_worker_observations_recorded_total"),
                ),
                "kinozal_bot_worker_progress_regressions_total": (
                    "Total detected progress regressions",
                    self._meta_int("metrics_worker_progress_regressions_total"),
                ),
                "kinozal_bot_worker_anomaly_holds_total": (
                    "Total deliveries held by anomaly detection",
                    self._meta_int("metrics_worker_anomaly_holds_total"),
                ),
                "kinozal_bot_admin_replayed_deliveries_total": (
                    "Total deliveries replayed manually from admin API",
                    self._meta_int("metrics_admin_replayed_deliveries_total"),
                ),
            },
            extra_gauges={
                "kinozal_bot_worker_cycle_duration_seconds": (
                    "Last completed worker cycle duration in seconds",
                    self._meta_float("metrics_worker_cycle_duration_seconds"),
                ),
                "kinozal_bot_worker_cycle_last_started_at": (
                    "Last worker cycle start timestamp",
                    self._meta_int("metrics_worker_cycle_last_started_at"),
                ),
                "kinozal_bot_worker_cycle_last_finished_at": (
                    "Last worker cycle finish timestamp",
                    self._meta_int("metrics_worker_cycle_last_finished_at"),
                ),
                "kinozal_bot_worker_last_cycle_items_fetched": (
                    "Items fetched in the last worker cycle",
                    self._meta_int("metrics_worker_last_cycle_items_fetched"),
                ),
                "kinozal_bot_worker_last_cycle_new_items": (
                    "New items inserted in the last worker cycle",
                    self._meta_int("metrics_worker_last_cycle_new_items"),
                ),
                "kinozal_bot_worker_last_cycle_updated_items": (
                    "Updated items in the last worker cycle",
                    self._meta_int("metrics_worker_last_cycle_updated_items"),
                ),
                "kinozal_bot_worker_last_cycle_deliveries_sent": (
                    "Delivered item notifications in the last worker cycle",
                    self._meta_int("metrics_worker_last_cycle_deliveries_sent"),
                ),
            },
        )

    def get_user_subscriptions(self, user_id: int) -> Dict[str, Any]:
        user = self.db.get_user_with_subscriptions(user_id)
        if not user:
            raise LookupError(f"user {user_id} not found")
        return user

    def _find_item_any_by_kinozal_id(self, kinozal_id: str) -> Dict[str, Any] | None:
        item = self.db.find_item_by_kinozal_id(kinozal_id)
        if item:
            return item
        finder = getattr(self.db, "find_item_any_by_kinozal_id", None)
        if finder is None:
            return None
        return finder(kinozal_id)

    async def build_match_debug(self, kinozal_id: str, live: bool = True) -> Dict[str, Any]:
        item = self._find_item_any_by_kinozal_id(kinozal_id)
        if not item:
            raise LookupError(f"kinozal_id {kinozal_id} not found")

        live_item = None
        if live:
            rematch_input = ReleaseItem.from_payload(_strip_existing_match_fields(item))
            live_item = (await self.tmdb_service.enrich_item(rematch_input)).to_dict()

        return {
            "kinozal_id": str(kinozal_id),
            "stored_item": item,
            "live_item": live_item,
            "explanation_html": build_match_explanation(self.db, item, live_item),
        }

    async def reparse_release(self, kinozal_id: str) -> Dict[str, Any]:
        item = self._find_item_any_by_kinozal_id(kinozal_id)
        if not item:
            raise LookupError(f"kinozal_id {kinozal_id} not found")

        before_release_text = str(item.get("source_release_text") or "")
        refreshed = await self.kinozal_service.enrich_item_with_details(
            ReleaseItem.from_payload(item),
            force_refresh=True,
        )
        details_title = str(refreshed.get("details_title") or "")
        details_progress = parse_episode_progress(details_title)
        if details_title and details_progress and details_progress != str(refreshed.get("source_episode_progress") or ""):
            refreshed.set("source_title", details_title)
            refreshed.set("source_episode_progress", details_progress)
        after_release_text = str(refreshed.get("source_release_text") or "")
        refreshed_item_id, _, _ = self.db.save_item(refreshed.to_dict())
        persisted_item = self.db.get_item(int(refreshed_item_id)) or refreshed.to_dict()

        return {
            "kinozal_id": str(kinozal_id),
            "item_id": int(refreshed_item_id),
            "title_before": str(item.get("source_title") or ""),
            "title_after": str(refreshed.get("source_title") or ""),
            "details_title": str(refreshed.get("details_title") or ""),
            "release_text_changed": before_release_text != after_release_text,
            "release_text_length_before": len(before_release_text),
            "release_text_length_after": len(after_release_text),
            "item": persisted_item,
        }

    def get_release_timeline(
        self,
        kinozal_id: str,
        versions_limit: int = 20,
        observations_limit: int = 50,
        anomalies_limit: int = 20,
        deliveries_limit: int = 20,
    ) -> Dict[str, Any]:
        version_timeline = self.db.get_version_timeline(kinozal_id, limit=versions_limit)
        observations = self.db.list_source_observations(kinozal_id, limit=observations_limit)
        anomalies = self.db.list_release_anomalies(kinozal_id, limit=anomalies_limit)
        deliveries = self.db.get_delivery_audits(kinozal_id, limit=deliveries_limit)
        current_item = self.db.find_item_by_kinozal_id(kinozal_id)
        if not current_item and not observations and not anomalies and not deliveries:
            raise LookupError(f"kinozal_id {kinozal_id} not found")
        return {
            "kinozal_id": str(kinozal_id),
            "current_item": current_item,
            "version_timeline": version_timeline,
            "source_observations": observations,
            "release_anomalies": anomalies,
            "delivery_audits": deliveries,
        }

    def explain_delivery(
        self,
        kinozal_id: str,
        tg_user_id: int,
        cooldown_seconds: int = 420,
    ) -> Dict[str, Any]:
        user = self.db.get_user(int(tg_user_id))
        if not user:
            raise LookupError(f"user {tg_user_id} not found")
        item = self.db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            raise LookupError(f"kinozal_id {kinozal_id} not found")

        matched_subscriptions: List[Dict[str, Any]] = []
        mismatched_subscriptions: List[Dict[str, Any]] = []
        for sub in self.db.list_user_subscriptions(int(tg_user_id)):
            sub_full = self.db.get_subscription(int(sub["id"]))
            if not sub_full:
                continue
            if not int(sub_full.get("is_enabled") or 0):
                mismatched_subscriptions.append(
                    {
                        "id": int(sub_full["id"]),
                        "name": str(sub_full.get("name") or ""),
                        "reason": "disabled",
                    }
                )
                continue
            reason = explain_subscription_match(self.db, sub_full, item)
            payload = {
                "id": int(sub_full["id"]),
                "name": str(sub_full.get("name") or ""),
                "reason": reason,
            }
            if reason == "passed":
                matched_subscriptions.append(payload)
            else:
                mismatched_subscriptions.append(payload)

        item_tmdb_id = int(item["tmdb_id"]) if item.get("tmdb_id") not in (None, "") else None
        muted = bool(item_tmdb_id and self.db.is_title_muted(int(tg_user_id), item_tmdb_id))
        delivered_exact = self.db.delivered(int(tg_user_id), int(item["id"]))
        delivered_equivalent = self.db.delivered_equivalent(int(tg_user_id), item)
        cooldown_active = bool(
            kinozal_id and self.db.recently_delivered_kinozal_id(int(tg_user_id), str(kinozal_id), cooldown_seconds)
        )

        quiet_start, quiet_end = self.db.get_user_quiet_hours(int(tg_user_id))
        current_hour = datetime.now(timezone.utc).hour
        quiet_active = False
        if quiet_start is not None and quiet_end is not None:
            if quiet_start < quiet_end:
                quiet_active = quiet_start <= current_hour < quiet_end
            else:
                quiet_active = current_hour >= quiet_start or current_hour < quiet_end

        pending_delivery = self.db.conn.execute(
            """
            SELECT item_id, matched_sub_ids, old_release_text, is_release_text_change, queued_at
            FROM pending_deliveries
            WHERE tg_user_id = ? AND item_id = ?
            LIMIT 1
            """,
            (int(tg_user_id), int(item["id"])),
        ).fetchone()
        debounce_entry = self.db.conn.execute(
            """
            SELECT tg_user_id, kinozal_id, item_id, matched_sub_ids, deliver_after_ts, reset_count
            FROM debounce_queue
            WHERE tg_user_id = ? AND kinozal_id = ?
            LIMIT 1
            """,
            (int(tg_user_id), str(kinozal_id)),
        ).fetchone()

        anomalies = [
            anomaly
            for anomaly in self.db.list_release_anomalies(str(kinozal_id), limit=10)
            if int(anomaly.get("item_id") or 0) == int(item["id"]) and str(anomaly.get("status") or "") == "open"
        ]
        pending_match_review = self.db.get_pending_match_review_by_item_id(int(item["id"]))

        status = "ready"
        blockers: List[str] = []
        if muted:
            status = "skipped"
            blockers.append("muted_title")
        if not matched_subscriptions:
            status = "skipped"
            blockers.append("no_matching_enabled_subscriptions")
        if delivered_exact:
            status = "delivered"
            blockers.append("delivered_exact")
        elif delivered_equivalent:
            status = "skipped"
            blockers.append("delivered_equivalent")
        elif cooldown_active:
            status = "waiting"
            blockers.append("cooldown")
        if pending_delivery:
            status = "waiting"
            blockers.append("pending_delivery")
        if debounce_entry:
            status = "waiting"
            blockers.append("debounce")
        if pending_match_review:
            status = "waiting"
            blockers.append("match_review")
        if anomalies:
            status = "held"
            blockers.append("anomaly_hold")
        if quiet_active:
            status = "waiting"
            blockers.append("quiet_hours")

        return {
            "kinozal_id": str(kinozal_id),
            "tg_user_id": int(tg_user_id),
            "item_id": int(item["id"]),
            "status": status,
            "blockers": blockers,
            "current_item": item,
            "matched_subscriptions": matched_subscriptions,
            "mismatched_subscriptions": mismatched_subscriptions,
            "muted_title": muted,
            "delivered_exact": delivered_exact,
            "delivered_equivalent": delivered_equivalent,
            "cooldown_active": cooldown_active,
            "cooldown_seconds": int(cooldown_seconds),
            "quiet_hours": {
                "start_hour_utc": quiet_start,
                "end_hour_utc": quiet_end,
                "active_now": quiet_active,
                "current_hour_utc": current_hour,
            },
            "pending_delivery": dict(pending_delivery) if pending_delivery else None,
            "debounce_entry": dict(debounce_entry) if debounce_entry else None,
            "pending_match_review": pending_match_review,
            "open_anomalies": anomalies,
        }

    async def replay_delivery(
        self,
        kinozal_id: str,
        tg_user_id: int,
        force: bool = False,
    ) -> Dict[str, Any]:
        if self.bot is None:
            raise RuntimeError("admin replay is unavailable without bot context")

        user = self.db.get_user(int(tg_user_id))
        if not user:
            raise LookupError(f"user {tg_user_id} not found")
        finder = getattr(self.db, "find_item_any_by_kinozal_id", None) or self.db.find_item_by_kinozal_id
        item = finder(kinozal_id)
        if not item:
            raise LookupError(f"kinozal_id {kinozal_id} not found")

        enabled_subs: List[Dict[str, Any]] = []
        mismatches: List[Dict[str, Any]] = []
        for sub in self.db.list_user_subscriptions(int(tg_user_id)):
            sub_full = self.db.get_subscription(int(sub["id"]))
            if not sub_full or not int(sub_full.get("is_enabled") or 0):
                continue
            if match_subscription(self.db, sub_full, item):
                enabled_subs.append(sub_full)
            else:
                mismatches.append(
                    {
                        "id": int(sub_full["id"]),
                        "name": str(sub_full.get("name") or ""),
                        "reason": explain_subscription_match(self.db, sub_full, item),
                    }
                )

        if not enabled_subs and not force:
            return {
                "status": "skipped",
                "reason": "no_matching_enabled_subscriptions",
                "kinozal_id": str(kinozal_id),
                "tg_user_id": int(tg_user_id),
                "matched_subscriptions": [],
                "mismatches": mismatches,
            }

        if self.db.delivered_equivalent(int(tg_user_id), item) and not force:
            return {
                "status": "skipped",
                "reason": "delivered_equivalent",
                "kinozal_id": str(kinozal_id),
                "tg_user_id": int(tg_user_id),
                "item_id": int(item["id"]),
                "matched_subscriptions": [
                    {"id": int(sub["id"]), "name": str(sub.get("name") or "")}
                    for sub in enabled_subs
                ],
                "mismatches": mismatches,
            }

        if not force:
            explanation = self.explain_delivery(str(kinozal_id), int(tg_user_id))
            suppressors = {
                "muted_title",
                "cooldown",
                "pending_delivery",
                "debounce",
                "match_review",
                "anomaly_hold",
                "quiet_hours",
            }
            active_suppressors = [blocker for blocker in explanation.get("blockers", []) if blocker in suppressors]
            if active_suppressors:
                return {
                    "status": "skipped",
                    "reason": active_suppressors[0],
                    "kinozal_id": str(kinozal_id),
                    "tg_user_id": int(tg_user_id),
                    "item_id": int(item["id"]),
                    "matched_subscriptions": [
                        {"id": int(sub["id"]), "name": str(sub.get("name") or "")}
                        for sub in enabled_subs
                    ],
                    "mismatches": mismatches,
                    "blockers": active_suppressors,
                }

        delivery_audit = build_delivery_audit(self.db, item, enabled_subs, context="admin_replay")
        matched_ids = [int(sub["id"]) for sub in enabled_subs]
        primary_sub_id = int(enabled_subs[0]["id"]) if enabled_subs else None
        if not self.db.begin_delivery_claim(
            int(tg_user_id),
            int(item["id"]),
            primary_sub_id,
            matched_ids,
            delivery_audit=delivery_audit,
            context="admin_replay",
        ):
            return {
                "status": "skipped",
                "reason": "delivery_claim_exists",
                "kinozal_id": str(kinozal_id),
                "tg_user_id": int(tg_user_id),
                "item_id": int(item["id"]),
            }

        try:
            await send_item_to_user(
                self.db,
                self.bot,
                int(tg_user_id),
                item,
                enabled_subs or None,
            )
            if not self.db.delivered(int(tg_user_id), int(item["id"])):
                self.db.record_delivery(
                    int(tg_user_id),
                    int(item["id"]),
                    primary_sub_id,
                    matched_ids,
                    delivery_audit=delivery_audit,
                )
        except Exception as exc:
            self.db.mark_delivery_claim_failed(int(tg_user_id), int(item["id"]), error=str(exc))
            raise
        self._increment_admin_metric("metrics_admin_replayed_deliveries_total", 1)
        return {
            "status": "sent",
            "reason": "forced" if force else "matched",
            "kinozal_id": str(kinozal_id),
            "tg_user_id": int(tg_user_id),
            "item_id": int(item["id"]),
            "matched_subscriptions": [
                {"id": int(sub["id"]), "name": str(sub.get("name") or "")}
                for sub in enabled_subs
            ],
            "mismatches": mismatches,
        }

    def _increment_admin_metric(self, key: str, delta: int) -> None:
        if delta <= 0:
            return
        current = self._meta_int(key)
        self.db.set_meta(key, str(current + delta))
