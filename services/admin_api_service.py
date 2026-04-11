from typing import Any, Dict

from domain import ReleaseItem
from match_debug_helpers import _strip_existing_match_fields, build_match_explanation
from metrics_registry import build_metrics_payload


class AdminApiService:
    def __init__(self, db: Any, tmdb_service: Any, kinozal_service: Any) -> None:
        self.db = db
        self.tmdb_service = tmdb_service
        self.kinozal_service = kinozal_service

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

    async def build_match_debug(self, kinozal_id: str, live: bool = True) -> Dict[str, Any]:
        item = self.db.find_item_by_kinozal_id(kinozal_id)
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
        item = self.db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            raise LookupError(f"kinozal_id {kinozal_id} not found")

        before_release_text = str(item.get("source_release_text") or "")
        refreshed = await self.kinozal_service.enrich_item_with_details(
            ReleaseItem.from_payload(item),
            force_refresh=True,
        )
        after_release_text = str(refreshed.get("source_release_text") or "")
        if after_release_text:
            self.db.update_item_release_text(int(item["id"]), after_release_text)

        return {
            "kinozal_id": str(kinozal_id),
            "item_id": int(item["id"]),
            "title_before": str(item.get("source_title") or ""),
            "title_after": str(refreshed.get("source_title") or ""),
            "details_title": str(refreshed.get("details_title") or ""),
            "release_text_changed": before_release_text != after_release_text,
            "release_text_length_before": len(before_release_text),
            "release_text_length_after": len(after_release_text),
            "item": refreshed.to_dict(),
        }
