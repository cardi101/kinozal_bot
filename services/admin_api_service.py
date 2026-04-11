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

    def get_metrics_payload(self) -> bytes:
        db_ok = True
        try:
            self.db.conn.execute("SELECT 1").fetchone()
        except Exception:
            db_ok = False

        users_total = self.db.count_users()
        subscriptions_enabled = len(self.db.list_enabled_subscriptions())
        source_fail_streak = int(self.db.get_meta("source_fail_streak") or 0)
        source_last_success_at = int(self.db.get_meta("source_last_success_at") or 0)
        source_last_failed_at = int(self.db.get_meta("source_last_failed_at") or 0)
        source_status = self.db.get_meta("source_health_status") or "unknown"
        return build_metrics_payload(
            database_up=db_ok,
            users_total=users_total,
            subscriptions_enabled=subscriptions_enabled,
            source_fail_streak=source_fail_streak,
            source_last_success_at=source_last_success_at,
            source_last_failed_at=source_last_failed_at,
            source_status=source_status,
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
