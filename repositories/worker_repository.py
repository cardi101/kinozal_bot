from typing import Any, Dict, List, Optional, Tuple


class WorkerRepository:
    def __init__(self, db: Any) -> None:
        self.db = db

    def get_meta(self, key: str) -> Optional[str]:
        return self.db.get_meta(key)

    def set_meta(self, key: str, value: str) -> None:
        self.db.set_meta(key, value)

    def find_existing_enriched(self, source_uid: Any, source_title: Any) -> Optional[Dict[str, Any]]:
        return self.db.find_existing_enriched(source_uid, source_title)

    def save_item(self, item: Dict[str, Any]) -> Tuple[int, bool, bool]:
        return self.db.save_item(item)

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        return self.db.get_item(item_id)

    def get_item_any(self, item_id: int) -> Optional[Dict[str, Any]]:
        return self.db.get_item_any(item_id)

    def update_item_release_text(self, item_id: int, release_text: str) -> None:
        self.db.update_item_release_text(item_id, release_text)

    def record_source_observation(
        self,
        kinozal_id: str,
        source_kind: str,
        poll_ts: Optional[int] = None,
        item_id: Optional[int] = None,
        source_title: str = "",
        details_title: str = "",
        episode_progress: str = "",
        release_text: str = "",
        source_format: str = "",
        source_audio_tracks: Any = None,
        raw_payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        return self.db.record_source_observation(
            kinozal_id=kinozal_id,
            source_kind=source_kind,
            poll_ts=poll_ts,
            item_id=item_id,
            source_title=source_title,
            details_title=details_title,
            episode_progress=episode_progress,
            release_text=release_text,
            source_format=source_format,
            source_audio_tracks=source_audio_tracks,
            raw_payload=raw_payload,
        )

    def record_release_anomaly(
        self,
        kinozal_id: str,
        anomaly_type: str,
        item_id: Optional[int] = None,
        old_value: str = "",
        new_value: str = "",
        details: str = "",
        status: str = "open",
    ) -> int:
        return self.db.record_release_anomaly(
            kinozal_id=kinozal_id,
            anomaly_type=anomaly_type,
            item_id=item_id,
            old_value=old_value,
            new_value=new_value,
            details=details,
            status=status,
        )

    def get_open_release_anomaly(
        self,
        kinozal_id: str,
        anomaly_type: str,
        old_value: str = "",
        new_value: str = "",
    ) -> Optional[Dict[str, Any]]:
        return self.db.get_open_release_anomaly(
            kinozal_id=kinozal_id,
            anomaly_type=anomaly_type,
            old_value=old_value,
            new_value=new_value,
        )

    def find_higher_progress_reference(
        self,
        kinozal_id: str,
        progress: str,
        item_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        return self.db.find_higher_progress_reference(kinozal_id, progress, item_id=item_id)

    def was_delivered_to_anyone(self, item_id: int) -> bool:
        return self.db.was_delivered_to_anyone(item_id)

    def list_enabled_subscriptions(self) -> List[Dict[str, Any]]:
        return self.db.list_enabled_subscriptions()

    def get_subscription(self, subscription_id: int) -> Optional[Dict[str, Any]]:
        return self.db.get_subscription(subscription_id)

    def list_user_subscriptions(self, tg_user_id: int) -> List[Dict[str, Any]]:
        return self.db.list_user_subscriptions(tg_user_id)

    def get_subscription_genres(self, subscription_id: int) -> List[int]:
        return self.db.get_subscription_genres(subscription_id)

    def is_title_muted(self, tg_user_id: int, tmdb_id: int) -> bool:
        return self.db.is_title_muted(tg_user_id, tmdb_id)

    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        return self.db.delivered(tg_user_id, item_id)

    def delivered_persisted(self, tg_user_id: int, item_id: int) -> bool:
        return self.db.delivered_persisted(tg_user_id, item_id)

    def delivered_equivalent(self, tg_user_id: int, item: Dict[str, Any]) -> bool:
        return self.db.delivered_equivalent(tg_user_id, item)

    def delivered_equivalent_persisted(self, tg_user_id: int, item: Dict[str, Any]) -> bool:
        return self.db.delivered_equivalent_persisted(tg_user_id, item)

    def recently_delivered(self, tg_user_id: int, item_id: int, cooldown_seconds: int) -> bool:
        return self.db.recently_delivered(tg_user_id, item_id, cooldown_seconds=cooldown_seconds)

    def recently_delivered_kinozal_id(self, tg_user_id: int, kinozal_id: Any, cooldown_seconds: int) -> bool:
        return self.db.recently_delivered_kinozal_id(
            tg_user_id,
            kinozal_id,
            cooldown_seconds=cooldown_seconds,
        )

    def upsert_debounce(
        self,
        tg_user_id: int,
        kinozal_id: Any,
        item_id: int,
        matched_sub_ids: str,
        delay_seconds: int,
    ) -> None:
        self.db.upsert_debounce(tg_user_id, kinozal_id, item_id, matched_sub_ids, delay_seconds=delay_seconds)

    def pop_due_pending_deliveries(self, current_hour: int) -> Dict[int, List[Dict[str, Any]]]:
        return self.db.pop_due_pending_deliveries(current_hour)

    def delete_pending_delivery(self, tg_user_id: int, item_id: int) -> None:
        self.db.delete_pending_delivery(tg_user_id, item_id)

    def pop_due_debounce(self) -> List[Dict[str, Any]]:
        return self.db.pop_due_debounce()

    def delete_debounce_entry(self, tg_user_id: int, kinozal_id: str) -> None:
        self.db.delete_debounce_entry(tg_user_id, kinozal_id)

    def get_user_quiet_hours(self, tg_user_id: int) -> Tuple[Optional[int], Optional[int]]:
        return self.db.get_user_quiet_hours(tg_user_id)

    def queue_pending_delivery(
        self,
        tg_user_id: int,
        item_id: int,
        matched_sub_ids: str,
        old_release_text: str,
        is_release_text_change: bool,
    ) -> None:
        self.db.queue_pending_delivery(
            tg_user_id,
            item_id,
            matched_sub_ids,
            old_release_text,
            is_release_text_change,
        )

    def record_delivery(
        self,
        tg_user_id: int,
        item_id: int,
        primary_sub_id: int,
        matched_sub_ids: List[int],
        delivery_audit: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.db.record_delivery(tg_user_id, item_id, primary_sub_id, matched_sub_ids, delivery_audit=delivery_audit)

    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item_id: int,
        primary_sub_id: int,
        matched_sub_ids: List[int],
        delivery_audit: Optional[Dict[str, Any]] = None,
        context: str = "",
    ) -> bool:
        return self.db.begin_delivery_claim(
            tg_user_id,
            item_id,
            primary_sub_id,
            matched_sub_ids,
            delivery_audit=delivery_audit,
            context=context,
        )

    def mark_delivery_claim_failed(self, tg_user_id: int, item_id: int, error: str = "") -> None:
        self.db.mark_delivery_claim_failed(tg_user_id, item_id, error=error)

    def get_latest_delivered_related_item(self, tg_user_id: int, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self.db.get_latest_delivered_related_item(tg_user_id, item)

    def queue_match_review(self, item_id: int, kinozal_id: str, reason: str = "") -> None:
        self.db.queue_match_review(item_id, kinozal_id, reason)

    def get_pending_match_review_by_item_id(self, item_id: int) -> Optional[Dict[str, Any]]:
        return self.db.get_pending_match_review_by_item_id(item_id)

    def mark_match_review_notified(self, item_id: int) -> None:
        self.db.mark_match_review_notified(item_id)
