import asyncio
import logging
from typing import Any, List, Optional, Sequence

from delivery_events import build_delivery_event_key, build_grouped_event_key, resolve_delivery_event_type
from delivery_audit import build_delivery_audit
from delivery_sender import send_grouped_items_to_user, send_item_to_user
from domain import DeliveryCandidate, ReleaseItem, SubscriptionRecord
from release_versioning import describe_variant_change
from utils import compact_spaces

log = logging.getLogger(__name__)


class DeliveryService:
    def __init__(self, repository: Any, bot: Any) -> None:
        self.repository = repository
        self.bot = bot

    def get_latest_delivered_related_item(self, tg_user_id: int, item: ReleaseItem) -> Optional[ReleaseItem]:
        previous = self.repository.get_latest_delivered_related_item(tg_user_id, item.to_dict())
        return ReleaseItem.from_payload(previous) if previous else None

    async def send_item(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs: Optional[Sequence[SubscriptionRecord]],
        old_release_text: str = "",
    ) -> None:
        await send_item_to_user(
            self.repository.db,
            self.bot,
            tg_user_id,
            item.to_dict(),
            [sub.to_dict() for sub in subs] if subs else None,
            old_release_text=old_release_text,
        )

    async def send_grouped_items(
        self,
        tg_user_id: int,
        items: List[ReleaseItem],
        subs: Optional[Sequence[SubscriptionRecord]],
    ) -> None:
        await send_grouped_items_to_user(
            self.repository.db,
            self.bot,
            tg_user_id,
            [item.to_dict() for item in items],
            [sub.to_dict() for sub in subs] if subs else None,
        )

    def _build_delivery_audit(
        self,
        item: ReleaseItem,
        subs: Sequence[SubscriptionRecord],
        context: str,
        *,
        event_type: str,
        event_key: str,
        grouped_event_key: str = "",
    ) -> dict:
        audit = build_delivery_audit(
            self.repository.db,
            item.to_dict(),
            [sub.to_dict() for sub in subs],
            context=context,
        )
        audit["event_type"] = event_type
        audit["event_key"] = event_key
        if grouped_event_key:
            audit["grouped_event_key"] = grouped_event_key
        return audit

    def build_delivery_event(self, tg_user_id: int, item: ReleaseItem, *, context: str = "worker", old_release_text: str = "") -> tuple[str, str]:
        event_type = resolve_delivery_event_type(context, is_release_text_change=(context == "release_text_update"))
        event_key = build_delivery_event_key(
            tg_user_id,
            item,
            context=context,
            is_release_text_change=(event_type == "release_text"),
            release_text=old_release_text or item.get("source_release_text") or "",
        )
        return event_type, event_key

    def build_candidate_delivery_event(self, tg_user_id: int, delivery: DeliveryCandidate, *, context: str = "worker") -> tuple[str, str]:
        event_type = delivery.event_type or resolve_delivery_event_type(context, is_release_text_change=delivery.is_release_text_change)
        event_key = delivery.event_key or build_delivery_event_key(
            tg_user_id,
            delivery.item,
            context=context,
            is_release_text_change=delivery.is_release_text_change,
            release_text=delivery.old_release_text or delivery.item.get("source_release_text") or "",
        )
        return event_type, event_key

    def build_group_delivery_event(
        self,
        tg_user_id: int,
        items: Sequence[ReleaseItem],
        *,
        group_key: str = "",
    ) -> tuple[str, str]:
        return "grouped", build_grouped_event_key(tg_user_id, items, group_key=group_key)

    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs: Sequence[SubscriptionRecord],
        context: str = "worker",
        *,
        event_type: str = "",
        event_key: str = "",
        old_release_text: str = "",
        grouped_event_key: str = "",
    ) -> bool:
        item_id = item.id
        primary_sub_id = subs[0].id if subs else 0
        matched_sub_ids = [sub.id for sub in subs]
        resolved_event_type = compact_spaces(str(event_type or ""))
        resolved_event_key = compact_spaces(str(event_key or ""))
        if not resolved_event_type or not resolved_event_key:
            computed_type, computed_key = self.build_delivery_event(
                tg_user_id,
                item,
                context=context,
                old_release_text=old_release_text,
            )
            resolved_event_type = resolved_event_type or computed_type
            resolved_event_key = resolved_event_key or computed_key
        delivery_audit = self._build_delivery_audit(
            item,
            subs,
            context,
            event_type=resolved_event_type,
            event_key=resolved_event_key,
            grouped_event_key=grouped_event_key,
        )
        return self.repository.begin_delivery_claim(
            tg_user_id,
            item_id,
            primary_sub_id,
            matched_sub_ids,
            delivery_audit=delivery_audit,
            context=context,
            event_type=resolved_event_type,
            event_key=resolved_event_key,
        )

    def mark_delivery_claim_failed(self, tg_user_id: int, item: ReleaseItem, error: str = "", *, event_key: str = "") -> None:
        self.repository.mark_delivery_claim_failed(tg_user_id, item.id, error=error, event_key=event_key)

    def record_delivery(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs: Sequence[SubscriptionRecord],
        context: str = "worker",
        *,
        event_type: str = "",
        event_key: str = "",
        old_release_text: str = "",
        grouped_event_key: str = "",
    ) -> None:
        item_id = item.id
        primary_sub_id = subs[0].id if subs else 0
        matched_sub_ids = [sub.id for sub in subs]
        resolved_event_type = compact_spaces(str(event_type or ""))
        resolved_event_key = compact_spaces(str(event_key or ""))
        if not resolved_event_type or not resolved_event_key:
            computed_type, computed_key = self.build_delivery_event(
                tg_user_id,
                item,
                context=context,
                old_release_text=old_release_text,
            )
            resolved_event_type = resolved_event_type or computed_type
            resolved_event_key = resolved_event_key or computed_key
        delivery_audit = self._build_delivery_audit(
            item,
            subs,
            context,
            event_type=resolved_event_type,
            event_key=resolved_event_key,
            grouped_event_key=grouped_event_key,
        )
        self.repository.record_delivery(
            tg_user_id,
            item_id,
            primary_sub_id,
            matched_sub_ids,
            delivery_audit=delivery_audit,
            event_type=resolved_event_type,
            event_key=resolved_event_key,
        )

    async def deliver_claimed_item(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs: Sequence[SubscriptionRecord],
        *,
        context: str = "worker",
        old_release_text: str = "",
        event_type: str = "",
        event_key: str = "",
        grouped_event_key: str = "",
    ) -> None:
        try:
            await self.send_item(
                tg_user_id,
                item,
                subs,
                old_release_text=old_release_text,
            )
            self.record_delivery(
                tg_user_id,
                item,
                subs,
                context=context,
                event_type=event_type,
                event_key=event_key,
                old_release_text=old_release_text,
                grouped_event_key=grouped_event_key,
            )
            await asyncio.sleep(0.12)
        except Exception as exc:
            self.mark_delivery_claim_failed(tg_user_id, item, error=str(exc), event_key=event_key)
            raise

    async def send_single(self, tg_user_id: int, delivery: DeliveryCandidate) -> None:
        item = delivery.item
        item_id = delivery.item_id
        matched_subs = delivery.subs
        previous_item = self.get_latest_delivered_related_item(tg_user_id, item)
        if previous_item:
            item.set("previous_related_item_id", previous_item.id)
            item.set("previous_progress", previous_item.get("source_episode_progress") or "")
            item.set("previous_source_title", previous_item.get("source_title") or "")
            item.set("previous_source_format", previous_item.get("source_format") or "")
            log.info(
                "Delivering updated release item=%s to user=%s source_uid=%s reason=%s prev_item_id=%s",
                item_id,
                tg_user_id,
                item.source_uid,
                describe_variant_change(previous_item.to_dict(), item.to_dict()),
                previous_item.id,
            )
        else:
            item.set("previous_related_item_id", None)
            item.set("previous_progress", "")
            item.set("previous_source_title", "")
            item.set("previous_source_format", "")
            log.info(
                "Delivering new release item=%s to user=%s source_uid=%s",
                item_id,
                tg_user_id,
                item.source_uid,
            )

        claim_context = delivery.delivery_context or "worker"
        event_type, event_key = self.build_candidate_delivery_event(tg_user_id, delivery, context=claim_context)
        if not self.begin_delivery_claim(
            tg_user_id,
            item,
            matched_subs,
            context=claim_context,
            event_type=event_type,
            event_key=event_key,
            old_release_text=delivery.old_release_text,
        ):
            return
        await self.deliver_claimed_item(
            tg_user_id,
            item,
            matched_subs,
            context=claim_context,
            old_release_text=delivery.old_release_text,
            event_type=event_type,
            event_key=event_key,
        )
