import asyncio
import logging
from typing import Any, List, Optional, Sequence

from delivery_audit import build_delivery_audit
from delivery_sender import send_grouped_items_to_user, send_item_to_user
from domain import DeliveryCandidate, ReleaseItem, SubscriptionRecord
from release_versioning import describe_variant_change

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

    def record_delivery(self, tg_user_id: int, item: ReleaseItem, subs: Sequence[SubscriptionRecord], context: str = "worker") -> None:
        item_id = item.id
        primary_sub_id = subs[0].id if subs else 0
        matched_sub_ids = [sub.id for sub in subs]
        delivery_audit = build_delivery_audit(
            self.repository.db,
            item.to_dict(),
            [sub.to_dict() for sub in subs],
            context=context,
        )
        self.repository.record_delivery(
            tg_user_id,
            item_id,
            primary_sub_id,
            matched_sub_ids,
            delivery_audit=delivery_audit,
        )

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

        await self.send_item(
            tg_user_id,
            item,
            matched_subs,
            old_release_text=delivery.old_release_text,
        )
        self.record_delivery(tg_user_id, item, matched_subs)
        await asyncio.sleep(0.12)
