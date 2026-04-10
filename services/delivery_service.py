import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence

from delivery_sender import send_grouped_items_to_user, send_item_to_user
from release_versioning import describe_variant_change

log = logging.getLogger(__name__)


class DeliveryService:
    def __init__(self, repository: Any, bot: Any) -> None:
        self.repository = repository
        self.bot = bot

    def get_latest_delivered_related_item(self, tg_user_id: int, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return self.repository.get_latest_delivered_related_item(tg_user_id, item)

    async def send_item(
        self,
        tg_user_id: int,
        item: Dict[str, Any],
        subs: Optional[Sequence[Dict[str, Any]]],
        old_release_text: str = "",
    ) -> None:
        await send_item_to_user(self.repository.db, self.bot, tg_user_id, item, subs, old_release_text=old_release_text)

    async def send_grouped_items(
        self,
        tg_user_id: int,
        items: List[Dict[str, Any]],
        subs: Optional[Sequence[Dict[str, Any]]],
    ) -> None:
        await send_grouped_items_to_user(self.repository.db, self.bot, tg_user_id, items, subs)

    def record_delivery(self, tg_user_id: int, item_id: int, subs: Sequence[Dict[str, Any]]) -> None:
        primary_sub_id = int(subs[0]["id"]) if subs else 0
        matched_sub_ids = [int(sub["id"]) for sub in subs]
        self.repository.record_delivery(tg_user_id, item_id, primary_sub_id, matched_sub_ids)

    async def send_single(self, tg_user_id: int, delivery: Dict[str, Any]) -> None:
        item = delivery["item"]
        item_id = delivery["item_id"]
        matched_subs = delivery["subs"]
        previous_item = self.get_latest_delivered_related_item(tg_user_id, item)
        if previous_item:
            log.info(
                "Delivering updated release item=%s to user=%s source_uid=%s reason=%s prev_item_id=%s",
                item_id,
                tg_user_id,
                item.get("source_uid"),
                describe_variant_change(previous_item, item),
                previous_item.get("id"),
            )
        else:
            log.info(
                "Delivering new release item=%s to user=%s source_uid=%s",
                item_id,
                tg_user_id,
                item.get("source_uid"),
            )

        await self.send_item(
            tg_user_id,
            item,
            matched_subs,
            old_release_text=str(delivery.get("old_release_text") or ""),
        )
        self.record_delivery(tg_user_id, item_id, matched_subs)
        await asyncio.sleep(0.12)
