from typing import Any, List

from domain import ReleaseItem
from kinozal_details import enrich_kinozal_item_with_details


class KinozalService:
    def __init__(self, source: Any) -> None:
        self.source = source

    async def fetch_latest(self) -> List[ReleaseItem]:
        return [ReleaseItem.from_payload(item) for item in await self.source.fetch_latest()]

    async def enrich_item_with_details(
        self,
        item: ReleaseItem,
        force_refresh: bool = False,
    ) -> ReleaseItem:
        enriched = await enrich_kinozal_item_with_details(item.to_dict(), force_refresh=force_refresh)
        return ReleaseItem.from_payload(enriched)

    async def close(self) -> None:
        await self.source.close()
