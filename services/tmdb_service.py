from typing import Any

from domain import ReleaseItem

class TMDBService:
    def __init__(self, client: Any) -> None:
        self.client = client

    async def enrich_item(self, item: ReleaseItem) -> ReleaseItem:
        enriched = await self.client.enrich_item(item.to_dict())
        return ReleaseItem.from_payload(enriched)

    async def ensure_genres(self, force: bool = False) -> None:
        await self.client.ensure_genres(force=force)

    async def close(self) -> None:
        await self.client.close()
