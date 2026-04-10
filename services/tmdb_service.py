from typing import Any, Dict


class TMDBService:
    def __init__(self, client: Any) -> None:
        self.client = client

    async def enrich_item(self, item: Dict[str, Any]) -> Dict[str, Any]:
        return await self.client.enrich_item(item)

    async def ensure_genres(self, force: bool = False) -> None:
        await self.client.ensure_genres(force=force)

    async def close(self) -> None:
        await self.client.close()
