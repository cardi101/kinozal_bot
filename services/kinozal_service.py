from typing import Any, Dict, List

from kinozal_details import enrich_kinozal_item_with_details


class KinozalService:
    def __init__(self, source: Any) -> None:
        self.source = source

    async def fetch_latest(self) -> List[Dict[str, Any]]:
        return await self.source.fetch_latest()

    async def enrich_item_with_details(
        self,
        item: Dict[str, Any],
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        return await enrich_kinozal_item_with_details(item, force_refresh=force_refresh)

    async def close(self) -> None:
        await self.source.close()
