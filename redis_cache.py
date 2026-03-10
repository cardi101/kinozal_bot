import json
import logging
from typing import Any, Dict, Optional

import redis.asyncio as redis


log = logging.getLogger(__name__)


class RedisCache:
    def __init__(self, url: str):
        self.url = url
        self.client = redis.from_url(url, decode_responses=True) if url else None

    async def get_json(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.client:
            return None
        try:
            raw = await self.client.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            log.warning("Redis get failed for key=%s", key, exc_info=True)
            return None

    async def set_json(self, key: str, value: Dict[str, Any], ex: int) -> None:
        if not self.client:
            return
        try:
            await self.client.set(key, json.dumps(value, ensure_ascii=False), ex=ex)
        except Exception:
            log.warning("Redis set failed for key=%s", key, exc_info=True)

    async def close(self) -> None:
        if self.client:
            await self.client.aclose()
