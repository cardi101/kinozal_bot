import os
import asyncio

os.environ.setdefault("BOT_TOKEN", "123456:TESTTOKEN")
os.environ.setdefault("DATABASE_URL", "postgresql://example/example")

from domain import ReleaseItem
from services.admin_api_service import AdminApiService


class _FakeDB:
    def __init__(self) -> None:
        self.saved_payload = None

    def find_item_by_kinozal_id(self, kinozal_id: str):
        return {
            "id": 42,
            "kinozal_id": kinozal_id,
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": "Before title",
            "source_release_text": "old text",
            "source_episode_progress": "1 сезон: 1-7 серии из 10",
            "media_type": "tv",
        }

    def save_item(self, payload: dict):
        self.saved_payload = dict(payload)
        return 99, True, True

    def get_item(self, item_id: int):
        return {
            "id": item_id,
            "kinozal_id": "2128422",
            "source_uid": "kinozal:2128422",
            "source_title": "After title",
            "source_release_text": "new text",
            "source_episode_progress": "1 сезон: 1-10 серии из 10",
            "media_type": "tv",
        }


class _FakeKinozalService:
    async def enrich_item_with_details(self, item: ReleaseItem, force_refresh: bool = False) -> ReleaseItem:
        item.set("source_title", "After title")
        item.set("details_title", "After title")
        item.set("source_release_text", "new text")
        item.set("source_episode_progress", "1 сезон: 1-10 серии из 10")
        return item


def test_reparse_release_persists_refreshed_item() -> None:
    db = _FakeDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=_FakeKinozalService(),
        bot=None,
    )

    result = asyncio.run(service.reparse_release("2128422"))

    assert db.saved_payload is not None
    assert db.saved_payload["source_title"] == "After title"
    assert db.saved_payload["source_episode_progress"] == "1 сезон: 1-10 серии из 10"
    assert result["item_id"] == 99
    assert result["item"]["source_title"] == "After title"
