import asyncio

from match_debug_helpers import _strip_existing_match_fields
from match_debug_helpers import rematch_item_live


def test_strip_existing_match_fields_marks_explicit_match_clear() -> None:
    cleaned = _strip_existing_match_fields(
        {
            "id": 42,
            "source_title": "Sample",
            "media_type": "tv",
            "tmdb_id": 123,
            "tmdb_title": "Stored",
            "tmdb_match_path": "search",
            "tmdb_match_confidence": "high",
            "imdb_id": "tt1234567",
        }
    )

    assert cleaned["source_title"] == "Sample"
    assert cleaned["_clear_tmdb_match"] is True
    assert "tmdb_id" not in cleaned
    assert "imdb_id" not in cleaned
    assert "media_type" not in cleaned


class _FakeDB:
    def __init__(self) -> None:
        self.deleted_override = None
        self.saved_payload = None

    def delete_match_override(self, kinozal_id: str) -> None:
        self.deleted_override = kinozal_id

    def save_item(self, payload: dict) -> None:
        self.saved_payload = dict(payload)

    def get_item(self, item_id: int):
        return {"id": item_id, "kinozal_id": "2128422", "tmdb_match_path": "search"}

    def find_item_by_kinozal_id(self, kinozal_id: str):
        return {"id": 42, "kinozal_id": kinozal_id, "tmdb_match_path": "search"}


class _FakeTMDB:
    def __init__(self) -> None:
        self.payload = None

    async def enrich_item(self, payload: dict):
        self.payload = dict(payload)
        enriched = dict(payload)
        enriched["tmdb_id"] = 777
        enriched["tmdb_match_path"] = "search"
        return enriched


def test_rematch_item_live_bypasses_and_clears_stored_override() -> None:
    db = _FakeDB()
    tmdb = _FakeTMDB()
    item = {
        "id": 42,
        "kinozal_id": "2128422",
        "source_uid": "kinozal:2128422",
        "source_title": "Sample",
        "tmdb_id": 123,
        "tmdb_match_path": "stored_override",
    }

    before, after, ok = asyncio.run(rematch_item_live(db, tmdb, item))

    assert ok is True
    assert before["tmdb_id"] == 123
    assert tmdb.payload["_skip_kinozal_override"] is True
    assert db.deleted_override == "2128422"
    assert db.saved_payload["tmdb_id"] == 777
    assert after["tmdb_match_path"] == "search"
