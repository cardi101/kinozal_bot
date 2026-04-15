import asyncio
import logging
from types import SimpleNamespace

from tmdb_client import TMDBClient


def test_stored_override_none_details_blocks_fallback_paths() -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: {"tmdb_id": 123, "media_type": "movie", "source": "admin"})
    client.cache = None
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en"

    calls = {"imdb": 0}

    async def _fake_get_details(media_type: str, tmdb_id: int):
        return None

    async def _fake_find_by_imdb(imdb_id: str):
        calls["imdb"] += 1
        return {"tmdb_id": 999, "media_type": "movie"}

    client.get_details = _fake_get_details
    client.find_by_imdb = _fake_find_by_imdb
    client._is_rejected_match = lambda item, details: False

    item = {
        "kinozal_id": "2128422",
        "source_uid": "kinozal:2128422",
        "source_title": "Sample Movie / 2026 / WEB-DL (1080p)",
        "source_description": "",
        "media_type": "movie",
        "source_imdb_id": "tt1234567",
    }

    result = asyncio.run(client.enrich_item(item))

    assert calls["imdb"] == 0
    assert result.get("tmdb_id") is None
    assert result.get("tmdb_match_path") is None
