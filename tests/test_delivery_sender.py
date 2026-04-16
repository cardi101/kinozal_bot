import asyncio
from types import SimpleNamespace

import delivery_sender as delivery_sender_module
from aiogram.exceptions import TelegramNetworkError
from delivery_sender import _tg_retry, send_item_to_user


class _FakeBot:
    def __init__(self) -> None:
        self.photos = []
        self.messages = []

    async def send_photo(self, tg_user_id, photo=None, caption=None, parse_mode=None, reply_markup=None):
        self.photos.append((tg_user_id, photo, caption))
        return SimpleNamespace(photo=[SimpleNamespace(file_id="new-file-id", file_unique_id="uniq-1")])

    async def send_message(self, tg_user_id, text=None, **kwargs):
        self.messages.append((tg_user_id, text))
        return SimpleNamespace()


class _FakeCacheDB:
    def __init__(self, cached_file_id: str = "") -> None:
        self.cached_file_id = cached_file_id
        self.saved = []

    def get_telegram_file_cache(self, cache_key: str):
        return self.cached_file_id or None

    def set_telegram_file_cache(self, cache_key: str, file_id: str, file_unique_id: str = "") -> None:
        self.saved.append((cache_key, file_id, file_unique_id))


def test_tg_retry_retries_network_errors() -> None:
    calls = {"count": 0}

    async def _flaky():
        calls["count"] += 1
        if calls["count"] < 3:
            raise TelegramNetworkError(method=None, message="temporary")
        return "ok"

    result = asyncio.run(_tg_retry(_flaky))

    assert result == "ok"
    assert calls["count"] == 3


def test_send_item_to_user_prefers_cached_poster_file_id(monkeypatch) -> None:
    db = _FakeCacheDB(cached_file_id="cached-file-id")
    bot = _FakeBot()
    item = {"id": 42, "tmdb_poster_url": "https://example.com/poster.jpg", "source_title": "Title"}

    async def _fake_enrich(payload: dict, force_refresh: bool = False):
        return payload

    monkeypatch.setattr(delivery_sender_module, "enrich_kinozal_item_with_details", _fake_enrich)
    monkeypatch.setattr(delivery_sender_module, "item_message", lambda db, item, subs, old_release_text="": "Hello")
    monkeypatch.setattr(delivery_sender_module, "mute_title_kb", lambda tmdb_id: None)
    monkeypatch.setattr(delivery_sender_module, "_build_poster_file", lambda poster_url, item_id: (_ for _ in ()).throw(AssertionError("upload path should not be used")))

    asyncio.run(send_item_to_user(db, bot, 1001, item, subs=None))

    assert bot.photos == [(1001, "cached-file-id", "Hello")]
    assert db.saved == []


def test_send_item_to_user_caches_uploaded_poster_file_id(monkeypatch) -> None:
    db = _FakeCacheDB()
    bot = _FakeBot()
    item = {"id": 42, "tmdb_poster_url": "https://example.com/poster.jpg", "source_title": "Title"}

    async def _fake_enrich(payload: dict, force_refresh: bool = False):
        return payload

    async def _fake_build_poster_file(poster_url: str, item_id: int):
        return object()

    monkeypatch.setattr(delivery_sender_module, "enrich_kinozal_item_with_details", _fake_enrich)
    monkeypatch.setattr(delivery_sender_module, "item_message", lambda db, item, subs, old_release_text="": "Hello")
    monkeypatch.setattr(delivery_sender_module, "mute_title_kb", lambda tmdb_id: None)
    monkeypatch.setattr(delivery_sender_module, "_build_poster_file", _fake_build_poster_file)

    asyncio.run(send_item_to_user(db, bot, 1001, item, subs=None))

    assert bot.photos
    assert db.saved == [("poster:https://example.com/poster.jpg", "new-file-id", "uniq-1")]
