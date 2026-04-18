import asyncio
import logging
from types import SimpleNamespace

from delivery_formatting import item_message
import tmdb_client as tmdb_client_module
from tmdb_client import TMDBClient, _should_expand_lexicon_alias


class _DummyDb:
    def get_all_genres_merged(self):
        return {}


def test_should_expand_lexicon_alias_skips_weak_semantic_translation() -> None:
    lexicon_best = SimpleNamespace(canonical_title="Tadaima, Ojamasaremasu!")
    item = {
        "source_title": "Я вернулась! Не помешаю? (1-2 серии из 12) / Tadaima, Ojama Saremasu! / 2026 / ЛМ (Dream Cast, DreamyVoice), СТ / HEVC / WEBRip (1080p)",
        "cleaned_title": "Я вернулась Не помешаю / Tadaima Ojama Saremasu",
    }

    assert _should_expand_lexicon_alias(item, item["cleaned_title"], lexicon_best, "Tadaima, Ojama Saremasu!") is True
    assert _should_expand_lexicon_alias(item, item["cleaned_title"], lexicon_best, "ただいま、おじゃまされます！") is True
    assert _should_expand_lexicon_alias(item, item["cleaned_title"], lexicon_best, "I’m Home!") is False


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


def test_stored_override_not_superseded_by_manual_override(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: {"tmdb_id": 123, "media_type": "movie", "source": "admin"})
    client.cache = None
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en"

    async def _fake_get_details(media_type: str, tmdb_id: int):
        return None

    client.get_details = _fake_get_details
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False
    monkeypatch.setattr(tmdb_client_module, "manual_tmdb_override_for_item", lambda item: ("movie", 999, "manual-key"))

    item = {
        "kinozal_id": "2128422",
        "source_uid": "kinozal:2128422",
        "source_title": "Sample Movie / 2026 / WEB-DL (1080p)",
        "source_description": "",
        "media_type": "movie",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result.get("tmdb_id") is None
    assert result.get("tmdb_match_path") is None


def test_enrich_item_prefers_valid_modern_matlock_candidate(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    calls: list[tuple[str, str, int | None]] = []

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        calls.append((query, media_type, year))
        if query == "Matlock" and media_type == "tv" and year is None:
            return [
                {
                    "tmdb_id": 100,
                    "media_type": "tv",
                    "tmdb_title": "Matlock",
                    "tmdb_original_title": "Matlock",
                    "search_match_title": "Matlock",
                    "search_match_original_title": "Matlock",
                    "tmdb_release_date": "1986-03-03",
                    "tmdb_number_of_seasons": 9,
                    "tmdb_number_of_episodes": 193,
                    "tmdb_rating": 7.0,
                    "tmdb_vote_count": 500,
                    "tmdb_status": "Ended",
                },
                {
                    "tmdb_id": 200,
                    "media_type": "tv",
                    "tmdb_title": "Matlock",
                    "tmdb_original_title": "Matlock",
                    "search_match_title": "Matlock",
                    "search_match_original_title": "Matlock",
                    "tmdb_release_date": "2024-09-22",
                    "tmdb_number_of_seasons": 1,
                    "tmdb_number_of_episodes": 18,
                    "tmdb_rating": 7.4,
                    "tmdb_vote_count": 4200,
                    "tmdb_status": "Returning Series",
                },
            ]
        return []

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(tmdb_client_module, "title_search_candidates", lambda source_title, cleaned_title: ["Matlock"])
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])

    item = {
        "kinozal_id": "3003",
        "source_uid": "kinozal:3003",
        "source_title": "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p)",
        "source_description": "",
        "media_type": "tv",
        "source_year": 2025,
        "source_episode_progress": "2 сезон: 1-13 серии из 16",
        "source_format": "1080",
        "source_audio_tracks": ["ПМ (TVShows)"],
    }

    result = asyncio.run(client.enrich_item(item))
    text = item_message(_DummyDb(), result, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert calls[0] == ("Matlock", "tv", None)
    assert result["tmdb_id"] == 200
    assert result["tmdb_title"] == "Matlock"
    assert result["tmdb_rating"] == 7.4
    assert "themoviedb.org/tv/200" in text


def test_enrich_item_keeps_query_variants_for_same_tmdb_candidate(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if media_type != "tv":
            return []
        return [
            {
                "tmdb_id": 333,
                "media_type": "tv",
                "tmdb_title": "Exact Show",
                "tmdb_original_title": "Exact Show",
                "search_match_title": "Exact Show",
                "search_match_original_title": "Exact Show",
                "tmdb_release_date": "2025-01-10",
                "tmdb_number_of_seasons": 1,
                "tmdb_number_of_episodes": 12,
                "tmdb_rating": 7.3,
                "tmdb_vote_count": 111,
                "tmdb_status": "Returning Series",
            }
        ]

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(tmdb_client_module, "title_search_candidates", lambda source_title, cleaned_title: ["Foo", "Exact Show"])
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])
    monkeypatch.setattr(
        tmdb_client_module,
        "tmdb_match_looks_valid",
        lambda item, query, details, requested_media_type: query == "Exact Show",
    )

    item = {
        "kinozal_id": "3010",
        "source_uid": "kinozal:3010",
        "source_title": "Exact Show / 2025 / WEB-DL (1080p)",
        "source_description": "",
        "media_type": "tv",
        "source_year": 2025,
        "source_format": "1080",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 333
    assert result["tmdb_match_path"] == "search"
    assert "\"candidate_rejected\"" in result["tmdb_match_debug"]
    assert "\"query\": \"Exact Show\"" in result["tmdb_match_debug"]


def test_enrich_item_rejects_short_prefix_movie_match_and_keeps_full_title_candidate(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if media_type != "movie":
            return []
        if query == "Good Luck":
            return [
                {
                    "tmdb_id": 1459584,
                    "media_type": "movie",
                    "tmdb_title": "Good Luck",
                    "tmdb_original_title": "グッドラック",
                    "search_match_title": "Good Luck",
                    "search_match_original_title": "グッドラック",
                    "tmdb_release_date": "2025-01-01",
                    "tmdb_rating": 6.5,
                    "tmdb_vote_count": 50,
                    "search_score": 3.498,
                }
            ]
        if query == "Удачи веселья не сдохни":
            return [
                {
                    "tmdb_id": 1119449,
                    "media_type": "movie",
                    "tmdb_title": "Удачи, веселья, не сдохни",
                    "tmdb_original_title": "Good Luck, Have Fun, Don't Die",
                    "search_match_title": "Удачи, веселья, не сдохни",
                    "search_match_original_title": "Good Luck, Have Fun, Don't Die",
                    "tmdb_release_date": "2024-09-12",
                    "tmdb_rating": 7.1,
                    "tmdb_vote_count": 200,
                    "search_score": 1.48,
                }
            ]
        return []

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(
        tmdb_client_module,
        "title_search_candidates",
        lambda source_title, cleaned_title: ["Good Luck", "Удачи веселья не сдохни"],
    )
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])

    item = {
        "kinozal_id": "2136344",
        "source_uid": "kinozal:2136344",
        "source_title": "Удачи, веселья, не сдохни / Good Luck, Have Fun, Don't Die / 2025 / ДБ, 2 x ПМ, АП (Яроцкий), ЛМ, СТ / Blu-Ray Remux (1080p)",
        "source_description": "",
        "media_type": "movie",
        "source_year": 2025,
        "source_category_id": "13",
        "source_category_name": "Кино - Фантастика",
        "source_format": "1080",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 1119449
    assert result["tmdb_title"] == "Удачи, веселья, не сдохни"
    assert result["tmdb_match_path"] == "search"
    assert "TRUNCATED_PREFIX_QUERY_MATCH" in result["tmdb_match_debug"]


def test_enrich_item_allows_short_numeric_movie_title_candidate(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if query == "180" and media_type == "movie":
            return [
                {
                    "tmdb_id": 1800,
                    "media_type": "movie",
                    "tmdb_title": "180",
                    "tmdb_original_title": "180",
                    "search_match_title": "180",
                    "search_match_original_title": "180",
                    "tmdb_release_date": "2026-01-01",
                    "tmdb_rating": 6.9,
                    "tmdb_vote_count": 120,
                    "search_score": 2.1,
                }
            ]
        return []

    client.search_ranked = _fake_search_ranked

    item = {
        "kinozal_id": "2136349",
        "source_uid": "kinozal:2136349",
        "source_title": "180 / 180 / 2026 / СТ / WEBRip (1080p)",
        "source_description": "",
        "media_type": "movie",
        "source_year": 2026,
        "source_category_id": "17",
        "source_category_name": "Кино - Драма",
        "source_format": "1080",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 1800
    assert result["tmdb_title"] == "180"
    assert result["tmdb_match_path"] == "search"
    assert "\"query\": \"180\"" in result["tmdb_match_debug"]


def test_enrich_item_keeps_on_air_anime_match_with_soft_episode_warning(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if query == "Tadaima, Ojama Saremasu!" and media_type == "tv":
            return [
                {
                    "tmdb_id": 7777,
                    "media_type": "tv",
                    "tmdb_title": "Tadaima, Ojama Saremasu!",
                    "tmdb_original_title": "ただいま、おじゃまされます！",
                    "search_match_title": "Tadaima, Ojama Saremasu!",
                    "search_match_original_title": "ただいま、おじゃまされます！",
                    "tmdb_release_date": "2026-01-09",
                    "tmdb_number_of_seasons": 1,
                    "tmdb_number_of_episodes": 24,
                    "tmdb_rating": 7.9,
                    "tmdb_vote_count": 88,
                    "tmdb_status": "Returning Series",
                }
            ]
        return []

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(
        tmdb_client_module,
        "title_search_candidates",
        lambda source_title, cleaned_title: ["Tadaima, Ojama Saremasu!"],
    )

    item = {
        "kinozal_id": "3004",
        "source_uid": "kinozal:3004",
        "source_title": "Я вернулась! Не помешаю? (1-2 серии из 12) / Tadaima, Ojama Saremasu! / 2026 / ЛМ (Dream Cast, DreamyVoice), СТ / HEVC / WEBRip (1080p)",
        "source_description": "",
        "media_type": "tv",
        "source_year": 2026,
        "source_episode_progress": "1-2 серии из 12",
        "source_format": "1080",
        "source_audio_tracks": ["ЛМ (Dream Cast, DreamyVoice)", "СТ"],
        "source_category_name": "Аниме",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 7777
    assert result["tmdb_match_confidence"] in {"medium", "high"}
    assert "warnings=episode_total_mismatch_soft" in result["tmdb_match_evidence"]
    assert "search_unmatched" not in str(result.get("tmdb_match_path") or "")


def test_enrich_item_keeps_anime_parent_series_for_later_season_with_large_year_delta(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if media_type != "tv":
            return []
        if query != "Re: Zero":
            return []
        return [
            {
                "tmdb_id": 65942,
                "media_type": "tv",
                "tmdb_title": "Re: Жизнь в другом мире с нуля",
                "tmdb_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
                "search_match_title": "Re: Жизнь в другом мире с нуля",
                "search_match_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
                "tmdb_release_date": "2016-04-04",
                "tmdb_number_of_seasons": 4,
                "tmdb_number_of_episodes": 66,
                "tmdb_rating": 7.8,
                "tmdb_vote_count": 1200,
                "tmdb_status": "Returning Series",
                "search_score": 1.431,
            }
        ]

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(
        tmdb_client_module,
        "title_search_candidates",
        lambda source_title, cleaned_title: ["Re: Zero", "Жизнь в альтернативном мире с нуля"],
    )
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])

    item = {
        "kinozal_id": "2135205",
        "source_uid": "kinozal:2135205",
        "source_title": "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)",
        "cleaned_title": "Жизнь в альтернативном мире с нуля / Re: Zero",
        "source_description": "",
        "media_type": "tv",
        "source_year": 2026,
        "source_episode_progress": "4 сезон: 1-2 серии из 18",
        "source_category_name": "Мульт - Аниме",
        "source_category_id": "20",
        "source_format": "1080",
        "bucket": "anime",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 65942
    assert result["tmdb_title"] == "Re: Жизнь в другом мире с нуля"
    assert result["tmdb_match_path"] == "search"
    assert "search_unmatched" not in str(result.get("tmdb_match_path") or "")


def test_enrich_item_keeps_long_running_single_season_anime_parent_series(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if media_type != "tv":
            return []
        if query not in {"Re: Zero", "Жизнь в альтернативном мире с нуля"}:
            return []
        return [
            {
                "tmdb_id": 65942,
                "media_type": "tv",
                "tmdb_title": "Re: Жизнь в другом мире с нуля",
                "tmdb_original_title": "Re:ゼロから始める異世界生活",
                "search_match_title": "Re:ZERO -Starting Life in Another World-",
                "search_match_original_title": "Re:ゼロから始める異世界生活",
                "tmdb_release_date": "2016-04-04",
                "tmdb_number_of_seasons": 1,
                "tmdb_number_of_episodes": 85,
                "tmdb_rating": 7.8,
                "tmdb_vote_count": 598,
                "tmdb_status": "Returning Series",
                "search_score": 0.66,
            }
        ]

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(
        tmdb_client_module,
        "title_search_candidates",
        lambda source_title, cleaned_title: ["Re: Zero", "Жизнь в альтернативном мире с нуля"],
    )
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])

    item = {
        "kinozal_id": "2135205",
        "source_uid": "kinozal:2135205",
        "source_title": "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)",
        "cleaned_title": "Жизнь в альтернативном мире с нуля / Re: Zero",
        "source_description": "",
        "media_type": "tv",
        "source_year": 2026,
        "source_episode_progress": "4 сезон: 1-2 серии из 18",
        "source_category_name": "Мульт - Аниме",
        "source_category_id": "20",
        "source_format": "1080",
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 65942
    assert result["tmdb_title"] == "Re: Жизнь в другом мире с нуля"
    assert result["tmdb_match_path"] == "search"


def test_enrich_item_records_reject_reason_in_debug_when_unmatched(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        return [
            {
                "tmdb_id": 900,
                "media_type": "tv",
                "tmdb_title": "Matlock",
                "tmdb_original_title": "Matlock",
                "search_match_title": "Matlock",
                "search_match_original_title": "Matlock",
                "tmdb_release_date": "1986-03-03",
                "tmdb_number_of_seasons": 9,
                "tmdb_number_of_episodes": 193,
                "tmdb_rating": 7.0,
                "tmdb_vote_count": 500,
                "tmdb_status": "Ended",
            }
        ]

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(tmdb_client_module, "title_search_candidates", lambda source_title, cleaned_title: ["Matlock"])
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])

    item = {
        "kinozal_id": "3005",
        "source_uid": "kinozal:3005",
        "source_title": "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p)",
        "source_description": "",
        "media_type": "tv",
        "source_year": 2025,
        "source_episode_progress": "2 сезон: 1-13 серии из 16",
        "source_format": "1080",
        "source_audio_tracks": ["ПМ (TVShows)"],
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_match_path"] == "search_unmatched"
    assert result["tmdb_match_confidence"] == "unmatched"
    assert "tmdb_match_debug" in result and "candidate_rejected" in result["tmdb_match_debug"]
    assert "\"reason_code\": \"TV_YEAR_DELTA_EXTREME\"" in result["tmdb_match_debug"]
    assert "candidate_ranking" in result["tmdb_match_debug"]
    assert "\"features\"" in result["tmdb_match_debug"]


def test_enrich_item_keeps_query_sensitive_candidate_until_validation(monkeypatch) -> None:
    client = object.__new__(TMDBClient)
    client.anime_title_lexicon = None
    client.anime_mapping_store = None
    client.cfg = SimpleNamespace(anime_resolver_enabled=False, anime_resolver_log_only=False)
    client.db = SimpleNamespace(get_match_override=lambda kinozal_id: None)
    client.cache = SimpleNamespace(client=None)
    client.log = logging.getLogger("test-tmdb-client")
    client.token = "token"
    client.language = "en-US"
    client.find_by_imdb = None
    client._is_rejected_match = lambda item, details: False

    async def _fake_search_ranked(query: str, media_type: str, year: int | None, limit: int = 5):
        if media_type != "movie":
            return []
        return [
            {
                "tmdb_id": 555,
                "media_type": "movie",
                "tmdb_title": "Good Title",
                "tmdb_original_title": "Good Title",
                "search_match_title": "Good Title",
                "search_match_original_title": "Good Title",
                "tmdb_release_date": "2025-01-01",
                "tmdb_rating": 7.1,
                "tmdb_vote_count": 99,
                "tmdb_status": "Released",
                "search_score": 1.0,
            }
        ]

    client.search_ranked = _fake_search_ranked
    monkeypatch.setattr(
        tmdb_client_module,
        "title_search_candidates",
        lambda source_title, cleaned_title: ["Bad Alias", "Good Title"],
    )
    monkeypatch.setattr(tmdb_client_module, "_extract_slash_title_candidates", lambda source_title: [])
    monkeypatch.setattr(
        tmdb_client_module,
        "extract_tmdb_match_features",
        lambda item, query, details, requested_media_type: SimpleNamespace(
            query_used=query,
            to_dict=lambda: {"query_used": query},
        ),
    )
    monkeypatch.setattr(
        tmdb_client_module,
        "score_tmdb_match_candidate",
        lambda features: 9.0 if features.query_used == "Bad Alias" else 1.0,
    )
    monkeypatch.setattr(
        tmdb_client_module,
        "tmdb_match_looks_valid",
        lambda item, query, details, requested_media_type: query == "Good Title",
    )

    item = {
        "kinozal_id": "3006",
        "source_uid": "kinozal:3006",
        "source_title": "Good Title / 2025 / WEB-DL (1080p)",
        "source_description": "",
        "media_type": "movie",
        "source_year": 2025,
    }

    result = asyncio.run(client.enrich_item(item))

    assert result["tmdb_id"] == 555
    assert result["tmdb_match_path"] == "search"
    assert "query=Good Title" in result["tmdb_match_evidence"]
    assert "search_unmatched" not in str(result.get("tmdb_match_path") or "")
