import asyncio

import kinozal_details as kinozal_details_module


def test_enrich_kinozal_item_with_details_backfills_missing_release_fields_from_details_title(monkeypatch) -> None:
    details_html = """
    <html>
      <head><title>Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p) :: Кинозал.ТВ</title></head>
      <body>tt26591147</body>
    </html>
    """

    async def _fake_fetch(url: str) -> str:
        if "action=2" in url:
            return ""
        return details_html

    monkeypatch.setattr(kinozal_details_module, "fetch_kinozal_html", _fake_fetch)
    kinozal_details_module._DETAILS_CACHE.clear()

    item = {
        "source_link": "https://kinozal.tv/details.php?id=2111748",
        "source_title": "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DLRip",
        "source_format": "",
        "source_year": None,
        "source_audio_tracks": [],
        "source_episode_progress": "",
        "source_release_type": "",
        "parsed_release_json": "",
    }

    enriched = asyncio.run(kinozal_details_module.enrich_kinozal_item_with_details(dict(item), force_refresh=True))

    assert enriched["details_title"].startswith("Мэтлок")
    assert enriched["source_year"] == 2025
    assert enriched["source_format"] == "1080"
    assert enriched["source_episode_progress"] == "2 сезон: 1-13 серии из 16"
    assert enriched["source_audio_tracks"] == ["ПМ (TVShows)"]
    assert enriched["source_release_type"] == "WEB-DL"
    assert enriched["parsed_release_json"]


def test_enrich_kinozal_item_with_details_does_not_override_existing_release_fields(monkeypatch) -> None:
    details_html = """
    <html>
      <head><title>Тест / Test / 2025 / ПМ / WEB-DL (1080p) :: Кинозал.ТВ</title></head>
      <body></body>
    </html>
    """

    async def _fake_fetch(_url: str) -> str:
        return details_html

    monkeypatch.setattr(kinozal_details_module, "fetch_kinozal_html", _fake_fetch)
    kinozal_details_module._DETAILS_CACHE.clear()

    item = {
        "source_link": "https://kinozal.tv/details.php?id=1",
        "source_title": "Тест / Test / 2025 / ПМ / WEB-DL (2160p)",
        "source_format": "2160",
        "source_year": 2025,
        "source_audio_tracks": ["ДБ"],
        "source_episode_progress": "1-2 серии из 10",
        "source_release_type": "WEB-DL",
        "parsed_release_json": '{"raw_title":"kept"}',
    }

    enriched = asyncio.run(kinozal_details_module.enrich_kinozal_item_with_details(dict(item), force_refresh=True))

    assert enriched["source_format"] == "2160"
    assert enriched["source_year"] == 2025
    assert enriched["source_audio_tracks"] == ["ДБ"]
    assert enriched["source_episode_progress"] == "1-2 серии из 10"
    assert enriched["source_release_type"] == "WEB-DL"
    assert enriched["parsed_release_json"] == '{"raw_title":"kept"}'


def test_enrich_kinozal_item_with_details_keeps_season_range_progress(monkeypatch) -> None:
    details_html = """
    <html>
      <head><title>Криминальное прошлое (1-2 сезон: 1-9 серии из 16) / Criminal Record / 2024-2026 / ПМ (RuDub), СТ / HEVC / WEBRip (1080p) :: Кинозал.ТВ</title></head>
      <body>tt21088136</body>
    </html>
    """

    async def _fake_fetch(url: str) -> str:
        if "action=2" in url:
            return ""
        return details_html

    monkeypatch.setattr(kinozal_details_module, "fetch_kinozal_html", _fake_fetch)
    kinozal_details_module._DETAILS_CACHE.clear()

    item = {
        "source_link": "https://kinozal.tv/details.php?id=2018161",
        "source_title": "Криминальное прошлое (1-2 сезон: 1-9 серии из 16) / Criminal Record / 2024-2026 / ПМ (RuDub), СТ / HEVC / WEBRip (1080p)",
        "source_format": "",
        "source_year": None,
        "source_audio_tracks": [],
        "source_episode_progress": "",
        "source_release_type": "",
        "parsed_release_json": "",
    }

    enriched = asyncio.run(kinozal_details_module.enrich_kinozal_item_with_details(dict(item), force_refresh=True))

    assert enriched["source_episode_progress"] == "1-2 сезон: 1-9 серии из 16"
