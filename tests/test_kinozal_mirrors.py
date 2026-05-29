import asyncio

import kinozal_details as kinozal_details_module
import kinozal_source as kinozal_source_module
from kinozal_source import KinozalSource


def test_kinozal_source_falls_back_to_mirror_when_primary_has_no_details(monkeypatch) -> None:
    monkeypatch.setenv("KINOZAL_BASE_URL", "https://kinozal.tv")
    monkeypatch.setenv("KINOZAL_MIRROR_BASE_URLS", "https://kinozal.guru")

    calls: list[str] = []

    async def _fake_fetch(url: str) -> tuple[str, str]:
        calls.append(url)
        if "kinozal.tv" in url:
            return url, "<html><head><title>Temporarily unavailable</title></head><body></body></html>"
        return url, """
        <table>
          <tr>
            <td><a href="/details.php?id=12345">Тестовый релиз / Test / 2026 / WEB-DL (1080p)</a></td>
            <td class="s">4</td>
            <td class="s">1.2 GB</td>
            <td class="s">Сегодня</td>
            <td class="sl_s">12</td>
            <td class="sl_p">3</td>
          </tr>
        </table>
        """

    monkeypatch.setattr(kinozal_source_module, "fetch_kinozal_html_with_url", _fake_fetch)

    items = asyncio.run(KinozalSource().fetch_latest())

    assert calls == [
        "https://kinozal.tv/browse.php?s=&page=0&c=0&d=0&v=0",
        "https://kinozal.guru/browse.php?s=&page=0&c=0&d=0&v=0",
    ]
    assert len(items) == 1
    assert items[0]["source_id"] == "12345"
    assert items[0]["source_link"] == "https://kinozal.guru/details.php?id=12345"


def test_kinozal_details_ajax_uses_source_link_domain(monkeypatch) -> None:
    calls: list[str] = []

    async def _fake_fetch(url: str) -> str:
        calls.append(url)
        if "details.php" in url:
            return """
            <html>
              <head><title>Тест / Test / 2026 / WEB-DL (1080p) :: Кинозал.ТВ</title></head>
              <body>tt1234567</body>
            </html>
            """
        return ""

    monkeypatch.setattr(kinozal_details_module, "fetch_kinozal_html", _fake_fetch)
    kinozal_details_module._DETAILS_CACHE.clear()

    item = {
        "source_link": "https://kinozal.guru/details.php?id=12345",
        "source_title": "Тест / Test / 2026 / WEB-DL (1080p)",
        "source_format": "",
        "source_year": None,
        "source_audio_tracks": [],
        "source_episode_progress": "",
        "source_release_type": "",
        "parsed_release_json": "",
    }

    enriched = asyncio.run(kinozal_details_module.enrich_kinozal_item_with_details(dict(item), force_refresh=True))

    assert enriched["source_imdb_id"] == "tt1234567"
    assert "https://kinozal.guru/get_srv_details.php?id=12345&action=2" in calls
