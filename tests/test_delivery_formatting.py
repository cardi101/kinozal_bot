from delivery_formatting import grouped_items_message, item_message


class _DummyDb:
    def get_all_genres_merged(self):
        return {18: "драма", 35: "комедия"}


def test_item_message_uses_clean_user_layout_for_updates() -> None:
    db = _DummyDb()
    item = {
        "kinozal_id": "2124376",
        "source_title": "Каждый за себя (Переходим на голландский) (2 сезон: 1-10 серии из 12) / Going Dutch / 2026 / 2 x ПМ (HDrezka Studio) / WEB-DL (1080p)",
        "tmdb_title": "Каждый за себя",
        "tmdb_original_title": "Going Dutch",
        "media_type": "tv",
        "tmdb_release_date": "2026-01-01",
        "tmdb_rating": 7.1,
        "tmdb_vote_count": 321,
        "source_format": "1080",
        "source_episode_progress": "1-10 из 12",
        "previous_progress": "1-9 из 12",
        "previous_related_item_id": 5171,
        "previous_source_title": "Каждый за себя (Переходим на голландский) (2 сезон: 1-9 серии из 12) / Going Dutch / 2026 / ПМ (HDrezka Studio) / WEB-DL (1080p)",
        "genre_ids": [18, 35],
        "source_link": "https://kinozal.tv/details.php?id=2124376",
        "tmdb_id": 123456,
        "tmdb_overview": "Длинное описание, которое не должно попадать в основной текст.",
    }

    text = item_message(db, item, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert "🟢 UPDATE • TV • 1080p" in text
    assert "Изменение: 1-9 из 12 → 1-10 из 12; добавлена многоголосая дорожка" in text
    assert "Kinozal 2124376" in text
    assert "matched: 1" not in text
    assert "Релиз: <code>Каждый за себя" not in text
    assert "Озвучка: 2×ПМ (HDrezka Studio)" in text
    assert "Жанры: драма, комедия" in text
    assert "Категория API" not in text
    assert "Длинное описание" not in text


def test_item_message_marks_release_text_changes_explicitly() -> None:
    db = _DummyDb()
    item = {
        "kinozal_id": "3001",
        "source_title": "Test Film / 2026 / ПМ / WEB-DL (1080p)",
        "tmdb_title": "Test Film",
        "media_type": "movie",
        "tmdb_release_date": "2026-02-01",
    }

    text = item_message(db, item, matched_subs=[{"name": "🌍 Новинки — мир"}], old_release_text="old")

    assert "🟢 UPDATE" in text
    assert "Изменение: обновились детали релиза" in text


def test_item_message_marks_tv_items_as_new_and_keeps_series_line() -> None:
    db = _DummyDb()
    item = {
        "source_title": "Медики Чикаго (11 сезон: 1-17 серии из 22) / Chicago Med / 2025-2026 / ПМ (TVShows), ЛМ (LE-Production) / HEVC / WEBRip (1080p)",
        "tmdb_title": "Медики Чикаго",
        "tmdb_original_title": "Chicago Med",
        "media_type": "tv",
        "tmdb_release_date": "2025-01-01",
        "source_episode_progress": "11 сезон: 1-17 серии из 22",
    }

    text = item_message(db, item, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert "🆕 NEW • TV • 1080p" in text
    assert "Серии: 11 сезон: 1-17 серии из 22" in text


def test_grouped_items_message_uses_compact_multi_layout() -> None:
    db = _DummyDb()
    items = [
        {
            "kinozal_id": "4002",
            "source_title": "Show / 2026 / ПМ (HDrezka Studio) / WEB-DL (1080p)",
            "tmdb_title": "Show",
            "tmdb_original_title": "Show Original",
            "media_type": "tv",
            "tmdb_release_date": "2026-03-01",
            "source_episode_progress": "1-4 из 10",
            "source_format": "1080",
            "source_link": "https://kinozal.tv/details.php?id=4002",
            "tmdb_id": 4002,
        },
        {
            "kinozal_id": "4002",
            "source_title": "Show / 2026 / LostFilm / WEB-DL (1080p)",
            "tmdb_title": "Show",
            "media_type": "tv",
            "tmdb_release_date": "2026-03-01",
            "source_episode_progress": "1-4 из 10",
            "source_format": "1080",
            "source_link": "https://kinozal.tv/details.php?id=4002&v=2",
        },
    ]

    text = grouped_items_message(db, items, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert "📦 MULTI" in text
    assert "Kinozal 4002" in text
    assert "1-4 из 10" in text
    assert "Ссылки:" in text
    assert "matched: 1" not in text


def test_item_message_keeps_audio_multiplier_and_shows_countries() -> None:
    db = _DummyDb()
    item = {
        "kinozal_id": "2135794",
        "source_title": "F1 / F1: The Movie / 2025 / 2 x ДБ, СТ / HEVC / BDRip (1080p)",
        "tmdb_title": "F1",
        "tmdb_original_title": "F1: The Movie",
        "media_type": "movie",
        "tmdb_release_date": "2025-01-01",
        "source_format": "1080",
        "tmdb_rating": 7.8,
        "tmdb_vote_count": 3715,
        "genre_ids": [18, 35],
        "tmdb_countries": ["US", "GB"],
        "source_link": "https://kinozal.tv/details.php?id=2135794",
        "tmdb_id": 911430,
    }

    text = item_message(db, item, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert "Озвучка: 2×ДБ, СТ" in text
    assert "Страны: США, Великобритания" in text


def test_item_message_writes_full_audio_list_without_compact_suffix() -> None:
    db = _DummyDb()
    item = {
        "kinozal_id": "2135633",
        "source_title": "Последствия / Outcome / 2026 / ДБ, ПМ, ЛМ, СТ / WEB-DL (1080p)",
        "tmdb_title": "Последствия",
        "tmdb_original_title": "Outcome",
        "media_type": "movie",
        "tmdb_release_date": "2026-01-01",
        "source_format": "1080",
    }

    text = item_message(db, item, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert "Озвучка: ДБ, ПМ, ЛМ, СТ" in text
    assert "+2" not in text


def test_item_message_hides_low_vote_tmdb_rating() -> None:
    db = _DummyDb()
    item = {
        "kinozal_id": "2136086",
        "source_title": "Sample / 2026 / ПМ / WEB-DL (1080p)",
        "tmdb_title": "Sample",
        "media_type": "movie",
        "tmdb_release_date": "2026-01-01",
        "source_format": "1080",
        "tmdb_rating": 10.0,
        "tmdb_vote_count": 1,
    }

    text = item_message(db, item, matched_subs=[{"name": "🌍 Новинки — мир"}])

    assert "TMDB:" not in text
