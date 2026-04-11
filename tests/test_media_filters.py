from media_detection import is_russian_release


def test_is_russian_release_detects_audio_track_labels() -> None:
    item = {
        "source_title": "Месть как лекарство (1-2 серии из 2) / 2017 / РУ, СТ /",
        "source_category_name": "Зарубежные сериалы",
        "source_audio_tracks": ["РУ", "СТ"],
    }

    assert is_russian_release(item) is True


def test_is_russian_release_detects_title_markers_without_audio_tracks() -> None:
    item = {
        "source_title": "Что-то / 2024 / РУ, ПМ / WEBRip (1080p)",
        "source_category_name": "Мульт - Буржуйский",
        "source_audio_tracks": [],
    }

    assert is_russian_release(item) is True


def test_is_russian_release_ignores_non_russian_foreign_release() -> None:
    item = {
        "source_title": "The Super Mario Galaxy Movie / 2026 / ДБ / WEBRip (1080p)",
        "source_category_name": "Мульт - Буржуйский",
        "source_audio_tracks": ["ДБ"],
    }

    assert is_russian_release(item) is False
