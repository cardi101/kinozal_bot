from parsed_release import parse_release_title


def test_parse_release_title_extracts_structured_tv_fields() -> None:
    parsed = parse_release_title(
        "Я вернулась! Не помешаю? (1-2 серии из 12) / Tadaima, Ojama Saremasu! / 2026 / ЛМ (Dream Cast, DreamyVoice), СТ / HEVC / WEBRip (1080p)",
        "tv",
    )

    assert parsed.title_local == "Я вернулась Не помешаю"
    assert parsed.title_original == "Tadaima Ojama Saremasu"
    assert parsed.year == 2026
    assert parsed.episode_start == 1
    assert parsed.episode_end == 2
    assert parsed.episode_total == 12
    assert parsed.resolution == "1080"
    assert parsed.codec == "hevc"
    assert parsed.audio_tracks == ["ЛМ (Dream Cast, DreamyVoice)", "СТ"]


def test_parse_release_title_handles_movie_without_episode_progress() -> None:
    parsed = parse_release_title("Последствия / Outcome / 2026 / ДБ, ПМ, ЛМ, СТ / WEB-DL (1080p)", "movie")

    assert parsed.title_local == "Последствия"
    assert parsed.title_original == "Outcome"
    assert parsed.episode_progress_text == ""
    assert parsed.release_type == "WEB-DL"
    assert parsed.audio_tracks == ["ДБ", "ПМ", "ЛМ", "СТ"]
