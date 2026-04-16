from parsing_audio import parse_audio_tracks


def test_parse_audio_tracks_keeps_professional_single_voice_label_with_studio() -> None:
    assert parse_audio_tracks("Title / 2025 / ПО (Кураж-Бамбей) / WEB-DL (1080p)") == ["ПО (Кураж-Бамбей)"]


def test_parse_audio_tracks_supports_bare_professional_single_voice() -> None:
    assert parse_audio_tracks("Title / 2025 / ПО / WEB-DL (1080p)") == ["ПО"]


def test_parse_audio_tracks_supports_pd_label() -> None:
    assert parse_audio_tracks("Title / 2025 / ПД / WEB-DL (1080p)") == ["ПД"]


def test_parse_audio_tracks_preserves_studio_grouping() -> None:
    assert parse_audio_tracks("Title / 2026 / ЛМ (Dream Cast, DreamyVoice), СТ / HEVC / WEBRip (1080p)") == [
        "ЛМ (Dream Cast, DreamyVoice)",
        "СТ",
    ]


def test_parse_audio_tracks_keeps_existing_core_labels() -> None:
    assert parse_audio_tracks("Последствия / Outcome / 2026 / ДБ, ПМ, ЛМ, СТ / WEB-DL (1080p)") == [
        "ДБ",
        "ПМ",
        "ЛМ",
        "СТ",
    ]
