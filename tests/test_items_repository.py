from repositories.items_repository import _normalize_source_audio_tracks


def test_normalize_source_audio_tracks_unwraps_nested_json_strings() -> None:
    value = '"\\"[\\\\\\"ЛМ (Dream Cast, DreamyVoice)\\\\\\", \\\\\\"СТ\\\\\\"]\\""'

    assert _normalize_source_audio_tracks(value) == ["ЛМ (Dream Cast, DreamyVoice)", "СТ"]


def test_normalize_source_audio_tracks_keeps_plain_list() -> None:
    assert _normalize_source_audio_tracks(["ПО (Кураж-Бамбей)", "СТ"]) == ["ПО (Кураж-Бамбей)", "СТ"]
