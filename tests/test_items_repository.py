from repositories.items_repository import (
    _normalize_not_null_text_fields,
    _normalize_source_audio_tracks,
    _pick_best_kinozal_version,
)


def test_normalize_not_null_text_fields_backfills_empty_strings() -> None:
    payload = {
        "tmdb_match_confidence": None,
        "tmdb_match_evidence": None,
        "tmdb_match_debug": None,
        "manual_bucket": None,
        "manual_country_codes": None,
        "parsed_release_json": None,
        "source_release_text": None,
    }

    normalized = _normalize_not_null_text_fields(payload)

    assert normalized["tmdb_match_confidence"] == ""
    assert normalized["tmdb_match_evidence"] == ""
    assert normalized["tmdb_match_debug"] == ""
    assert normalized["manual_bucket"] == ""
    assert normalized["manual_country_codes"] == ""
    assert normalized["parsed_release_json"] == ""
    assert normalized["source_release_text"] == ""


def test_normalize_not_null_text_fields_keeps_existing_values() -> None:
    payload = {
        "tmdb_match_confidence": "  unmatched  ",
        "tmdb_match_evidence": "reason",
        "tmdb_match_debug": "debug",
        "manual_bucket": "anime",
        "manual_country_codes": "JP",
        "parsed_release_json": "{}",
        "source_release_text": "text",
    }

    normalized = _normalize_not_null_text_fields(payload)

    assert normalized["tmdb_match_confidence"] == "unmatched"
    assert normalized["tmdb_match_evidence"] == "reason"
    assert normalized["tmdb_match_debug"] == "debug"
    assert normalized["manual_bucket"] == "anime"
    assert normalized["manual_country_codes"] == "JP"
    assert normalized["parsed_release_json"] == "{}"
    assert normalized["source_release_text"] == "text"


def test_normalize_source_audio_tracks_unwraps_nested_json_strings() -> None:
    value = '"\\"[\\\\\\"ЛМ (Dream Cast, DreamyVoice)\\\\\\", \\\\\\"СТ\\\\\\"]\\""'

    assert _normalize_source_audio_tracks(value) == ["ЛМ (Dream Cast, DreamyVoice)", "СТ"]


def test_normalize_source_audio_tracks_keeps_plain_list() -> None:
    assert _normalize_source_audio_tracks(["ПО (Кураж-Бамбей)", "СТ"]) == ["ПО (Кураж-Бамбей)", "СТ"]


def test_pick_best_kinozal_version_prefers_higher_episode_progress_over_older_published_row() -> None:
    older = {
        "id": 1102,
        "source_episode_progress": "24 сезон: 1-7 серии из 15",
        "source_published_at": 1773340620,
        "created_at": 1773115667,
    }
    newer = {
        "id": 6354,
        "source_episode_progress": "24 сезон: 1-11 серии из 15",
        "source_published_at": None,
        "created_at": 1776170243,
    }

    assert _pick_best_kinozal_version([older, newer]) == newer
