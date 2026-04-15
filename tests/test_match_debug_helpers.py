from match_debug_helpers import _strip_existing_match_fields


def test_strip_existing_match_fields_marks_explicit_match_clear() -> None:
    cleaned = _strip_existing_match_fields(
        {
            "id": 42,
            "source_title": "Sample",
            "media_type": "tv",
            "tmdb_id": 123,
            "tmdb_title": "Stored",
            "tmdb_match_path": "search",
            "tmdb_match_confidence": "high",
            "imdb_id": "tt1234567",
        }
    )

    assert cleaned["source_title"] == "Sample"
    assert cleaned["_clear_tmdb_match"] is True
    assert "tmdb_id" not in cleaned
    assert "imdb_id" not in cleaned
    assert "media_type" not in cleaned
