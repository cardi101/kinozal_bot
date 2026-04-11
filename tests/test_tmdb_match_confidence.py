from tmdb_client import _search_match_confidence


def test_search_match_confidence_accepts_alias_match_from_parentheses() -> None:
    item = {
        "source_title": "Сирена (Поцелуй серены) / Seiren (Siren's Kiss) / 2026 / ЛД / WEBRip",
        "cleaned_title": "Сирена (Поцелуй серены) / Seiren (Siren's Kiss)",
        "source_year": 2026,
    }
    details = {
        "search_match_title": "Siren's Kiss",
        "search_match_original_title": "Seiren",
        "tmdb_title": "Сирена",
        "tmdb_original_title": "세이렌",
        "tmdb_release_date": "2026-03-02",
    }

    confidence, evidence = _search_match_confidence(item, details)

    assert confidence in {"high", "medium"}
    assert "exact=1" in evidence


def test_search_match_confidence_keeps_bad_title_match_low() -> None:
    item = {
        "source_title": "Mac / 2025 / RU / Система",
        "cleaned_title": "Mac",
        "source_year": 2025,
    }
    details = {
        "search_match_title": "Macbeth",
        "search_match_original_title": "Macbeth",
        "tmdb_title": "Macbeth",
        "tmdb_original_title": "Macbeth",
        "tmdb_release_date": "2015-01-01",
    }

    confidence, _evidence = _search_match_confidence(item, details)

    assert confidence == "low"
