from tmdb_match_validation import tmdb_match_looks_valid


def test_tmdb_validation_allows_on_air_anime_episode_total_mismatch_as_soft_warning() -> None:
    item = {
        "source_title": "Я вернулась! Не помешаю? (1-2 серии из 12) / Tadaima, Ojama Saremasu! / 2026 / ЛМ (Dream Cast, DreamyVoice), СТ / HEVC / WEBRip (1080p)",
        "cleaned_title": "Я вернулась! Не помешаю? / Tadaima, Ojama Saremasu!",
        "source_year": 2026,
        "source_episode_progress": "1-2 серии из 12",
        "media_type": "tv",
        "source_category_name": "Аниме",
    }
    details = {
        "tmdb_id": 7777,
        "media_type": "tv",
        "tmdb_title": "Tadaima, Ojama Saremasu!",
        "tmdb_original_title": "ただいま、おじゃまされます！",
        "search_match_title": "Tadaima, Ojama Saremasu!",
        "search_match_original_title": "ただいま、おじゃまされます！",
        "tmdb_release_date": "2026-01-09",
        "tmdb_number_of_seasons": 1,
        "tmdb_number_of_episodes": 24,
        "tmdb_status": "Returning Series",
    }

    assert tmdb_match_looks_valid(item, "Tadaima, Ojama Saremasu!", details, "tv") is True
    assert details["tmdb_validation_warnings"] == ["episode_total_mismatch_soft"]


def test_tmdb_validation_sets_stable_reject_code_for_large_tv_year_delta() -> None:
    item = {
        "source_title": "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p)",
        "cleaned_title": "Мэтлок / Matlock",
        "source_year": 2025,
        "source_episode_progress": "2 сезон: 1-13 серии из 16",
        "media_type": "tv",
    }
    details = {
        "tmdb_id": 2093,
        "media_type": "tv",
        "tmdb_title": "Мэтлок",
        "tmdb_original_title": "Matlock",
        "search_match_title": "Matlock",
        "search_match_original_title": "Matlock",
        "tmdb_release_date": "1986-03-03",
        "tmdb_number_of_seasons": 9,
        "tmdb_number_of_episodes": 193,
        "tmdb_status": "Ended",
    }

    assert tmdb_match_looks_valid(item, "Matlock", details, "tv") is False
    assert details["tmdb_validation_reject_code"] == "TV_YEAR_DELTA_EXTREME"
    assert details["tmdb_validation_reject_reason"] == "tmdb_match_looks_valid:L441"
