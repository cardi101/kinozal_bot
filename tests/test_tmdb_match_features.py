from tmdb_match_features import extract_tmdb_match_features, score_tmdb_match_candidate


def test_extract_tmdb_match_features_keeps_year_and_episode_context() -> None:
    item = {
        "source_title": "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p)",
        "cleaned_title": "Мэтлок / Matlock",
        "source_year": 2025,
        "source_episode_progress": "2 сезон: 1-13 серии из 16",
        "media_type": "tv",
    }
    details = {
        "tmdb_id": 226174,
        "media_type": "tv",
        "tmdb_title": "Matlock",
        "tmdb_original_title": "Matlock",
        "search_match_title": "Matlock",
        "search_match_original_title": "Matlock",
        "tmdb_release_date": "2024-09-22",
        "tmdb_number_of_seasons": 1,
        "tmdb_number_of_episodes": 18,
        "search_score": 1.21,
    }

    features = extract_tmdb_match_features(item, "Matlock", details, "tv")

    assert features.requested_media_type == "tv"
    assert features.candidate_media_type == "tv"
    assert features.year_delta == 1
    assert features.season_hint == 2
    assert features.expected_episodes == 16
    assert features.title_similarity >= 0.99
    assert features.exact_normalized is True


def test_score_tmdb_match_candidate_prefers_modern_continuation_over_legacy_title() -> None:
    item = {
        "source_title": "Мэтлок (2 сезон: 1-13 серии из 16) / Matlock / 2025 / ПМ (TVShows) / WEB-DL (1080p)",
        "cleaned_title": "Мэтлок / Matlock",
        "source_year": 2025,
        "source_episode_progress": "2 сезон: 1-13 серии из 16",
        "media_type": "tv",
    }
    legacy = {
        "tmdb_id": 2093,
        "media_type": "tv",
        "tmdb_title": "Matlock",
        "tmdb_original_title": "Matlock",
        "search_match_title": "Matlock",
        "search_match_original_title": "Matlock",
        "tmdb_release_date": "1986-03-03",
        "tmdb_number_of_seasons": 9,
        "tmdb_number_of_episodes": 193,
        "search_score": 1.28,
    }
    modern = {
        "tmdb_id": 226174,
        "media_type": "tv",
        "tmdb_title": "Matlock",
        "tmdb_original_title": "Matlock",
        "search_match_title": "Matlock",
        "search_match_original_title": "Matlock",
        "tmdb_release_date": "2024-09-22",
        "tmdb_number_of_seasons": 1,
        "tmdb_number_of_episodes": 18,
        "search_score": 1.21,
    }

    legacy_score = score_tmdb_match_candidate(extract_tmdb_match_features(item, "Matlock", legacy, "tv"))
    modern_score = score_tmdb_match_candidate(extract_tmdb_match_features(item, "Matlock", modern, "tv"))

    assert modern_score > legacy_score
