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


def test_tmdb_validation_rejects_short_prefix_match_for_long_movie_title() -> None:
    item = {
        "source_title": "Удачи, веселья, не сдохни / Good Luck, Have Fun, Don't Die / 2025 / ДБ, 2 x ПМ, АП (Яроцкий), ЛМ, СТ / Blu-Ray Remux (1080p)",
        "cleaned_title": "Удачи веселья не сдохни / Good Luck Have Fun Don't Die",
        "source_year": 2025,
        "media_type": "movie",
        "source_category_name": "Кино - Фантастика",
    }
    details = {
        "tmdb_id": 1459584,
        "media_type": "movie",
        "tmdb_title": "Good Luck",
        "tmdb_original_title": "グッドラック",
        "search_match_title": "Good Luck",
        "search_match_original_title": "グッドラック",
        "tmdb_release_date": "2025-01-01",
    }

    assert tmdb_match_looks_valid(item, "Good Luck", details, "movie") is False
    assert details["tmdb_validation_reject_code"] == "TRUNCATED_PREFIX_QUERY_MATCH"
    assert details["tmdb_validation_reject_reason"] == "tmdb_match_looks_valid:truncated_prefix_query"


def test_tmdb_validation_allows_anime_later_season_parent_series_despite_large_year_delta() -> None:
    item = {
        "source_title": "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)",
        "cleaned_title": "Жизнь в альтернативном мире с нуля / Re: Zero",
        "source_year": 2026,
        "source_episode_progress": "4 сезон: 1-2 серии из 18",
        "media_type": "tv",
        "source_category_name": "Мульт - Аниме",
        "bucket": "anime",
    }
    details = {
        "tmdb_id": 65942,
        "media_type": "tv",
        "tmdb_title": "Re: Жизнь в другом мире с нуля",
        "tmdb_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "search_match_title": "Re: Жизнь в другом мире с нуля",
        "search_match_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "tmdb_release_date": "2016-04-04",
        "tmdb_number_of_seasons": 4,
        "tmdb_number_of_episodes": 66,
        "tmdb_status": "Returning Series",
    }

    assert tmdb_match_looks_valid(item, "Re: Zero", details, "tv") is True


def test_tmdb_validation_allows_anime_later_season_parent_series_when_tmdb_season_count_missing() -> None:
    item = {
        "source_title": "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)",
        "cleaned_title": "Жизнь в альтернативном мире с нуля / Re: Zero",
        "source_year": 2026,
        "source_episode_progress": "4 сезон: 1-2 серии из 18",
        "media_type": "tv",
        "source_category_name": "Мульт - Аниме",
        "source_category_id": "20",
    }
    details = {
        "tmdb_id": 65942,
        "media_type": "tv",
        "tmdb_title": "Re: Жизнь в другом мире с нуля",
        "tmdb_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "search_match_title": "Re: Жизнь в другом мире с нуля",
        "search_match_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "tmdb_release_date": "2016-04-04",
        "tmdb_number_of_seasons": None,
        "tmdb_number_of_episodes": 66,
        "tmdb_status": "Returning Series",
    }

    assert tmdb_match_looks_valid(item, "Re: Zero", details, "tv") is True


def test_tmdb_validation_allows_later_season_parent_series_with_missing_season_count_and_bilingual_title() -> None:
    item = {
        "source_title": "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)",
        "cleaned_title": "Жизнь в альтернативном мире с нуля / Re: Zero",
        "source_year": 2026,
        "source_episode_progress": "4 сезон: 1-2 серии из 18",
        "media_type": "tv",
    }
    details = {
        "tmdb_id": 65942,
        "media_type": "tv",
        "tmdb_title": "Re: Жизнь в другом мире с нуля",
        "tmdb_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "search_match_title": "Re: Жизнь в другом мире с нуля",
        "search_match_original_title": "Re:Zero kara Hajimeru Isekai Seikatsu",
        "tmdb_release_date": "2016-04-04",
        "tmdb_number_of_seasons": None,
        "tmdb_number_of_episodes": 66,
        "tmdb_status": "Returning Series",
    }

    assert tmdb_match_looks_valid(item, "Re: Zero", details, "tv") is True


def test_tmdb_validation_allows_long_running_single_season_anime_parent_series() -> None:
    item = {
        "source_title": "Жизнь в альтернативном мире с нуля (4 сезон: 1-2 серии из 18) / Re: Zero / 2026 / ДБ (AniStar), ЛМ (AniLibria), СТ / WEB-DL (1080p)",
        "cleaned_title": "Жизнь в альтернативном мире с нуля / Re: Zero",
        "source_year": 2026,
        "source_episode_progress": "4 сезон: 1-2 серии из 18",
        "media_type": "tv",
        "source_category_name": "Мульт - Аниме",
        "source_category_id": "20",
    }
    details = {
        "tmdb_id": 65942,
        "media_type": "tv",
        "tmdb_title": "Re: Жизнь в другом мире с нуля",
        "tmdb_original_title": "Re:ゼロから始める異世界生活",
        "search_match_title": "Re:ZERO -Starting Life in Another World-",
        "search_match_original_title": "Re:ゼロから始める異世界生活",
        "tmdb_release_date": "2016-04-04",
        "tmdb_number_of_seasons": 1,
        "tmdb_number_of_episodes": 85,
        "tmdb_status": "Returning Series",
    }

    assert tmdb_match_looks_valid(item, "Re: Zero", details, "tv") is True
