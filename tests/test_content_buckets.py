from content_buckets import item_content_bucket, resolve_item_content_bucket


def test_asian_cartoon_category_is_bucketed_as_anime_without_tmdb_animation_genre() -> None:
    item = {
        "source_category_id": 21,
        "source_category_name": "Мульт - Буржуйский",
        "media_type": "movie",
        "genre_ids": [12, 14, 35],
        "tmdb_countries": ["CN"],
        "tmdb_original_language": "zh",
        "source_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai / 2025 / ПМ / WEB-DL (2160p)",
        "cleaned_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai",
        "tmdb_title": "Маленькие демоны путешествуют на Запад",
        "tmdb_original_title": "浪浪山小妖怪",
    }

    decision = resolve_item_content_bucket(item)

    assert decision["bucket"] == "anime"
    assert decision["reason"] == "asian_animation_invariant"


def test_non_animation_asian_movie_can_still_be_dorama() -> None:
    item = {
        "source_category_id": 47,
        "source_category_name": "Кино - Азиатский",
        "media_type": "movie",
        "genre_ids": [18],
        "tmdb_countries": ["KR"],
        "tmdb_original_language": "ko",
        "source_title": "Обычная корейская драма / Some Korean Drama / 2025 / ПМ / WEB-DL (1080p)",
        "cleaned_title": "Обычная корейская драма / Some Korean Drama",
        "tmdb_title": "Обычная корейская драма",
        "tmdb_original_title": "어떤 드라마",
    }

    decision = resolve_item_content_bucket(item)

    assert decision["bucket"] == "dorama"
    assert decision["reason"] in {"source_category_dorama_hint", "asian_dorama_signal"}


def test_asian_animation_overrides_dorama_source_category_hint() -> None:
    item = {
        "source_category_id": 47,
        "source_category_name": "Кино - Азиатский",
        "media_type": "movie",
        "genre_ids": [16, 14],
        "tmdb_countries": ["CN"],
        "tmdb_original_language": "zh",
        "source_title": "Китайский мульт / Chinese Animated Film / 2025 / ПМ / WEB-DL (1080p)",
        "cleaned_title": "Китайский мульт / Chinese Animated Film",
        "tmdb_title": "Китайский мульт",
        "tmdb_original_title": "中国动画电影",
    }

    decision = resolve_item_content_bucket(item)

    assert decision["bucket"] == "anime"
    assert decision["reason"] == "asian_animation_overrides_dorama_hint"


def test_western_animation_stays_regular_and_never_becomes_dorama() -> None:
    item = {
        "source_category_id": 21,
        "source_category_name": "Мульт - Буржуйский",
        "media_type": "movie",
        "genre_ids": [16, 35],
        "tmdb_countries": ["US"],
        "tmdb_original_language": "en",
        "source_title": "Обычный западный мульт / Regular Western Cartoon / 2025 / ПМ / WEB-DL (1080p)",
        "cleaned_title": "Обычный западный мульт / Regular Western Cartoon",
        "tmdb_title": "Обычный западный мульт",
        "tmdb_original_title": "Regular Western Cartoon",
    }

    decision = resolve_item_content_bucket(item)

    assert decision["bucket"] == "regular"
    assert decision["reason"] == "default_regular"


def test_manual_bucket_override_wins_over_asian_animation_invariant() -> None:
    item = {
        "manual_bucket": "dorama",
        "source_category_id": 21,
        "source_category_name": "Мульт - Буржуйский",
        "media_type": "movie",
        "genre_ids": [16, 14],
        "tmdb_countries": ["CN"],
        "tmdb_original_language": "zh",
        "source_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai / 2025 / ПМ / WEB-DL (2160p)",
        "cleaned_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai",
        "tmdb_title": "Маленькие демоны путешествуют на Запад",
        "tmdb_original_title": "浪浪山小妖怪",
    }

    decision = resolve_item_content_bucket(item)

    assert decision["bucket"] == "dorama"
    assert decision["reason"] == "manual_bucket_override"


def test_source_category_anime_hint_wins_even_without_strong_metadata() -> None:
    item = {
        "source_category_id": 20,
        "source_category_name": "Мульт - Аниме",
        "media_type": "movie",
        "genre_ids": [],
        "tmdb_countries": [],
        "tmdb_original_language": "",
        "source_title": "Какой-то релиз / 2025 / ПМ / WEB-DL (1080p)",
        "cleaned_title": "Какой-то релиз",
        "tmdb_title": "",
        "tmdb_original_title": "",
    }

    decision = resolve_item_content_bucket(item)

    assert decision["bucket"] == "anime"
    assert decision["reason"] == "source_category_anime_hint"


def test_item_content_bucket_keeps_backward_compatible_string_api() -> None:
    item = {
        "source_category_id": 21,
        "source_category_name": "Мульт - Буржуйский",
        "media_type": "movie",
        "genre_ids": [16, 14],
        "tmdb_countries": ["CN"],
        "tmdb_original_language": "zh",
        "source_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai / 2025 / ПМ / WEB-DL (2160p)",
        "cleaned_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai",
        "tmdb_title": "Маленькие демоны путешествуют на Запад",
        "tmdb_original_title": "浪浪山小妖怪",
    }

    assert item_content_bucket(item) == "anime"
