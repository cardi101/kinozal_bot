from content_buckets import item_content_bucket


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

    assert item_content_bucket(item) == "anime"


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

    assert item_content_bucket(item) == "dorama"
