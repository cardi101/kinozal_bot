from delivery_audit import build_delivery_audit


class _FakeDB:
    def get_subscription_genres(self, _sub_id: int):
        return []


def test_build_delivery_audit_includes_bucket_context_and_match_reasons() -> None:
    item = {
        "id": 6053,
        "kinozal_id": "2135734",
        "source_title": "Маленькие демоны путешествуют на Запад / Langlang shan xiao yao guai / 2025 / ПМ / WEB-DL (2160p)",
        "source_category_name": "Мульт - Буржуйский",
        "media_type": "movie",
        "tmdb_id": 1304434,
        "tmdb_title": "Маленькие демоны путешествуют на Запад",
        "tmdb_match_path": "search",
        "tmdb_match_confidence": "high",
        "tmdb_countries": ["CN"],
        "tmdb_original_language": "zh",
        "manual_bucket": "anime",
        "genre_ids": [12, 14, 16, 35],
        "source_format": "2160",
        "tmdb_rating": 7.8,
        "tmdb_release_date": "2025-08-02",
    }
    sub = {
        "id": 19,
        "name": "🍥 Новинки — аниме",
        "preset_key": "anime",
        "is_enabled": 1,
        "media_type": "any",
        "year_from": None,
        "year_to": None,
        "allow_720": 0,
        "allow_1080": 1,
        "allow_2160": 1,
        "min_tmdb_rating": None,
        "genre_ids": [],
        "content_filter": "only_anime",
        "country_codes": "",
        "country_codes_list": [],
        "exclude_country_codes": "",
        "exclude_country_codes_list": [],
        "include_keywords": "",
        "exclude_keywords": "",
    }

    audit = build_delivery_audit(_FakeDB(), item, [sub], context="worker")

    assert audit["context"] == "worker"
    assert audit["bucket"] == "anime"
    assert audit["kinozal_id"] == "2135734"
    assert audit["tmdb_match_confidence"] == "high"
    assert audit["item_snapshot"]["source_title"] == item["source_title"]
    assert audit["item_snapshot"]["source_format"] == "2160"
    assert audit["matched_subscriptions"][0]["id"] == 19
    assert audit["matched_subscriptions"][0]["reason"] == "passed"
    assert audit["matched_subscriptions"][0]["explain"]
    assert audit["matched_subscriptions"][0]["compiled_subscription"]["id"] == 19
