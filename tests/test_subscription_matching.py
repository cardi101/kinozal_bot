from subscription_matching import (
    compile_subscription,
    explain_subscription_match,
    explain_subscription_match_details,
    match_subscription,
)


class FakeDB:
    def get_subscription_genres(self, _sub_id: int):
        return []


def make_world_subscription() -> dict:
    return {
        "id": 16,
        "name": "🌍 Новинки — мир",
        "preset_key": "world",
        "is_enabled": 1,
        "media_type": "any",
        "year_from": None,
        "year_to": None,
        "allow_720": 0,
        "allow_1080": 1,
        "allow_2160": 1,
        "min_tmdb_rating": None,
        "genre_ids": [],
        "content_filter": "exclude_anime_dorama",
        "country_codes": "",
        "country_codes_list": [],
        "exclude_country_codes": "TR,RU,UA,JP,KR,CN,TW,TH,HK,ID,MY,SG,PH",
        "exclude_country_codes_list": ["TR", "RU", "UA", "JP", "KR", "CN", "TW", "TH", "HK", "ID", "MY", "SG", "PH"],
        "include_keywords": "",
        "exclude_keywords": "hdr,lossless,mp3,flac,fb2,epub,pdf,mobi,ру,укр,украин",
    }


def make_regular_item(*countries: str) -> dict:
    return {
        "media_type": "movie",
        "genre_ids": [16],
        "tmdb_countries": list(countries),
        "tmdb_original_language": "en",
        "source_title": "Супер Марио: Галактическое кино / The Super Mario Galaxy Movie / 2026 / ДБ / WEBRip (1080p)",
        "source_description": "",
        "source_category_name": "Мульт - Буржуйский",
        "source_format": "1080",
        "tmdb_rating": 7.5,
        "tmdb_title": "Супер Марио: Галактическое кино",
        "tmdb_original_title": "The Super Mario Galaxy Movie",
        "tmdb_id": 1226863,
        "tmdb_release_date": "2026-01-01",
        "manual_bucket": "regular",
    }


def test_world_subscription_allows_regular_coproduction_with_non_excluded_country() -> None:
    db = FakeDB()
    sub = make_world_subscription()
    item = make_regular_item("JP", "US")

    assert match_subscription(db, sub, item) is True
    assert explain_subscription_match(db, sub, item) == "passed"


def test_world_subscription_still_rejects_fully_excluded_regular_country_set() -> None:
    db = FakeDB()
    sub = make_world_subscription()
    item = make_regular_item("JP")

    assert match_subscription(db, sub, item) is False
    assert explain_subscription_match(db, sub, item) == "excluded_country:['JP']"


def test_custom_excluded_countries_keep_strict_any_intersection_behavior() -> None:
    db = FakeDB()
    sub = make_world_subscription() | {
        "id": 999,
        "name": "Custom",
        "preset_key": "",
        "exclude_country_codes": "JP",
        "exclude_country_codes_list": ["JP"],
        "content_filter": "any",
    }
    item = make_regular_item("JP", "US")

    assert match_subscription(db, sub, item) is False
    assert explain_subscription_match(db, sub, item) == "excluded_country:['JP']"


def test_compiled_subscription_keeps_matching_semantics() -> None:
    db = FakeDB()
    sub = make_world_subscription()
    compiled = compile_subscription(db, sub)
    item = make_regular_item("JP", "US")

    assert match_subscription(db, compiled, item) is True
    assert explain_subscription_match(db, compiled, item) == "passed"


def test_explain_subscription_match_details_contains_structured_checks() -> None:
    db = FakeDB()
    sub = make_world_subscription()
    item = make_regular_item("JP")

    details = explain_subscription_match_details(db, sub, item)

    assert details["summary"] == "excluded_country:['JP']"
    assert details["compiled_subscription_snapshot"]["exclude_country_codes"]
    assert any(check["check"] == "excluded_country" and check["passed"] is False for check in details["checks"])
