from services.subscription_service import SubscriptionService


class _FakeRepository:
    def __init__(self) -> None:
        self.list_enabled_calls = 0
        self.get_subscription_calls = 0

    def list_enabled_subscriptions(self):
        self.list_enabled_calls += 1
        return [
            {
                "id": 7,
                "tg_user_id": 1001,
                "name": "Sub",
                "is_enabled": 1,
                "media_type": "tv",
                "allow_1080": 1,
                "allow_720": 0,
                "allow_2160": 0,
                "genre_ids": [18],
                "country_codes_list": ["US"],
                "exclude_country_codes_list": [],
                "content_filter": "any",
                "include_keywords": "",
                "exclude_keywords": "",
            }
        ]

    def get_subscription(self, sub_id: int):
        self.get_subscription_calls += 1
        return {"id": sub_id}

    def get_subscription_genres(self, _sub_id: int):
        return [18]


def test_list_enabled_compiled_avoids_per_subscription_get_subscription_calls() -> None:
    repository = _FakeRepository()
    service = SubscriptionService(repository)

    compiled = service.list_enabled_compiled()

    assert len(compiled) == 1
    assert compiled[0].media_type == "tv"
    assert repository.list_enabled_calls == 1
    assert repository.get_subscription_calls == 0
