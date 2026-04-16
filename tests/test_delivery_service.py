import asyncio

import services.delivery_service as delivery_service_module
from domain import DeliveryCandidate, ReleaseItem, SubscriptionRecord
from services.delivery_service import DeliveryService


class _FakeDB:
    def get_subscription_genres(self, _sub_id: int):
        return []


class _FakeRepository:
    def __init__(self) -> None:
        self.db = _FakeDB()
        self.claimed = []
        self.failed = []
        self.recorded = []

    def get_latest_delivered_related_item(self, tg_user_id: int, item: dict):
        return None

    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item_id: int,
        primary_sub_id: int,
        matched_sub_ids,
        delivery_audit=None,
        context: str = "",
        event_type: str = "",
        event_key: str = "",
    ):
        self.claimed.append((tg_user_id, item_id, primary_sub_id, list(matched_sub_ids), context, event_type, event_key))
        return True

    def mark_delivery_claim_failed(self, tg_user_id: int, item_id: int, error: str = ""):
        self.failed.append((tg_user_id, item_id, error))

    def record_delivery(self, tg_user_id: int, item_id: int, primary_sub_id: int, matched_sub_ids, delivery_audit=None):
        self.recorded.append((tg_user_id, item_id, primary_sub_id, list(matched_sub_ids), delivery_audit))


def _candidate() -> DeliveryCandidate:
    return DeliveryCandidate(
        item=ReleaseItem.from_payload(
            {
                "id": 42,
                "kinozal_id": "2128422",
                "source_uid": "kinozal:2128422",
                "source_title": "Sample / 2026 / ПМ / WEB-DL (1080p)",
                "media_type": "movie",
                "source_format": "1080",
                "version_signature": "v1",
            }
        ),
        subs=[SubscriptionRecord.from_payload({"id": 7, "tg_user_id": 1001, "name": "Sub", "is_enabled": 1})],
        old_release_text="",
        is_release_text_change=False,
    )


def test_send_single_claims_and_records(monkeypatch) -> None:
    repository = _FakeRepository()
    service = DeliveryService(repository=repository, bot=object())

    async def _fake_send_item_to_user(*args, **kwargs):
        return None

    monkeypatch.setattr(delivery_service_module, "send_item_to_user", _fake_send_item_to_user)

    asyncio.run(service.send_single(1001, _candidate()))

    assert repository.claimed == [(1001, 42, 7, [7], "worker", "release", "release:1001:2128422:v1")]
    assert len(repository.recorded) == 1
    assert repository.failed == []


def test_send_single_marks_claim_failed_on_send_error(monkeypatch) -> None:
    repository = _FakeRepository()
    service = DeliveryService(repository=repository, bot=object())

    async def _fake_send_item_to_user(*args, **kwargs):
        raise RuntimeError("telegram down")

    monkeypatch.setattr(delivery_service_module, "send_item_to_user", _fake_send_item_to_user)

    try:
        asyncio.run(service.send_single(1001, _candidate()))
    except RuntimeError:
        pass
    else:
        raise AssertionError("send_single should propagate send failure")

    assert repository.claimed == [(1001, 42, 7, [7], "worker", "release", "release:1001:2128422:v1")]
    assert repository.recorded == []
    assert repository.failed
