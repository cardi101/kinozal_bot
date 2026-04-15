import asyncio

from domain import ReleaseItem
from services.worker_service import WorkerService


def test_should_emit_anomaly_alert_only_for_impacted_users() -> None:
    assert WorkerService._should_emit_anomaly_alert({}, None) is False
    assert WorkerService._should_emit_anomaly_alert({123: []}, None) is True


def test_should_emit_anomaly_alert_suppresses_duplicates() -> None:
    existing = {"id": 1, "status": "open"}
    assert WorkerService._should_emit_anomaly_alert({123: []}, existing) is False


class _FakeWorkerRepository:
    def __init__(self) -> None:
        self.deleted: list[tuple[int, int]] = []
        self.subscriptions = {
            7: {
                "id": 7,
                "tg_user_id": 1001,
                "name": "Sub",
                "is_enabled": 1,
            }
        }

    def pop_due_pending_deliveries(self, current_hour: int):
        return {
            1001: [
                {
                    "item_id": 42,
                    "matched_sub_ids": "7",
                    "old_release_text": "",
                    "is_release_text_change": 0,
                }
            ]
        }

    def pop_due_debounce(self):
        return [
            {
                "tg_user_id": 1001,
                "item_id": 42,
                "kinozal_id": "2128422",
                "matched_sub_ids": "7",
            }
        ]

    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        return False

    def delivered_equivalent(self, tg_user_id: int, item: dict) -> bool:
        return False

    def get_item_any(self, item_id: int):
        return {
            "id": item_id,
            "kinozal_id": "2128422",
            "source_uid": "kinozal:2128422",
            "source_title": "Archived release",
            "media_type": "tv",
            "source_episode_progress": "1 сезон: 1-8 серии из 10",
        }

    def get_subscription(self, subscription_id: int):
        return self.subscriptions.get(subscription_id)

    def list_user_subscriptions(self, tg_user_id: int):
        return [sub for sub in self.subscriptions.values() if sub["tg_user_id"] == tg_user_id]

    def delete_pending_delivery(self, tg_user_id: int, item_id: int) -> None:
        self.deleted.append((tg_user_id, item_id))


class _FakeKinozalService:
    async def enrich_item_with_details(self, item: ReleaseItem, force_refresh: bool = False) -> ReleaseItem:
        item.set("details_title", item.get("source_title"))
        return item


class _FakeDeliveryService:
    def __init__(self) -> None:
        self.sent: list[tuple[int, int]] = []
        self.recorded: list[tuple[int, int, str]] = []
        self.claimed: list[tuple[int, int, str]] = []
        self.failed: list[tuple[int, int]] = []

    async def send_item(self, tg_user_id: int, item: ReleaseItem, subs, old_release_text: str = "") -> None:
        self.sent.append((tg_user_id, item.id))

    def record_delivery(self, tg_user_id: int, item: ReleaseItem, subs, context: str = "worker") -> None:
        self.recorded.append((tg_user_id, item.id, context))

    def begin_delivery_claim(self, tg_user_id: int, item: ReleaseItem, subs, context: str = "worker") -> bool:
        self.claimed.append((tg_user_id, item.id, context))
        return True

    def mark_delivery_claim_failed(self, tg_user_id: int, item: ReleaseItem, error: str = "") -> None:
        self.failed.append((tg_user_id, item.id))

    async def deliver_claimed_item(self, tg_user_id: int, item: ReleaseItem, subs, *, context: str = "worker", old_release_text: str = "") -> None:
        await self.send_item(tg_user_id, item, subs, old_release_text=old_release_text)
        self.record_delivery(tg_user_id, item, subs, context=context)


class _FakeSubscriptionService:
    def matches(self, sub: dict, item: ReleaseItem) -> bool:
        return True


def test_flush_due_pending_deliveries_uses_archived_item_payload() -> None:
    repository = _FakeWorkerRepository()
    delivery_service = _FakeDeliveryService()
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_FakeSubscriptionService(),
        delivery_service=delivery_service,
        bot=None,
    )

    metrics = worker._new_cycle_metrics()
    asyncio.run(worker._flush_due_pending_deliveries(current_hour=12, cycle_metrics=metrics))

    assert delivery_service.sent == [(1001, 42)]
    assert delivery_service.recorded == [(1001, 42, "pending_flush")]
    assert repository.deleted == [(1001, 42)]
    assert metrics["deliveries_sent_total"] == 1


def test_flush_due_debounce_uses_archived_item_payload() -> None:
    repository = _FakeWorkerRepository()
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_FakeSubscriptionService(),
        delivery_service=_FakeDeliveryService(),
        bot=None,
    )

    pending: dict[int, list] = {}
    asyncio.run(worker._flush_due_debounce(pending))

    assert 1001 in pending
    assert len(pending[1001]) == 1
    assert pending[1001][0].item_id == 42


def test_flush_due_pending_deliveries_falls_back_to_current_matching_subscriptions() -> None:
    repository = _FakeWorkerRepository()
    repository.subscriptions = {
        11: {
            "id": 11,
            "tg_user_id": 1001,
            "name": "New Sub",
            "is_enabled": 1,
        }
    }
    delivery_service = _FakeDeliveryService()
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_FakeSubscriptionService(),
        delivery_service=delivery_service,
        bot=None,
    )

    metrics = worker._new_cycle_metrics()
    asyncio.run(worker._flush_due_pending_deliveries(current_hour=12, cycle_metrics=metrics))

    assert delivery_service.sent == [(1001, 42)]
    assert repository.deleted == [(1001, 42)]


def test_flush_due_debounce_falls_back_to_current_matching_subscriptions() -> None:
    repository = _FakeWorkerRepository()
    repository.subscriptions = {
        11: {
            "id": 11,
            "tg_user_id": 1001,
            "name": "New Sub",
            "is_enabled": 1,
        }
    }
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_FakeSubscriptionService(),
        delivery_service=_FakeDeliveryService(),
        bot=None,
    )

    pending: dict[int, list] = {}
    asyncio.run(worker._flush_due_debounce(pending))

    assert 1001 in pending
    assert pending[1001][0].subs[0].id == 11


class _SelectiveSubscriptionService:
    def matches(self, sub, item: ReleaseItem) -> bool:
        return int(sub.id if hasattr(sub, "id") else sub["id"]) == 11


def test_resolve_delivery_subscriptions_rechecks_stored_subscription_match() -> None:
    repository = _FakeWorkerRepository()
    repository.subscriptions = {
        7: {
            "id": 7,
            "tg_user_id": 1001,
            "name": "Old Sub",
            "is_enabled": 1,
        },
        11: {
            "id": 11,
            "tg_user_id": 1001,
            "name": "New Sub",
            "is_enabled": 1,
        },
    }
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_SelectiveSubscriptionService(),
        delivery_service=_FakeDeliveryService(),
        bot=None,
    )

    subs = worker._resolve_delivery_subscriptions(
        1001,
        ReleaseItem.from_payload(repository.get_item_any(42)),
        "7",
    )

    assert [sub.id for sub in subs] == [11]
