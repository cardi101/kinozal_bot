import asyncio

from config import CFG
import services.worker_service as worker_service_module
from delivery_events import build_delivery_event_key, build_grouped_event_key
from domain import DeliveryCandidate, ReleaseItem, SubscriptionRecord
from services.worker_service import WorkerService


def test_should_emit_anomaly_alert_only_for_impacted_users() -> None:
    assert WorkerService._should_emit_anomaly_alert({}, None) is False
    assert WorkerService._should_emit_anomaly_alert({123: []}, None) is True


def test_should_emit_anomaly_alert_suppresses_duplicates() -> None:
    existing = {"id": 1, "status": "open"}
    assert WorkerService._should_emit_anomaly_alert({123: []}, existing) is False


class _AlertBot:
    def __init__(self) -> None:
        self.calls = []

    async def send_message(self, chat_id: int, text: str, **kwargs) -> None:
        self.calls.append((chat_id, text, kwargs))


def test_notify_admins_about_anomaly_attaches_inline_actions() -> None:
    original_admin_ids = CFG.admin_ids
    original_ops_alert_chat_ids = CFG.ops_alert_chat_ids
    CFG.admin_ids = ()
    CFG.ops_alert_chat_ids = (-1003930216844,)
    bot = _AlertBot()
    service = WorkerService(
        repository=None,
        kinozal_service=None,
        tmdb_service=None,
        subscription_service=None,
        delivery_service=None,
        bot=bot,
    )
    item = ReleaseItem.from_payload(
        {
            "id": 6882,
            "kinozal_id": "2130194",
            "source_uid": "kinozal:2130194",
            "source_title": "Клиника",
            "media_type": "tv",
            "source_episode_progress": "1 сезон: 1-9 серии из 9",
        }
    )

    try:
        sent = asyncio.run(
            service._notify_admins_about_anomaly(
                "2130194",
                item,
                "progress_regression",
                "10 сезон: 1-7 серии из 9",
                "1 сезон: 1-9 серии из 9",
            )
        )
    finally:
        CFG.admin_ids = original_admin_ids
        CFG.ops_alert_chat_ids = original_ops_alert_chat_ids

    assert sent == 1
    assert bot.calls[0][0] == -1003930216844
    reply_markup = bot.calls[0][2]["reply_markup"]
    buttons = [button.callback_data for row in reply_markup.inline_keyboard for button in row]
    assert buttons == ["anomaly:timeline:2130194", "anomaly:replay:2130194"]


class _FakeWorkerRepository:
    def __init__(self) -> None:
        self.deleted: list[tuple[int, int]] = []
        self.deleted_debounce: list[tuple[int, str]] = []
        self.released_pending: list[tuple[int, str]] = []
        self.released_debounce: list[tuple[int, str, str]] = []
        self.queued_pending: list[dict] = []
        self.persisted_event_keys: set[str] = set()
        self.quiet_profile = (None, None, "")
        self.subscriptions = {
            7: {
                "id": 7,
                "tg_user_id": 1001,
                "name": "Sub",
                "is_enabled": 1,
            }
        }

    def lease_due_pending_deliveries(self, current_ts=None):
        return [
            {
                "id": 1,
                "tg_user_id": 1001,
                "item_id": 42,
                "matched_sub_ids": "7",
                "old_release_text": "",
                "is_release_text_change": 0,
                "event_key": "release:1001:2128422:v1",
                "lease_token": "pending-1",
            }
        ]

    def lease_due_debounce_entries(self, current_ts=None):
        return [
            {
                "tg_user_id": 1001,
                "item_id": 42,
                "kinozal_id": "2128422",
                "matched_sub_ids": "7",
                "lease_token": "debounce-1",
            }
        ]

    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        return False

    def delivered_persisted(self, tg_user_id: int, item_id: int) -> bool:
        return False

    def delivery_event_persisted(self, tg_user_id: int, item_id: int, *, event_type: str = "", event_key: str = "") -> bool:
        del tg_user_id, item_id, event_type
        return str(event_key or "") in self.persisted_event_keys

    def delivered_equivalent(self, tg_user_id: int, item: dict) -> bool:
        return False

    def delivered_equivalent_persisted(self, tg_user_id: int, item: dict) -> bool:
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

    def get_user_quiet_hours(self, tg_user_id: int):
        return None, None

    def get_user_quiet_profile(self, tg_user_id: int):
        del tg_user_id
        return self.quiet_profile

    def delete_pending_delivery(self, tg_user_id: int, item_id: int, event_key: str = "") -> None:
        self.deleted.append((tg_user_id, item_id))

    def queue_pending_delivery(
        self,
        tg_user_id: int,
        item_id: int,
        matched_sub_ids: str,
        old_release_text: str,
        is_release_text_change: bool,
        *,
        event_type: str = "",
        event_key: str = "",
        deliver_not_before_ts=None,
    ) -> None:
        self.queued_pending.append(
            {
                "tg_user_id": tg_user_id,
                "item_id": item_id,
                "matched_sub_ids": matched_sub_ids,
                "old_release_text": old_release_text,
                "is_release_text_change": is_release_text_change,
                "event_type": event_type,
                "event_key": event_key,
                "deliver_not_before_ts": deliver_not_before_ts,
            }
        )

    def delete_debounce_entry(self, tg_user_id: int, kinozal_id: str) -> None:
        self.deleted_debounce.append((tg_user_id, kinozal_id))

    def release_pending_delivery_lease(self, pending_id: int, *, lease_token: str, error: str = "", deliver_not_before_ts=None) -> None:
        self.released_pending.append((pending_id, lease_token))

    def release_debounce_lease(self, tg_user_id: int, kinozal_id: str, *, lease_token: str, error: str = "", deliver_after_ts=None) -> None:
        self.released_debounce.append((tg_user_id, kinozal_id, lease_token))


class _FakeKinozalService:
    async def enrich_item_with_details(self, item: ReleaseItem, force_refresh: bool = False) -> ReleaseItem:
        item.set("details_title", item.get("source_title"))
        return item


class _FakeDeliveryService:
    def __init__(self) -> None:
        self.sent: list[tuple[int, int]] = []
        self.grouped_sent: list[tuple[int, tuple[int, ...]]] = []
        self.recorded: list[tuple[int, int, str, str, str]] = []
        self.claimed: list[tuple[int, int, str, str, str]] = []
        self.failed: list[tuple[int, int, str]] = []

    async def send_item(self, tg_user_id: int, item: ReleaseItem, subs, old_release_text: str = "") -> None:
        self.sent.append((tg_user_id, item.id))

    async def send_single(self, tg_user_id: int, delivery) -> None:
        await self.send_item(tg_user_id, delivery.item, delivery.subs, old_release_text=delivery.old_release_text)

    async def send_grouped_items(self, tg_user_id: int, items, subs) -> None:
        self.grouped_sent.append((tg_user_id, tuple(item.id for item in items)))

    def build_candidate_delivery_event(self, tg_user_id: int, delivery, *, context: str = "worker"):
        return (
            delivery.event_type or ("release_text" if delivery.is_release_text_change else "release"),
            delivery.event_key
            or build_delivery_event_key(
                tg_user_id,
                delivery.item,
                context=context,
                is_release_text_change=delivery.is_release_text_change,
                release_text=delivery.old_release_text or delivery.item.get("source_release_text") or "",
            ),
        )

    def build_group_delivery_event(self, tg_user_id: int, items, *, group_key: str = ""):
        return "grouped", build_grouped_event_key(tg_user_id, items, group_key=group_key)

    def record_delivery(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs,
        context: str = "worker",
        *,
        event_type: str = "",
        event_key: str = "",
        old_release_text: str = "",
        grouped_event_key: str = "",
    ) -> None:
        self.recorded.append((tg_user_id, item.id, context, event_type, event_key or grouped_event_key))

    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs,
        context: str = "worker",
        *,
        event_type: str = "",
        event_key: str = "",
        old_release_text: str = "",
        grouped_event_key: str = "",
    ) -> bool:
        self.claimed.append((tg_user_id, item.id, context, event_type, event_key or grouped_event_key))
        return True

    def mark_delivery_claim_failed(self, tg_user_id: int, item: ReleaseItem, error: str = "", *, event_key: str = "") -> None:
        self.failed.append((tg_user_id, item.id, event_key))

    async def deliver_claimed_item(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs,
        *,
        context: str = "worker",
        old_release_text: str = "",
        event_type: str = "",
        event_key: str = "",
        grouped_event_key: str = "",
    ) -> None:
        await self.send_item(tg_user_id, item, subs, old_release_text=old_release_text)
        self.record_delivery(
            tg_user_id,
            item,
            subs,
            context=context,
            event_type=event_type,
            event_key=event_key,
            grouped_event_key=grouped_event_key,
        )


class _ClaimBlockedDeliveryService(_FakeDeliveryService):
    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item: ReleaseItem,
        subs,
        context: str = "worker",
        *,
        event_type: str = "",
        event_key: str = "",
        old_release_text: str = "",
        grouped_event_key: str = "",
    ) -> bool:
        self.claimed.append((tg_user_id, item.id, context, event_type, event_key or grouped_event_key))
        return False


class _FailingSingleDeliveryService(_FakeDeliveryService):
    async def send_single(self, tg_user_id: int, delivery) -> None:
        raise RuntimeError("send failed")


class _FakeSubscriptionService:
    def matches(self, sub: dict, item: ReleaseItem) -> bool:
        return True


class _EmptySubscriptionService:
    def list_enabled(self):
        return []


class _CachedRefreshRepository:
    def __init__(self) -> None:
        self.saved_payload = None
        self.meta = {"bootstrap_done": "1"}
        self.cached = {
            "tmdb_id": 86831,
            "tmdb_title": "Любовь. Смерть. Роботы",
            "tmdb_match_path": "search",
            "tmdb_match_confidence": "",
            "imdb_id": "tt9561862",
            "media_type": "tv",
            "cleaned_title": "Любовь после смерти / Mu Xu Ci",
        }

    def get_meta(self, key: str):
        return self.meta.get(key)

    def set_meta(self, key: str, value: str) -> None:
        self.meta[key] = value

    def record_source_observation(self, *args, **kwargs):
        return 0

    def find_existing_enriched(self, source_uid, source_title):
        return dict(self.cached)

    def save_item(self, payload: dict):
        self.saved_payload = dict(payload)
        return 1, False, True

    def was_delivered_to_anyone(self, item_id: int) -> bool:
        return False

    def get_item(self, item_id: int):
        return self.saved_payload

    def find_higher_progress_reference(self, kinozal_id: str, progress: str, item_id: int | None = None):
        return None

    def lease_due_pending_deliveries(self, current_ts=None):
        return []

    def lease_due_debounce_entries(self, current_ts=None):
        return []


class _FetchSingleKinozalService:
    async def fetch_latest(self):
        return [
            ReleaseItem.from_payload(
                {
                    "source_uid": "kinozal:2134721",
                    "kinozal_id": "2134721",
                    "source_title": "Любовь после смерти (Любовь за гранью смерти) (1 сезон: 1-40 серии из 40) / Mu Xu Ci / 2026 / ЛМ (DubLik TV) / WEB-DL (1080p)",
                    "source_category_id": "46",
                    "source_category_name": "Сериал - Буржуйский",
                    "source_year": 2026,
                    "source_episode_progress": "1 сезон: 1-40 серии из 40",
                    "source_audio_tracks": ["ЛМ (DubLik TV)"],
                    "source_format": "1080",
                    "media_type": "tv",
                }
            )
        ]

    async def enrich_item_with_details(self, item: ReleaseItem, force_refresh: bool = False) -> ReleaseItem:
        return item


class _FakeRefreshTMDBService:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def enrich_item(self, item: ReleaseItem) -> ReleaseItem:
        self.calls.append(item.to_dict())
        enriched = item.clone()
        enriched.set("tmdb_id", 282503)
        enriched.set("tmdb_title", "Любовь сильнее смерти")
        enriched.set("tmdb_match_path", "search")
        enriched.set("tmdb_match_confidence", "medium")
        return enriched


class _NoopDeliveryService:
    def record_delivery(self, *args, **kwargs) -> None:
        return None


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
    assert delivery_service.claimed == [
        (1001, 42, "pending_flush", "release", "release:1001:2128422:v1")
    ]
    assert delivery_service.recorded == [
        (1001, 42, "pending_flush", "release", "release:1001:2128422:v1")
    ]
    assert repository.deleted == [(1001, 42)]
    assert metrics["deliveries_sent_total"] == 1


def test_process_new_items_refreshes_cached_match_without_confidence() -> None:
    repository = _CachedRefreshRepository()
    tmdb_service = _FakeRefreshTMDBService()
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FetchSingleKinozalService(),
        tmdb_service=tmdb_service,
        subscription_service=_EmptySubscriptionService(),
        delivery_service=_NoopDeliveryService(),
        bot=None,
    )

    metrics = worker._new_cycle_metrics()
    asyncio.run(worker.process_new_items(metrics))

    assert len(tmdb_service.calls) == 1
    assert tmdb_service.calls[0]["_clear_tmdb_match"] is True
    assert repository.saved_payload["tmdb_id"] == 282503
    assert repository.saved_payload["tmdb_title"] == "Любовь сильнее смерти"
    assert repository.saved_payload["tmdb_match_confidence"] == "medium"
    assert metrics["items_tmdb_enriched_total"] == 1


def test_flush_due_pending_deliveries_keeps_row_when_claim_already_exists() -> None:
    repository = _FakeWorkerRepository()
    delivery_service = _ClaimBlockedDeliveryService()
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

    assert delivery_service.sent == []
    assert repository.deleted == []
    assert repository.released_pending == [(1, "pending-1")]


def test_flush_due_pending_deliveries_preserves_release_text_event_identity() -> None:
    repository = _FakeWorkerRepository()
    repository.lease_due_pending_deliveries = lambda current_ts=None: [
        {
            "id": 1,
            "tg_user_id": 1001,
            "item_id": 42,
            "matched_sub_ids": "7",
            "old_release_text": "old text",
            "is_release_text_change": 1,
            "event_type": "release_text",
            "event_key": "release_text:1001:2128422:abc",
            "lease_token": "pending-1",
        }
    ]
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

    assert delivery_service.claimed == [
        (1001, 42, "pending_flush", "release_text", "release_text:1001:2128422:abc")
    ]
    assert delivery_service.recorded == [
        (1001, 42, "pending_flush", "release_text", "release_text:1001:2128422:abc")
    ]


def test_flush_due_pending_deliveries_does_not_drop_release_text_when_base_release_already_persisted() -> None:
    repository = _FakeWorkerRepository()
    repository.lease_due_pending_deliveries = lambda current_ts=None: [
        {
            "id": 1,
            "tg_user_id": 1001,
            "item_id": 42,
            "matched_sub_ids": "7",
            "old_release_text": "old text",
            "is_release_text_change": 1,
            "event_type": "release_text",
            "event_key": "release_text:1001:2128422:abc",
            "lease_token": "pending-1",
        }
    ]
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
    assert pending[1001][0].debounce_kinozal_id == "2128422"


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


def test_debounce_entry_deleted_only_after_successful_delivery() -> None:
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

    pending: dict[int, list] = {}
    asyncio.run(worker._flush_due_debounce(pending))
    metrics = worker._new_cycle_metrics()
    asyncio.run(worker._deliver_current_cycle(pending, current_hour=12, cycle_metrics=metrics))

    assert repository.deleted_debounce == [(1001, "2128422")]
    assert delivery_service.sent == [(1001, 42)]


def test_debounce_entry_survives_failed_delivery() -> None:
    repository = _FakeWorkerRepository()
    delivery_service = _FailingSingleDeliveryService()
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_FakeSubscriptionService(),
        delivery_service=delivery_service,
        bot=None,
    )

    pending: dict[int, list] = {}
    asyncio.run(worker._flush_due_debounce(pending))
    metrics = worker._new_cycle_metrics()
    asyncio.run(worker._deliver_current_cycle(pending, current_hour=12, cycle_metrics=metrics))

    assert repository.deleted_debounce == []
    assert repository.released_debounce == [(1001, "2128422", "debounce-1")]


def test_grouped_delivery_uses_single_group_envelope_event_key() -> None:
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

    item_a = ReleaseItem.from_payload(
        {
            "id": 42,
            "kinozal_id": "2128422",
            "source_uid": "kinozal:2128422",
            "source_title": "A",
            "media_type": "movie",
            "tmdb_id": 77,
            "version_signature": "v1",
        }
    )
    item_b = ReleaseItem.from_payload(
        {
            "id": 43,
            "kinozal_id": "2128423",
            "source_uid": "kinozal:2128423",
            "source_title": "B",
            "media_type": "movie",
            "tmdb_id": 77,
            "version_signature": "v2",
        }
    )
    deliveries = {
        1001: [
            DeliveryCandidate(
                item=item_a,
                subs=[SubscriptionRecord.from_payload({"id": 7, "tg_user_id": 1001, "name": "Sub", "is_enabled": 1})],
                delivery_context="worker",
                event_type="release",
                event_key="release:1001:2128422:v1",
            ),
            DeliveryCandidate(
                item=item_b,
                subs=[SubscriptionRecord.from_payload({"id": 7, "tg_user_id": 1001, "name": "Sub", "is_enabled": 1})],
                delivery_context="worker",
                event_type="release",
                event_key="release:1001:2128423:v2",
            ),
        ]
    }

    metrics = worker._new_cycle_metrics()
    asyncio.run(worker._deliver_current_cycle(deliveries, current_hour=12, cycle_metrics=metrics))

    assert delivery_service.grouped_sent == [(1001, (42, 43))]
    grouped_claims = [row for row in delivery_service.claimed if row[2] == "grouped"]
    assert len(grouped_claims) == 2
    assert grouped_claims[0][3] == "grouped"
    assert grouped_claims[0][4] != grouped_claims[1][4]
    assert grouped_claims[0][4].startswith("grouped:1001:tmdb:77:")
    assert grouped_claims[1][4].startswith("grouped:1001:tmdb:77:")
    grouped_records = [row for row in delivery_service.recorded if row[2] == "grouped"]
    assert len(grouped_records) == 2
    assert grouped_records[0][4] != grouped_records[1][4]
    assert grouped_records[0][4].startswith("grouped:1001:tmdb:77:")
    assert grouped_records[1][4].startswith("grouped:1001:tmdb:77:")


def test_quiet_hours_queue_preserves_grouped_event_identity() -> None:
    repository = _FakeWorkerRepository()
    repository.quiet_profile = (22, 8, "Europe/Berlin")
    delivery_service = _FakeDeliveryService()
    worker = WorkerService(
        repository=repository,
        kinozal_service=_FakeKinozalService(),
        tmdb_service=None,
        subscription_service=_FakeSubscriptionService(),
        delivery_service=delivery_service,
        bot=None,
    )
    original_quiet_window_status = worker_service_module.quiet_window_status
    original_next_quiet_end = worker_service_module.next_quiet_window_end_ts
    worker_service_module.quiet_window_status = lambda start_h, end_h, timezone_name: {
        "active": True,
        "timezone": timezone_name,
        "local_hour": 2,
    }
    worker_service_module.next_quiet_window_end_ts = lambda start_h, end_h, timezone_name: 999999

    item_a = ReleaseItem.from_payload(
        {
            "id": 42,
            "kinozal_id": "2128422",
            "source_uid": "kinozal:2128422",
            "source_title": "A",
            "media_type": "movie",
            "tmdb_id": 77,
            "version_signature": "v1",
        }
    )
    item_b = ReleaseItem.from_payload(
        {
            "id": 43,
            "kinozal_id": "2128423",
            "source_uid": "kinozal:2128423",
            "source_title": "B",
            "media_type": "movie",
            "tmdb_id": 77,
            "version_signature": "v2",
        }
    )
    deliveries = {
        1001: [
            DeliveryCandidate(
                item=item_a,
                subs=[SubscriptionRecord.from_payload({"id": 7, "tg_user_id": 1001, "name": "Sub", "is_enabled": 1})],
                delivery_context="worker",
                event_type="release",
                event_key="release:1001:2128422:v1",
            ),
            DeliveryCandidate(
                item=item_b,
                subs=[SubscriptionRecord.from_payload({"id": 7, "tg_user_id": 1001, "name": "Sub", "is_enabled": 1})],
                delivery_context="worker",
                event_type="release",
                event_key="release:1001:2128423:v2",
            ),
        ]
    }

    try:
        metrics = worker._new_cycle_metrics()
        asyncio.run(worker._deliver_current_cycle(deliveries, current_hour=12, cycle_metrics=metrics))
    finally:
        worker_service_module.quiet_window_status = original_quiet_window_status
        worker_service_module.next_quiet_window_end_ts = original_next_quiet_end

    assert len(repository.queued_pending) == 2
    assert {row["event_type"] for row in repository.queued_pending} == {"grouped"}
    assert repository.queued_pending[0]["event_key"] != repository.queued_pending[1]["event_key"]
    assert repository.queued_pending[0]["event_key"].startswith("grouped:1001:tmdb:77:")
    assert repository.queued_pending[1]["event_key"].startswith("grouped:1001:tmdb:77:")


def test_flush_due_pending_deliveries_regroups_grouped_events() -> None:
    repository = _FakeWorkerRepository()
    repository.lease_due_pending_deliveries = lambda current_ts=None: [
        {
            "id": 1,
            "tg_user_id": 1001,
            "item_id": 42,
            "matched_sub_ids": "7",
            "old_release_text": "",
            "is_release_text_change": 0,
            "event_type": "grouped",
            "event_key": "grouped:1001:tmdb:77:abcd:item:42",
            "lease_token": "pending-1",
        },
        {
            "id": 2,
            "tg_user_id": 1001,
            "item_id": 43,
            "matched_sub_ids": "7",
            "old_release_text": "",
            "is_release_text_change": 0,
            "event_type": "grouped",
            "event_key": "grouped:1001:tmdb:77:abcd:item:43",
            "lease_token": "pending-2",
        },
    ]
    original_get_item_any = repository.get_item_any
    repository.get_item_any = lambda item_id: {
        **original_get_item_any(item_id),
        "id": item_id,
        "kinozal_id": "2128422" if item_id == 42 else "2128423",
        "source_uid": "kinozal:2128422" if item_id == 42 else "kinozal:2128423",
        "tmdb_id": 77,
        "version_signature": "v1" if item_id == 42 else "v2",
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

    assert delivery_service.grouped_sent == [(1001, (42, 43))]
    assert delivery_service.sent == []


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
