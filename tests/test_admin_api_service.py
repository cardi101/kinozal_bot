import os
import asyncio
from types import SimpleNamespace

os.environ.setdefault("BOT_TOKEN", "123456:TESTTOKEN")
os.environ.setdefault("DATABASE_URL", "postgresql://example/example")

import services.admin_api_service as admin_api_module
from domain import ReleaseItem
from services.admin_api_service import AdminApiService


class _FakeDB:
    def __init__(self) -> None:
        self.saved_payload = None

    def find_item_by_kinozal_id(self, kinozal_id: str):
        return {
            "id": 42,
            "kinozal_id": kinozal_id,
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": "Before title",
            "source_release_text": "old text",
            "source_episode_progress": "1 сезон: 1-7 серии из 10",
            "media_type": "tv",
        }

    def find_item_any_by_kinozal_id(self, kinozal_id: str):
        return self.find_item_by_kinozal_id(kinozal_id)

    def get_item(self, item_id: int):
        return {
            "id": item_id,
            "kinozal_id": "2128422",
            "source_uid": "kinozal:2128422",
            "source_title": "After title",
            "source_release_text": "new text",
            "source_episode_progress": "1 сезон: 1-10 серии из 10",
            "media_type": "tv",
        }

    def get_item_any(self, item_id: int):
        return self.get_item(item_id)

    def save_item(self, payload: dict):
        self.saved_payload = dict(payload)
        return 99, True, True

    def list_enabled_subscriptions(self):
        return []

    def get_subscription(self, sub_id: int):
        return None


class _FakeReplayDB:
    def __init__(self) -> None:
        self.recorded = []
        self.meta = {}
        self.resolved_anomalies = []
        self.conn = SimpleNamespace(execute=lambda *args, **kwargs: SimpleNamespace(fetchone=lambda: None))

    def get_user(self, tg_user_id: int):
        return {"tg_user_id": tg_user_id}

    def find_item_by_kinozal_id(self, kinozal_id: str):
        return {
            "id": 42,
            "kinozal_id": kinozal_id,
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": "Replay title",
            "media_type": "tv",
        }

    def find_item_any_by_kinozal_id(self, kinozal_id: str):
        return self.find_item_by_kinozal_id(kinozal_id)

    def get_item(self, item_id: int):
        item = self.find_item_by_kinozal_id("2128422")
        item["id"] = item_id
        return item

    def get_item_any(self, item_id: int):
        return self.get_item(item_id)

    def list_user_subscriptions(self, tg_user_id: int):
        return [{"id": 7, "tg_user_id": tg_user_id}]

    def list_enabled_subscriptions(self):
        return [{"id": 7, "tg_user_id": 1001}]

    def get_subscription(self, sub_id: int):
        return {"id": sub_id, "tg_user_id": 1001, "name": "Sub", "is_enabled": 1}

    def get_subscription_genres(self, sub_id: int):
        return []

    def delivered_equivalent(self, tg_user_id: int, item: dict) -> bool:
        return False

    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        return False

    def begin_delivery_claim(
        self,
        tg_user_id: int,
        item_id: int,
        sub_id: int,
        matched_ids,
        delivery_audit=None,
        context: str = "",
        event_type: str = "",
        event_key: str = "",
    ):
        return True

    def record_delivery(self, tg_user_id: int, item_id: int, sub_id: int, matched_ids, delivery_audit=None):
        self.recorded.append((tg_user_id, item_id, sub_id, list(matched_ids), delivery_audit))

    def mark_delivery_claim_failed(self, tg_user_id: int, item_id: int, error: str = ""):
        return None

    def is_title_muted(self, tg_user_id: int, tmdb_id: int) -> bool:
        return False

    def recently_delivered_kinozal_id(self, tg_user_id: int, kinozal_id: str, cooldown_seconds: int) -> bool:
        return False

    def get_user_quiet_hours(self, tg_user_id: int):
        return None, None

    def get_user_quiet_profile(self, tg_user_id: int):
        return None, None, ""

    def list_release_anomalies(self, kinozal_id: str, limit: int = 10):
        return []

    def get_pending_match_review_by_item_id(self, item_id: int):
        return None

    def get_meta(self, key: str):
        return self.meta.get(key)

    def set_meta(self, key: str, value: str):
        self.meta[key] = value

    def resolve_release_anomalies(self, kinozal_id: str, *, item_id=None, anomaly_type: str = "", status: str = "resolved"):
        self.resolved_anomalies.append((kinozal_id, item_id, anomaly_type, status))
        return 1


class _FakeKinozalService:
    async def enrich_item_with_details(self, item: ReleaseItem, force_refresh: bool = False) -> ReleaseItem:
        item.set("source_title", "After title")
        item.set("details_title", "After title")
        item.set("source_release_text", "new text")
        item.set("source_episode_progress", "1 сезон: 1-10 серии из 10")
        return item


def test_reparse_release_persists_refreshed_item() -> None:
    db = _FakeDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=_FakeKinozalService(),
        bot=None,
    )

    result = asyncio.run(service.reparse_release("2128422"))

    assert db.saved_payload is not None
    assert db.saved_payload["source_title"] == "After title"
    assert db.saved_payload["source_episode_progress"] == "1 сезон: 1-10 серии из 10"
    assert result["item_id"] == 99
    assert result["item"]["source_title"] == "After title"


def test_replay_delivery_respects_non_force_suppressors(monkeypatch) -> None:
    db = _FakeReplayDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=object(),
    )

    async def _fake_send_item_to_user(*args, **kwargs):
        return None

    monkeypatch.setattr(admin_api_module, "send_item_to_user", _fake_send_item_to_user)
    monkeypatch.setattr(admin_api_module, "match_subscription", lambda db, sub, item: True)
    monkeypatch.setattr(
        service,
        "explain_delivery",
        lambda kinozal_id, tg_user_id, cooldown_seconds=420: {
            "blockers": ["anomaly_hold", "quiet_hours"],
        },
    )

    result = asyncio.run(service.replay_delivery("2128422", 1001, force=False))

    assert result["status"] == "skipped"
    assert result["reason"] == "anomaly_hold"
    assert db.recorded == []


class _ArchivedReplayDB(_FakeReplayDB):
    def find_item_by_kinozal_id(self, kinozal_id: str):
        return None

    def find_item_any_by_kinozal_id(self, kinozal_id: str):
        item = _FakeReplayDB.find_item_by_kinozal_id(self, kinozal_id)
        item["id"] = 77
        return item


def test_replay_delivery_uses_archive_aware_lookup(monkeypatch) -> None:
    db = _ArchivedReplayDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=object(),
    )

    async def _fake_send_item_to_user(*args, **kwargs):
        return None

    monkeypatch.setattr(admin_api_module, "send_item_to_user", _fake_send_item_to_user)
    monkeypatch.setattr(admin_api_module, "match_subscription", lambda db, sub, item: True)
    monkeypatch.setattr(service, "explain_delivery", lambda kinozal_id, tg_user_id, cooldown_seconds=420: {"blockers": []})

    result = asyncio.run(service.replay_delivery("2128422", 1001, force=False))

    assert result["status"] == "sent"
    assert result["item_id"] == 77
    assert db.recorded[0][1] == 77


def test_replay_delivery_to_matching_users_forces_replay_and_resolves_anomalies(monkeypatch) -> None:
    db = _FakeReplayDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=object(),
    )

    async def _fake_send_item_to_user(*args, **kwargs):
        return None

    monkeypatch.setattr(admin_api_module, "send_item_to_user", _fake_send_item_to_user)
    monkeypatch.setattr(admin_api_module, "match_subscription", lambda db, sub, item: True)

    result = asyncio.run(
        service.replay_delivery_to_matching_users(
            "2128422",
            force=True,
            resolve_anomalies=True,
        )
    )

    assert result["matched_subscriptions"] == 1
    assert result["matched_users"] == 1
    assert result["delivered_count"] == 1
    assert result["status_counts"] == {"sent": 1}
    assert result["resolved_anomalies"] == 1
    assert db.resolved_anomalies == [("2128422", 42, "", "resolved")]


def test_replay_delivery_to_matching_users_honors_explicit_item_id(monkeypatch) -> None:
    db = _FakeReplayDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=object(),
    )

    async def _fake_send_item_to_user(*args, **kwargs):
        return None

    monkeypatch.setattr(admin_api_module, "send_item_to_user", _fake_send_item_to_user)
    monkeypatch.setattr(admin_api_module, "match_subscription", lambda db, sub, item: True)

    result = asyncio.run(
        service.replay_delivery_to_matching_users(
            "2128422",
            item_id=77,
            force=True,
            resolve_anomalies=True,
        )
    )

    assert result["item_id"] == 77
    assert db.recorded[0][1] == 77
    assert db.resolved_anomalies == [("2128422", 77, "", "resolved")]


class _ArchivedOnlyDB(_FakeDB):
    def find_item_by_kinozal_id(self, kinozal_id: str):
        return None

    def find_item_any_by_kinozal_id(self, kinozal_id: str):
        return {
            "id": 77,
            "kinozal_id": kinozal_id,
            "source_uid": f"kinozal:{kinozal_id}",
            "source_title": "Archived title",
            "source_release_text": "old text",
            "source_episode_progress": "1 сезон: 1-8 серии из 10",
            "media_type": "tv",
        }


def test_build_match_debug_uses_archive_aware_lookup() -> None:
    db = _ArchivedOnlyDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=None,
    )

    result = asyncio.run(service.build_match_debug("2128422", live=False))

    assert result["stored_item"]["id"] == 77
    assert result["live_item"] is None


def test_build_match_debug_strips_raw_tmdb_debug_from_item_payload() -> None:
    class _DebugDB(_FakeDB):
        def find_item_by_kinozal_id(self, kinozal_id: str):
            item = super().find_item_by_kinozal_id(kinozal_id)
            item["tmdb_match_debug"] = '[{"stage":"candidate_probe"}]'
            return item

    db = _DebugDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=None,
    )

    result = asyncio.run(service.build_match_debug("2128422", live=False))

    assert "tmdb_match_debug" not in result["stored_item"]
    assert result["stored_tmdb_debug_events"] == [{"stage": "candidate_probe"}]


def test_build_match_debug_live_bypasses_stored_override() -> None:
    class _CapturingTMDBService:
        def __init__(self) -> None:
            self.last_payload = None

        async def enrich_item(self, item: ReleaseItem) -> ReleaseItem:
            self.last_payload = item.to_dict()
            enriched = item.to_dict()
            enriched["tmdb_id"] = 555
            enriched["tmdb_title"] = "Live Match"
            return ReleaseItem.from_payload(enriched)

    tmdb_service = _CapturingTMDBService()
    service = AdminApiService(
        db=_FakeDB(),
        tmdb_service=tmdb_service,
        kinozal_service=None,
        bot=None,
    )

    result = asyncio.run(service.build_match_debug("2128422", live=True))

    assert tmdb_service.last_payload is not None
    assert tmdb_service.last_payload["_skip_kinozal_override"] is True
    assert result["live_item"]["tmdb_id"] == 555
    assert "_skip_kinozal_override" not in result["live_item"]


class _ExplainConn:
    def execute(self, query: str, params=None):
        del params

        class _Cursor:
            def __init__(self, rows):
                self._rows = rows

            def fetchone(self):
                return self._rows[0] if self._rows else None

            def fetchall(self):
                return self._rows

        if "FROM delivery_claims" in query:
            return _Cursor(
                [
                    {
                        "event_type": "release",
                        "event_key": "release:1001:2128422:v1",
                        "status": "sent",
                        "delivery_context": "worker",
                        "claimed_at": 1,
                        "updated_at": 2,
                        "sent_at": 2,
                        "last_error": "",
                    }
                ]
            )
        return _Cursor([])


class _ExplainDB(_FakeReplayDB):
    def __init__(self) -> None:
        super().__init__()
        self.conn = _ExplainConn()


def test_explain_delivery_returns_semantic_event_debug(monkeypatch) -> None:
    db = _ExplainDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=None,
        bot=None,
    )

    monkeypatch.setattr(
        admin_api_module,
        "explain_subscription_match_details",
        lambda db, sub, item: {"summary": "passed", "checks": [{"check": "media", "passed": True}], "compiled_subscription_snapshot": {"id": int(sub["id"])}},
    )

    result = service.explain_delivery("2128422", 1001)

    assert result["status"] in {"ready", "delivered"}
    assert result["computed_delivery_events"]["release"]["event_key"].startswith("release:1001:2128422:")
    assert result["computed_delivery_events"]["release_text"]["event_key"].startswith("release_text:1001:2128422:")
    assert result["delivery_claims"][0]["event_key"] == "release:1001:2128422:v1"
    assert result["matched_subscriptions"][0]["compiled_subscription"] == {"id": 7}


def test_reparse_release_uses_archive_aware_lookup() -> None:
    db = _ArchivedOnlyDB()
    service = AdminApiService(
        db=db,
        tmdb_service=None,
        kinozal_service=_FakeKinozalService(),
        bot=None,
    )

    result = asyncio.run(service.reparse_release("2128422"))

    assert db.saved_payload is not None
    assert db.saved_payload["source_title"] == "After title"
    assert result["item_id"] == 99
