import threading

import repositories.delivery_repository as delivery_repo_module
from repositories.delivery_repository import (
    DELIVERY_CLAIM_LEASE_SECONDS,
    DeliveryRepository,
    _delivery_claim_is_active,
)


class _FakeCursor:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row

    def fetchall(self):
        return [self._row] if self._row is not None else []


class _FakeConn:
    def __init__(self, claim_row=None, *, live_item_exists: bool = True):
        self.claim_rows = {}
        if claim_row:
            event_key = str(claim_row.get("event_key") or "legacy")
            self.claim_rows[event_key] = dict(claim_row)
        self.commits = 0
        self.live_item_exists = live_item_exists
        self.deliveries = []
        self.archive_deliveries = []

    def execute(self, query: str, params=None):
        params = params or ()
        normalized = " ".join(query.split())

        if normalized.startswith("INSERT INTO delivery_claims(") and "ON CONFLICT (tg_user_id, event_key) DO UPDATE SET" in normalized:
            stale_cutoff = int(params[13])
            payload = {
                "tg_user_id": params[0],
                "item_id": params[1],
                "kinozal_id": params[2],
                "source_uid": params[3],
                "version_signature": params[4],
                "event_type": params[5],
                "event_key": params[6],
                "subscription_id": params[7],
                "matched_subscription_ids": params[8],
                "delivery_context": params[9],
                "delivery_audit_json": params[10],
                "status": "sending",
                "last_error": "",
                "claimed_at": params[11],
                "updated_at": params[12],
                "sent_at": None,
            }
            claim_row = self.claim_rows.get(str(payload["event_key"]) or "legacy")
            if claim_row is None:
                self.claim_rows[str(payload["event_key"]) or "legacy"] = payload
                return _FakeCursor({"tg_user_id": params[0]})
            is_active = str(claim_row.get("status") or "") == "sent" or (
                str(claim_row.get("status") or "") == "sending"
                and int(claim_row.get("updated_at") or claim_row.get("claimed_at") or 0) > stale_cutoff
            )
            if is_active:
                return _FakeCursor(None)
            payload["last_error"] = (
                "stale_claim_reclaimed"
                if str(claim_row.get("status") or "") == "sending"
                and int(claim_row.get("updated_at") or claim_row.get("claimed_at") or 0) <= stale_cutoff
                else ""
            )
            self.claim_rows[str(payload["event_key"]) or "legacy"] = payload
            return _FakeCursor({"tg_user_id": params[0]})

        if "SELECT 1 FROM (" in normalized and "FROM delivery_claims dc" in normalized:
            cutoff = int(params[0])
            is_active = any(
                str(claim_row.get("status") or "") == "sent"
                or (
                    str(claim_row.get("status") or "") == "sending"
                    and int(claim_row.get("updated_at") or claim_row.get("claimed_at") or 0) > cutoff
                )
                for claim_row in self.claim_rows.values()
            )
            return _FakeCursor({"exists": 1} if is_active else None)

        if normalized == "SELECT 1 FROM items WHERE id = ? LIMIT 1":
            return _FakeCursor({"exists": 1} if self.live_item_exists else None)

        if normalized.startswith("SELECT * FROM items_archive WHERE original_item_id = ?"):
            item_id = int(params[0])
            return _FakeCursor(
                {
                    "original_item_id": item_id,
                    "kinozal_id": "2128422",
                    "source_uid": "kinozal:2128422",
                    "media_type": "movie",
                    "version_signature": "v1",
                    "source_title": "Archived title",
                    "merged_into_item_id": None,
                }
            )

        if normalized.startswith("INSERT INTO deliveries("):
            self.deliveries.append(tuple(params))
            return _FakeCursor()

        if normalized.startswith("INSERT INTO deliveries_archive("):
            self.archive_deliveries.append(
                {
                    "tg_user_id": params[1],
                    "original_item_id": params[2],
                    "delivery_audit_json": params[10],
                }
            )
            return _FakeCursor()

        if normalized.startswith("SELECT delivery_audit_json FROM deliveries WHERE tg_user_id = ? AND item_id = ? UNION ALL SELECT delivery_audit_json FROM deliveries_archive WHERE tg_user_id = ? AND original_item_id = ?"):
            tg_user_id = int(params[0])
            item_id = int(params[1])
            rows = [
                {"delivery_audit_json": row["delivery_audit_json"]}
                for row in self.archive_deliveries
                if int(row.get("tg_user_id") or 0) == tg_user_id and int(row.get("original_item_id") or 0) == item_id
            ]
            return _FakeCursorList(rows)

        if normalized.startswith("UPDATE delivery_claims SET status = 'sent'"):
            sent_at = params[0]
            updated_at = params[1]
            tg_user_id = params[2]
            event_key = params[4]
            item_id = params[6]
            event_type = params[7]
            for claim_row in self.claim_rows.values():
                if int(claim_row.get("tg_user_id") or tg_user_id) != tg_user_id:
                    continue
                has_key = str(claim_row.get("event_key") or "") != ""
                key_matches = has_key and str(claim_row.get("event_key") or "") == str(event_key or "")
                legacy_matches = (not has_key) and int(claim_row.get("item_id") or 0) == int(item_id) and str(claim_row.get("event_type") or "") == str(event_type or "")
                if key_matches or legacy_matches:
                    claim_row["status"] = "sent"
                    claim_row["sent_at"] = sent_at
                    claim_row["updated_at"] = updated_at
                    claim_row["last_error"] = ""
            return _FakeCursor()

        if normalized.startswith("UPDATE delivery_claims SET status = 'failed'"):
            error = params[0]
            updated_at = params[1]
            tg_user_id = params[2]
            event_key = params[4]
            item_id = params[6]
            for claim_row in self.claim_rows.values():
                if int(claim_row.get("tg_user_id") or tg_user_id) != tg_user_id:
                    continue
                if str(claim_row.get("status") or "") != "sending":
                    continue
                has_key = str(claim_row.get("event_key") or "") != ""
                key_matches = has_key and str(claim_row.get("event_key") or "") == str(event_key or "")
                legacy_matches = (not has_key) and int(claim_row.get("item_id") or 0) == int(item_id)
                if key_matches or legacy_matches:
                    claim_row["status"] = "failed"
                    claim_row["updated_at"] = updated_at
                    claim_row["last_error"] = error
            return _FakeCursor()

        return _FakeCursor()

    def commit(self):
        self.commits += 1


class _FakeDB:
    def __init__(self, conn):
        self.conn = conn
        self.lock = threading.RLock()

    def get_item_any(self, item_id: int):
        return {
            "id": item_id,
            "kinozal_id": "2128422",
            "source_uid": "kinozal:2128422",
            "version_signature": "v1",
        }


class _FakeCursorList:
    def __init__(self, rows):
        self._rows = rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class _StrictPlaceholderConn:
    def __init__(self, rows):
        self.rows = rows
        self.last_query = ""
        self.last_params = ()

    def execute(self, query: str, params=None):
        params = tuple(params or ())
        self.last_query = query
        self.last_params = params
        assert query.count("?") == len(params)
        return _FakeCursorList(self.rows)


def test_delivery_claim_is_active_only_while_lease_is_fresh() -> None:
    now = 10_000
    assert _delivery_claim_is_active({"status": "sent", "claimed_at": 1, "updated_at": 1}, now=now) is True
    assert _delivery_claim_is_active({"status": "sending", "claimed_at": now, "updated_at": now}, now=now) is True
    assert _delivery_claim_is_active(
        {
            "status": "sending",
            "claimed_at": now - DELIVERY_CLAIM_LEASE_SECONDS - 1,
            "updated_at": now - DELIVERY_CLAIM_LEASE_SECONDS - 1,
        },
        now=now,
    ) is False


def test_begin_delivery_claim_reclaims_stale_sending_claim(monkeypatch) -> None:
    now = 20_000
    stale_ts = now - DELIVERY_CLAIM_LEASE_SECONDS - 5
    conn = _FakeConn(
        {
            "event_key": "release:1001:2128422:v1",
            "status": "sending",
            "claimed_at": stale_ts,
            "updated_at": stale_ts,
            "last_error": "",
        }
    )
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    claimed = repository.begin_delivery_claim(
        1001,
        42,
        7,
        [7],
        delivery_audit={"item_snapshot": {"kinozal_id": "2128422"}},
        event_type="release",
        event_key="release:1001:2128422:v1",
    )

    assert claimed is True
    claim_row = conn.claim_rows["release:1001:2128422:v1"]
    assert claim_row["status"] == "sending"
    assert claim_row["claimed_at"] == now
    assert claim_row["updated_at"] == now


def test_delivered_ignores_stale_sending_claim(monkeypatch) -> None:
    now = 30_000
    stale_ts = now - DELIVERY_CLAIM_LEASE_SECONDS - 5
    conn = _FakeConn(
        {
            "event_key": "release:1001:2128422:v1",
            "status": "sending",
            "claimed_at": stale_ts,
            "updated_at": stale_ts,
        }
    )
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    assert repository.delivered(1001, 42) is False


def test_begin_delivery_claim_returns_false_when_active_claim_exists(monkeypatch) -> None:
    now = 40_000
    conn = _FakeConn(
        {
            "event_key": "release:1001:2128422:v1",
            "status": "sending",
            "claimed_at": now,
            "updated_at": now,
        }
    )
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    claimed = repository.begin_delivery_claim(
        1001,
        42,
        7,
        [7],
        delivery_audit={"item_snapshot": {"kinozal_id": "2128422"}},
        event_type="release",
        event_key="release:1001:2128422:v1",
    )

    assert claimed is False


def test_delivered_persisted_ignores_active_sending_claim() -> None:
    repository = DeliveryRepository(_FakeDB(_FakeConn()))

    assert repository.delivered_persisted(1001, 42) is False


def test_begin_delivery_claim_allows_distinct_release_text_event_for_same_item(monkeypatch) -> None:
    now = 50_000
    conn = _FakeConn()
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    claimed_release = repository.begin_delivery_claim(
        1001,
        42,
        7,
        [7],
        delivery_audit={"item_snapshot": {"kinozal_id": "2128422"}},
        context="worker",
        event_type="release",
        event_key="release:1001:2128422:v1",
    )
    conn.claim_rows["release:1001:2128422:v1"]["status"] = "sent"
    claimed_release_text = repository.begin_delivery_claim(
        1001,
        42,
        7,
        [7],
        delivery_audit={"item_snapshot": {"kinozal_id": "2128422"}},
        context="release_text_update",
        event_type="release_text",
        event_key="release_text:1001:2128422:abc",
    )

    assert claimed_release is True
    assert claimed_release_text is True


def test_mark_delivery_claim_failed_targets_only_matching_event_key(monkeypatch) -> None:
    now = 60_000
    conn = _FakeConn()
    conn.claim_rows = {
        "release:1001:2128422:v1": {
            "tg_user_id": 1001,
            "item_id": 42,
            "event_type": "release",
            "event_key": "release:1001:2128422:v1",
            "status": "sending",
            "claimed_at": now,
            "updated_at": now,
        },
        "release_text:1001:2128422:abc": {
            "tg_user_id": 1001,
            "item_id": 42,
            "event_type": "release_text",
            "event_key": "release_text:1001:2128422:abc",
            "status": "sending",
            "claimed_at": now,
            "updated_at": now,
        },
    }
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now + 1)

    repository.mark_delivery_claim_failed(1001, 42, error="boom", event_key="release_text:1001:2128422:abc")

    assert conn.claim_rows["release:1001:2128422:v1"]["status"] == "sending"
    assert conn.claim_rows["release_text:1001:2128422:abc"]["status"] == "failed"


def test_record_delivery_marks_only_matching_event_key(monkeypatch) -> None:
    now = 70_000
    conn = _FakeConn()
    conn.claim_rows = {
        "release:1001:2128422:v1": {
            "tg_user_id": 1001,
            "item_id": 42,
            "event_type": "release",
            "event_key": "release:1001:2128422:v1",
            "status": "sending",
            "claimed_at": now,
            "updated_at": now,
        },
        "release_text:1001:2128422:abc": {
            "tg_user_id": 1001,
            "item_id": 42,
            "event_type": "release_text",
            "event_key": "release_text:1001:2128422:abc",
            "status": "sending",
            "claimed_at": now,
            "updated_at": now,
        },
    }
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now + 1)

    repository.record_delivery(
        1001,
        42,
        7,
        [7],
        delivery_audit={"event_type": "release", "event_key": "release:1001:2128422:v1"},
        event_type="release",
        event_key="release:1001:2128422:v1",
    )

    assert conn.claim_rows["release:1001:2128422:v1"]["status"] == "sent"
    assert conn.claim_rows["release_text:1001:2128422:abc"]["status"] == "sending"


def test_record_delivery_marks_only_matching_event_key_as_sent(monkeypatch) -> None:
    now = 60_000
    conn = _FakeConn()
    conn.claim_rows["release:1001:2128422:v1"] = {
        "tg_user_id": 1001,
        "item_id": 42,
        "event_type": "release",
        "event_key": "release:1001:2128422:v1",
        "status": "sending",
    }
    conn.claim_rows["release_text:1001:2128422:abc"] = {
        "tg_user_id": 1001,
        "item_id": 42,
        "event_type": "release_text",
        "event_key": "release_text:1001:2128422:abc",
        "status": "sending",
    }
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    repository.record_delivery(
        1001,
        42,
        7,
        [7],
        delivery_audit={"event_type": "release", "event_key": "release:1001:2128422:v1"},
        event_type="release",
        event_key="release:1001:2128422:v1",
    )

    assert conn.claim_rows["release:1001:2128422:v1"]["status"] == "sent"
    assert conn.claim_rows["release_text:1001:2128422:abc"]["status"] == "sending"


def test_mark_delivery_claim_failed_marks_only_matching_event_key(monkeypatch) -> None:
    now = 70_000
    conn = _FakeConn()
    conn.claim_rows["grouped:1001:tmdb:77:aaa"] = {
        "tg_user_id": 1001,
        "item_id": 42,
        "event_type": "grouped",
        "event_key": "grouped:1001:tmdb:77:aaa",
        "status": "sending",
    }
    conn.claim_rows["release:1001:2128422:v1"] = {
        "tg_user_id": 1001,
        "item_id": 42,
        "event_type": "release",
        "event_key": "release:1001:2128422:v1",
        "status": "sending",
    }
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    repository.mark_delivery_claim_failed(
        1001,
        42,
        error="group send failed",
        event_key="grouped:1001:tmdb:77:aaa",
    )

    assert conn.claim_rows["grouped:1001:tmdb:77:aaa"]["status"] == "failed"
    assert conn.claim_rows["release:1001:2128422:v1"]["status"] == "sending"


def test_delivery_event_persisted_checks_event_key_not_item_only() -> None:
    conn = _FakeConn(live_item_exists=False)
    conn.archive_deliveries = [
        {
            "tg_user_id": 1001,
            "original_item_id": 42,
            "delivery_audit_json": '{"event_type":"release","event_key":"release:1001:2128422:v1"}',
        }
    ]
    repository = DeliveryRepository(_FakeDB(conn))

    assert repository.delivery_event_persisted(1001, 42, event_type="release", event_key="release:1001:2128422:v1") is True
    assert repository.delivery_event_persisted(1001, 42, event_type="release_text", event_key="release_text:1001:2128422:abc") is False


def test_record_delivery_archived_path_marks_claim_sent_for_new_event_type(monkeypatch) -> None:
    now = 80_000
    conn = _FakeConn(live_item_exists=False)
    conn.archive_deliveries = [
        {
            "tg_user_id": 1001,
            "original_item_id": 42,
            "delivery_audit_json": '{"event_type":"release","event_key":"release:1001:2128422:v1"}',
        }
    ]
    conn.claim_rows["release_text:1001:2128422:abc"] = {
        "tg_user_id": 1001,
        "item_id": 42,
        "event_type": "release_text",
        "event_key": "release_text:1001:2128422:abc",
        "status": "sending",
        "claimed_at": now,
        "updated_at": now,
    }
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now + 1)

    repository.record_delivery(
        1001,
        42,
        7,
        [7],
        delivery_audit={"event_type": "release_text", "event_key": "release_text:1001:2128422:abc"},
        event_type="release_text",
        event_key="release_text:1001:2128422:abc",
    )

    assert conn.claim_rows["release_text:1001:2128422:abc"]["status"] == "sent"
    assert len(conn.archive_deliveries) == 2


def test_list_recent_delivery_users_applies_exclusion_placeholders(monkeypatch) -> None:
    conn = _StrictPlaceholderConn([{"tg_user_id": 1003}, {"tg_user_id": 1001}])
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: 90_000)

    users = repository.list_recent_delivery_users_for_kinozal_id(
        "2128422",
        limit=25,
        exclude_user_ids=[1002, 1004],
    )

    assert users == [1003, 1001]
    assert "WHERE tg_user_id NOT IN (?, ?)" in conn.last_query
    assert conn.last_params == (
        "2128422",
        "2128422",
        "2128422",
        90_000 - DELIVERY_CLAIM_LEASE_SECONDS,
        1002,
        1004,
        25,
    )
