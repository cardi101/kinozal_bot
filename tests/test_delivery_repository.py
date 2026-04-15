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
    def __init__(self, claim_row=None):
        self.claim_row = dict(claim_row) if claim_row else None
        self.commits = 0

    def execute(self, query: str, params=None):
        params = params or ()
        normalized = " ".join(query.split())

        if normalized.startswith("INSERT INTO delivery_claims(") and "ON CONFLICT (tg_user_id, item_id) DO UPDATE SET" in normalized:
            stale_cutoff = int(params[11])
            payload = {
                "tg_user_id": params[0],
                "item_id": params[1],
                "kinozal_id": params[2],
                "source_uid": params[3],
                "version_signature": params[4],
                "subscription_id": params[5],
                "matched_subscription_ids": params[6],
                "delivery_context": params[7],
                "delivery_audit_json": params[8],
                "status": "sending",
                "last_error": "",
                "claimed_at": params[9],
                "updated_at": params[10],
                "sent_at": None,
            }
            if self.claim_row is None:
                self.claim_row = payload
                return _FakeCursor({"tg_user_id": params[0]})
            is_active = str(self.claim_row.get("status") or "") == "sent" or (
                str(self.claim_row.get("status") or "") == "sending"
                and int(self.claim_row.get("updated_at") or self.claim_row.get("claimed_at") or 0) > stale_cutoff
            )
            if is_active:
                return _FakeCursor(None)
            payload["last_error"] = (
                "stale_claim_reclaimed"
                if str(self.claim_row.get("status") or "") == "sending"
                and int(self.claim_row.get("updated_at") or self.claim_row.get("claimed_at") or 0) <= stale_cutoff
                else ""
            )
            self.claim_row = payload
            return _FakeCursor({"tg_user_id": params[0]})

        if "SELECT 1 FROM (" in normalized and "FROM delivery_claims dc" in normalized:
            cutoff = int(params[0])
            is_active = bool(self.claim_row) and (
                str(self.claim_row.get("status") or "") == "sent"
                or (
                    str(self.claim_row.get("status") or "") == "sending"
                    and int(self.claim_row.get("updated_at") or self.claim_row.get("claimed_at") or 0) > cutoff
                )
            )
            return _FakeCursor({"exists": 1} if is_active else None)

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
            "status": "sending",
            "claimed_at": stale_ts,
            "updated_at": stale_ts,
            "last_error": "",
        }
    )
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    claimed = repository.begin_delivery_claim(1001, 42, 7, [7], delivery_audit={"item_snapshot": {"kinozal_id": "2128422"}})

    assert claimed is True
    assert conn.claim_row["status"] == "sending"
    assert conn.claim_row["claimed_at"] == now
    assert conn.claim_row["updated_at"] == now


def test_delivered_ignores_stale_sending_claim(monkeypatch) -> None:
    now = 30_000
    stale_ts = now - DELIVERY_CLAIM_LEASE_SECONDS - 5
    conn = _FakeConn(
        {
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
            "status": "sending",
            "claimed_at": now,
            "updated_at": now,
        }
    )
    repository = DeliveryRepository(_FakeDB(conn))
    monkeypatch.setattr(delivery_repo_module, "utc_ts", lambda: now)

    claimed = repository.begin_delivery_claim(1001, 42, 7, [7], delivery_audit={"item_snapshot": {"kinozal_id": "2128422"}})

    assert claimed is False


def test_delivered_persisted_ignores_active_sending_claim() -> None:
    repository = DeliveryRepository(_FakeDB(_FakeConn()))

    assert repository.delivered_persisted(1001, 42) is False
