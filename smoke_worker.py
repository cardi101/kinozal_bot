import time
from typing import Any

from config import CFG
from db import DB
from domain import ReleaseItem
from services.delivery_service import DeliveryService
from services.subscription_service import SubscriptionService
from services.worker_service import WorkerService


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _cleanup(db: DB, tg_user_id: int, source_uid: str) -> None:
    with db.lock:
        item_rows = db.conn.execute(
            "SELECT id FROM items WHERE source_uid = ?",
            (source_uid,),
        ).fetchall()
        item_ids = [int(row["id"]) for row in item_rows]

        db.conn.execute("DELETE FROM pending_deliveries WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM debounce_queue WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM delivery_claims WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM deliveries WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM subscriptions WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM users WHERE tg_user_id = ?", (tg_user_id,))

        if item_ids:
            db.conn.executemany("DELETE FROM item_genres WHERE item_id = ?", [(item_id,) for item_id in item_ids])
            db.conn.executemany("DELETE FROM deliveries WHERE item_id = ?", [(item_id,) for item_id in item_ids])
            db.conn.executemany("DELETE FROM items WHERE id = ?", [(item_id,) for item_id in item_ids])
        db.conn.commit()


class _FakeKinozalService:
    def __init__(self, source_uid: str, source_title: str) -> None:
        self._item = ReleaseItem.from_payload(
            {
                "source_uid": source_uid,
                "source_title": source_title,
                "source_link": "",
                "source_published_at": int(time.time()),
                "source_description": "Worker smoke item",
                "source_year": 2026,
                "source_format": "1080",
                "source_episode_progress": "",
                "source_audio_tracks": ["ПМ"],
                "source_category_id": "46",
                "source_category_name": "Сериал - Буржуйский",
                "media_type": "movie",
                "raw_json": {"smoke_worker": True},
            }
        )

    async def fetch_latest(self):
        return [self._item.clone()]

    async def enrich_item_with_details(self, item: ReleaseItem, force_refresh: bool = False) -> ReleaseItem:
        del force_refresh
        item.set("details_title", item.source_title)
        item.set("source_release_text", "Первая строка\nВторая строка")
        return item


class _FakeTMDBService:
    async def enrich_item(self, item: ReleaseItem) -> ReleaseItem:
        item.set("cleaned_title", "Smoke Worker Movie")
        item.set("media_type", "movie")
        item.set("tmdb_id", 987654)
        item.set("tmdb_title", "Smoke Worker Movie")
        item.set("tmdb_original_title", "Smoke Worker Movie")
        item.set("tmdb_original_language", "en")
        item.set("tmdb_rating", 8.3)
        item.set("tmdb_vote_count", 1234)
        item.set("tmdb_release_date", "2026-03-01")
        item.set("tmdb_overview", "Worker smoke overview")
        item.set("tmdb_status", "Released")
        item.set("tmdb_countries", ["US"])
        item.set("genre_ids", [28, 18])
        item.set("tmdb_match_path", "smoke_fake")
        item.set("tmdb_match_confidence", "high")
        item.set("tmdb_match_evidence", "smoke fake enrich")
        return item


class _RecordingDeliveryService(DeliveryService):
    def __init__(self, repository: Any) -> None:
        super().__init__(repository, bot=None)
        self.sent: list[tuple[int, int]] = []

    async def send_item(self, tg_user_id: int, item: ReleaseItem, subs, old_release_text: str = "") -> None:
        del subs, old_release_text
        self.sent.append((int(tg_user_id), int(item.id)))


def main() -> None:
    stamp = int(time.time() * 1000)
    tg_user_id = -stamp
    source_uid = f"smoke-worker:{stamp}"
    source_title = "Smoke Worker Movie / 2026 / ПМ / WEB-DL (1080p)"

    db = DB(CFG.database_url)
    try:
        _cleanup(db, tg_user_id, source_uid)

        db.ensure_user(
            tg_user_id=tg_user_id,
            username="worker_smoke",
            first_name="Worker",
            auto_grant=True,
        )
        subscription = db.create_subscription(tg_user_id, name="Worker Smoke")
        db.update_subscription(
            int(subscription["id"]),
            media_type="movie",
            min_tmdb_rating=7.0,
            include_keywords="smoke",
            content_filter="any",
        )
        db.set_meta("bootstrap_done", "1")

        delivery_service = _RecordingDeliveryService(db)
        worker = WorkerService(
            repository=db,
            kinozal_service=_FakeKinozalService(source_uid, source_title),
            tmdb_service=_FakeTMDBService(),
            subscription_service=SubscriptionService(db),
            delivery_service=delivery_service,
            bot=None,
        )

        cycle_metrics = worker._new_cycle_metrics()
        import asyncio

        asyncio.run(worker.process_new_items(cycle_metrics))

        items = db.conn.execute("SELECT id FROM items WHERE source_uid = ?", (source_uid,)).fetchall()
        _assert(len(items) == 1, "worker smoke should persist one item")
        item_id = int(items[0]["id"])

        _assert((tg_user_id, item_id) in delivery_service.sent, "worker smoke should send mocked delivery for smoke user")
        history = db.get_user_delivery_history(tg_user_id, limit=5)
        _assert(history and int(history[0]["id"]) == item_id, "worker smoke delivery history missing item")

        claim = db.conn.execute(
            "SELECT event_type, event_key, status FROM delivery_claims WHERE tg_user_id = ? AND item_id = ? ORDER BY updated_at DESC LIMIT 1",
            (tg_user_id, item_id),
        ).fetchone()
        _assert(claim is not None, "worker smoke delivery claim missing")
        _assert(str(claim["event_type"]) == "release", "worker smoke event_type mismatch")
        _assert(str(claim["status"]) == "sent", "worker smoke claim should be sent")
        _assert(str(claim["event_key"]).startswith(f"release:{tg_user_id}:smoke-worker:"), "worker smoke event_key mismatch")

        _assert(cycle_metrics["items_saved_new_total"] == 1, "worker smoke should insert one new item")
        _assert(cycle_metrics["deliveries_sent_total"] >= 1, "worker smoke should send at least one delivery")

        print("worker smoke ok")
    finally:
        try:
            _cleanup(db, tg_user_id, source_uid)
        finally:
            db.close()


if __name__ == "__main__":
    main()
