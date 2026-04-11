import time
from typing import Any

from config import CFG
from db import DB


def _assert(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def _cleanup(db: DB, tg_user_id: int, source_uid: str, meta_key: str) -> None:
    with db.lock:
        item_rows = db.conn.execute(
            "SELECT id FROM items WHERE source_uid = ?",
            (source_uid,),
        ).fetchall()
        item_ids = [int(row["id"]) for row in item_rows]

        db.conn.execute("DELETE FROM pending_deliveries WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM debounce_queue WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM muted_titles WHERE tg_user_id = ?", (tg_user_id,))

        if item_ids:
            db.conn.executemany(
                "DELETE FROM deliveries WHERE item_id = ?",
                [(item_id,) for item_id in item_ids],
            )
            db.conn.executemany(
                "DELETE FROM item_genres WHERE item_id = ?",
                [(item_id,) for item_id in item_ids],
            )

        db.conn.execute("DELETE FROM deliveries WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM subscriptions WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM users WHERE tg_user_id = ?", (tg_user_id,))
        db.conn.execute("DELETE FROM items WHERE source_uid = ?", (source_uid,))
        db.conn.execute("DELETE FROM meta WHERE key = ?", (meta_key,))
        db.conn.commit()


def main() -> None:
    stamp = int(time.time() * 1000)
    tg_user_id = -stamp
    kinozal_id = f"99{stamp}"
    source_uid = f"kinozal:{kinozal_id}"
    meta_key = f"smoke.meta.{stamp}"

    db = DB(CFG.database_url)
    try:
        _cleanup(db, tg_user_id, source_uid, meta_key)

        user = db.ensure_user(
            tg_user_id=tg_user_id,
            username="smoke_user",
            first_name="Smoke",
            auto_grant=True,
        )
        _assert(user is not None, "ensure_user returned no user")
        _assert(int(user["tg_user_id"]) == tg_user_id, "user tg_user_id mismatch")
        _assert(db.user_has_access(tg_user_id), "user should have access")

        db.set_user_quiet_hours(tg_user_id, None, None)
        quiet_start, quiet_end = db.get_user_quiet_hours(tg_user_id)
        _assert(quiet_start is None and quiet_end is None, "quiet hours should be cleared")

        subscription = db.create_subscription(tg_user_id, name="Smoke subscription")
        sub_id = int(subscription["id"])
        db.update_subscription(
            sub_id,
            media_type="movie",
            min_tmdb_rating=7.5,
            include_keywords="test, smoke",
            exclude_keywords="camrip",
            content_filter="any",
        )
        db.set_subscription_genres(sub_id, [28, 35])
        db.set_subscription_country_codes(sub_id, ["US", "JP"])
        db.set_subscription_exclude_country_codes(sub_id, ["RU"])
        subscription = db.get_subscription(sub_id)
        _assert(subscription is not None, "subscription not found after create")
        _assert(subscription["media_type"] == "movie", "subscription media_type mismatch")
        _assert(subscription["genre_ids"] == [28, 35], "subscription genres mismatch")
        _assert(subscription["country_codes_list"] == ["US", "JP"], "subscription countries mismatch")
        _assert(subscription["exclude_country_codes_list"] == ["RU"], "subscription exclude countries mismatch")

        item_payload: dict[str, Any] = {
            "source_uid": source_uid,
            "source_title": "Smoke Test Movie / 2026 / EN / WEB-DL 1080p",
            "source_link": f"https://kinozal.tv/details.php?id={kinozal_id}",
            "source_published_at": int(time.time()),
            "source_year": 2026,
            "source_format": "WEB-DL 1080p",
            "source_description": "Repository smoke item",
            "source_episode_progress": "",
            "source_audio_tracks": ["EN"],
            "source_category_id": "movies",
            "source_category_name": "Кино - Зарубежные фильмы",
            "media_type": "movie",
            "imdb_id": "tt1234567",
            "cleaned_title": "Smoke Test Movie",
            "tmdb_id": 777000 + (stamp % 1000),
            "tmdb_title": "Smoke Test Movie",
            "tmdb_original_title": "Smoke Test Movie",
            "tmdb_original_language": "en",
            "tmdb_rating": 8.1,
            "tmdb_vote_count": 1200,
            "tmdb_release_date": "2026-01-01",
            "tmdb_overview": "Smoke repository overview",
            "tmdb_poster_url": "https://example.com/poster.jpg",
            "tmdb_status": "Released",
            "tmdb_age_rating": "PG-13",
            "tmdb_countries": ["US", "JP"],
            "manual_bucket": "",
            "manual_country_codes": [],
            "genre_ids": [28, 35],
            "raw_json": {"smoke": True},
        }

        item_id, is_new, materially_changed = db.save_item(item_payload)
        _assert(is_new, "item should be inserted as new")
        _assert(materially_changed, "new item should be materially changed")

        item = db.get_item(item_id)
        _assert(item is not None, "get_item returned no row")
        _assert(str(item["kinozal_id"]) == kinozal_id, "item kinozal_id mismatch")
        _assert(item["genre_ids"] == [28, 35], "item genres mismatch")
        _assert(item["tmdb_countries"] == ["US", "JP"], "item countries mismatch")

        found_item = db.find_item_by_kinozal_id(kinozal_id)
        _assert(found_item is not None, "find_item_by_kinozal_id returned no row")
        _assert(int(found_item["id"]) == item_id, "find_item_by_kinozal_id returned wrong item")

        db.set_meta(meta_key, "ok")
        _assert(db.get_meta(meta_key) == "ok", "meta roundtrip failed")

        db.record_delivery(tg_user_id, item_id, sub_id, [sub_id])
        _assert(db.delivered(tg_user_id, item_id), "delivery was not recorded")
        _assert(db.recently_delivered(tg_user_id, item_id, 300), "recently_delivered should be true")
        _assert(db.delivered_equivalent(tg_user_id, item), "delivered_equivalent should be true")
        history = db.get_user_delivery_history(tg_user_id, limit=5)
        _assert(history and int(history[0]["id"]) == item_id, "delivery history missing item")

        db.upsert_debounce(tg_user_id, kinozal_id, item_id, str(sub_id), delay_seconds=0)
        due_debounce = db.pop_due_debounce()
        _assert(len(due_debounce) == 1, "due debounce should contain one row")
        _assert(int(due_debounce[0]["item_id"]) == item_id, "debounce item_id mismatch")

        db.queue_pending_delivery(tg_user_id, item_id, str(sub_id), "", False)
        due_pending = db.pop_due_pending_deliveries(current_hour=12)
        _assert(tg_user_id in due_pending, "pending deliveries missing user")
        _assert(int(due_pending[tg_user_id][0]["item_id"]) == item_id, "pending item_id mismatch")
        db.delete_pending_delivery(tg_user_id, item_id)

        db.mute_title(tg_user_id, int(item["tmdb_id"]))
        _assert(db.is_title_muted(tg_user_id, int(item["tmdb_id"])), "title should be muted")
        db.unmute_title(tg_user_id, int(item["tmdb_id"]))
        _assert(not db.is_title_muted(tg_user_id, int(item["tmdb_id"])), "title should be unmuted")

        print("repository smoke ok")
    finally:
        try:
            _cleanup(db, tg_user_id, source_uid, meta_key)
        finally:
            db.close()


if __name__ == "__main__":
    main()
