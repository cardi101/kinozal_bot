from typing import Any, Dict, Iterable, List, Optional

from country_helpers import parse_country_codes
from subscription_presets import detect_subscription_preset_key, subscription_presets
from utils import compact_spaces, utc_ts

from .base import BaseRepository


class SubscriptionsRepository(BaseRepository):
    def create_subscription(self, tg_user_id: int, name: Optional[str] = None) -> Dict[str, Any]:
        ts = utc_ts()
        if not name:
            count = len(self.db.list_user_subscriptions(tg_user_id)) + 1
            name = f"Подписка {count}"
        with self.lock:
            row = self.conn.execute(
                """
                INSERT INTO subscriptions(
                    tg_user_id, name, media_type, allow_1080, is_enabled, created_at, updated_at
                )
                VALUES(?, ?, 'any', 1, 1, ?, ?)
                RETURNING id
                """,
                (tg_user_id, name, ts, ts),
            ).fetchone()
            sub_id = int(row["id"])
            self.conn.commit()
        return self.db.get_subscription(sub_id)

    def list_user_subscriptions(self, tg_user_id: int) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM subscriptions
                WHERE tg_user_id = ?
                ORDER BY id DESC
                """,
                (tg_user_id,),
            ).fetchall()
            return [dict(x) for x in rows]

    def list_enabled_subscriptions(self) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT s.*
                FROM subscriptions s
                JOIN users u ON u.tg_user_id = s.tg_user_id
                WHERE s.is_enabled = 1
                  AND u.is_active = 1
                  AND u.access_granted = 1
                  AND (u.access_expires_at IS NULL OR u.access_expires_at > ?)
                ORDER BY s.id ASC
                """,
                (utc_ts(),),
            ).fetchall()
            return [dict(x) for x in rows]

    def rollout_existing_preset_subscriptions(self, rollout_version: str) -> int:
        rollout_version = compact_spaces(str(rollout_version or ""))
        if not rollout_version:
            return 0
        meta_key = "preset_rollout_version"
        if self.db.get_meta(meta_key) == rollout_version:
            return 0

        updated = 0
        rows = self.conn.execute("SELECT * FROM subscriptions ORDER BY id ASC").fetchall()
        for row in rows:
            sub = dict(row)
            preset_key = detect_subscription_preset_key(sub)
            spec = subscription_presets().get(preset_key) if preset_key else None
            if not spec:
                continue
            fields = dict(spec["fields"])
            fields["name"] = spec["name"]
            fields["preset_key"] = preset_key
            self.db.update_subscription(int(sub["id"]), **fields)
            self.db.set_subscription_genres(int(sub["id"]), spec.get("genre_ids", []))
            updated += 1

        self.db.set_meta(meta_key, rollout_version)
        return updated

    def get_subscription(self, sub_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            data["genre_ids"] = self.db.get_subscription_genres(sub_id)
            data["country_codes_list"] = self.db.get_subscription_country_codes(sub_id)
            data["exclude_country_codes_list"] = self.db.get_subscription_exclude_country_codes(sub_id)
            return data

    def subscription_belongs_to(self, sub_id: int, tg_user_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ? AND tg_user_id = ?",
                (sub_id, tg_user_id),
            ).fetchone()
            return row is not None

    def update_subscription(self, sub_id: int, **fields: Any) -> None:
        if not fields:
            return
        allowed = {
            "name",
            "media_type",
            "year_from",
            "year_to",
            "allow_720",
            "allow_1080",
            "allow_2160",
            "min_tmdb_rating",
            "include_keywords",
            "exclude_keywords",
            "content_filter",
            "country_codes",
            "exclude_country_codes",
            "preset_key",
            "is_enabled",
        }
        cleaned = {k: v for k, v in fields.items() if k in allowed}
        if not cleaned:
            return
        cleaned["updated_at"] = utc_ts()
        assignments = ", ".join(f"{k} = ?" for k in cleaned.keys())
        values = list(cleaned.values()) + [sub_id]
        with self.lock:
            self.conn.execute(
                f"UPDATE subscriptions SET {assignments} WHERE id = ?",
                values,
            )
            self.conn.commit()

    def delete_subscription(self, sub_id: int) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
            self.conn.commit()

    def get_subscription_genres(self, sub_id: int) -> List[int]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT genre_id FROM subscription_genres WHERE subscription_id = ? ORDER BY genre_id",
                (sub_id,),
            ).fetchall()
            return [int(row["genre_id"]) for row in rows]

    def set_subscription_genres(self, sub_id: int, genre_ids: Iterable[int]) -> None:
        normalized_genres = sorted({int(g) for g in genre_ids})
        with self.lock:
            self.conn.execute("DELETE FROM subscription_genres WHERE subscription_id = ?", (sub_id,))
            if normalized_genres:
                self.conn.executemany(
                    "INSERT INTO subscription_genres(subscription_id, genre_id) VALUES(?, ?)",
                    [(sub_id, genre_id) for genre_id in normalized_genres],
                )
            self.conn.commit()

    def toggle_subscription_genre(self, sub_id: int, genre_id: int) -> None:
        current = set(self.db.get_subscription_genres(sub_id))
        if genre_id in current:
            current.remove(genre_id)
        else:
            current.add(genre_id)
        self.db.set_subscription_genres(sub_id, current)

    def get_subscription_country_codes(self, sub_id: int) -> List[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT country_codes FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            return parse_country_codes(row["country_codes"] if row else "")

    def set_subscription_country_codes(self, sub_id: int, country_codes: Iterable[str]) -> None:
        normalized = ",".join(parse_country_codes(list(country_codes)))
        self.db.update_subscription(sub_id, country_codes=normalized)

    def toggle_subscription_country_code(self, sub_id: int, country_code: str) -> None:
        current = set(self.db.get_subscription_country_codes(sub_id))
        code = compact_spaces(str(country_code or "")).upper()
        if not code:
            return
        if code in current:
            current.remove(code)
        else:
            current.add(code)
        self.db.set_subscription_country_codes(sub_id, sorted(current))

    def get_subscription_exclude_country_codes(self, sub_id: int) -> List[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT exclude_country_codes FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            return parse_country_codes(row["exclude_country_codes"] if row else "")

    def set_subscription_exclude_country_codes(self, sub_id: int, country_codes: Iterable[str]) -> None:
        normalized = ",".join(parse_country_codes(list(country_codes)))
        self.db.update_subscription(sub_id, exclude_country_codes=normalized)

    def toggle_subscription_exclude_country_code(self, sub_id: int, country_code: str) -> None:
        current = set(self.db.get_subscription_exclude_country_codes(sub_id))
        code = compact_spaces(str(country_code or "")).upper()
        if not code:
            return
        if code in current:
            current.remove(code)
        else:
            current.add(code)
        self.db.set_subscription_exclude_country_codes(sub_id, sorted(current))
