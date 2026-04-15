from typing import Any, Dict, List, Optional

from utils import compact_spaces, utc_ts

from .base import BaseRepository


class MatchReviewRepository(BaseRepository):
    def queue_match_review(self, item_id: int, kinozal_id: str, reason: str = "") -> None:
        ts = utc_ts()
        with self.lock:
            row = self.conn.execute(
                "SELECT status, notified_at FROM match_review_queue WHERE item_id = ?",
                (int(item_id),),
            ).fetchone()
            if row:
                status = compact_spaces(str(row.get("status") or "")).lower() or "pending"
                notified_at = row.get("notified_at")
                if status == "pending":
                    self.conn.execute(
                        """
                        UPDATE match_review_queue
                        SET kinozal_id = ?, reason = ?, updated_at = ?, notified_at = ?
                        WHERE item_id = ?
                        """,
                        (compact_spaces(kinozal_id), compact_spaces(reason), ts, notified_at, int(item_id)),
                    )
                    self.conn.commit()
                return

            self.conn.execute(
                """
                INSERT INTO match_review_queue(
                    item_id, kinozal_id, status, reason, created_at, updated_at
                )
                VALUES(?, ?, 'pending', ?, ?, ?)
                """,
                (int(item_id), compact_spaces(kinozal_id), compact_spaces(reason), ts, ts),
            )
            self.conn.commit()

    def get_pending_match_review_by_item_id(self, item_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT *
                FROM match_review_queue
                WHERE item_id = ? AND status = 'pending'
                LIMIT 1
                """,
                (int(item_id),),
            ).fetchone()
            return dict(row) if row else None

    def get_pending_match_review(self, kinozal_id: str) -> Optional[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return None
        with self.lock:
            row = self.conn.execute(
                """
                SELECT *
                FROM match_review_queue
                WHERE kinozal_id = ? AND status = 'pending'
                ORDER BY created_at DESC, item_id DESC
                LIMIT 1
                """,
                (kinozal_id,),
            ).fetchone()
            return dict(row) if row else None

    def list_pending_match_reviews(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT *
                FROM match_review_queue
                WHERE status = 'pending'
                ORDER BY created_at DESC, item_id DESC
                LIMIT ?
                """,
                (max(1, min(int(limit or 20), 100)),),
            ).fetchall()
            return [dict(row) for row in rows]

    def mark_match_review_notified(self, item_id: int) -> None:
        with self.lock:
            self.conn.execute(
                """
                UPDATE match_review_queue
                SET notified_at = ?, updated_at = ?
                WHERE item_id = ? AND status = 'pending'
                """,
                (utc_ts(), utc_ts(), int(item_id)),
            )
            self.conn.commit()

    def resolve_match_review(
        self,
        item_id: int,
        status: str,
        admin_user_id: int,
        note: str = "",
    ) -> None:
        status_norm = compact_spaces(str(status or "")).lower()
        if status_norm not in {"approved", "rejected", "overridden", "no_match", "forced"}:
            raise ValueError(f"Unsupported review status: {status}")
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                UPDATE match_review_queue
                SET status = ?, updated_at = ?, decided_at = ?, decision_by = ?, decision_note = ?
                WHERE item_id = ?
                """,
                (status_norm, ts, ts, int(admin_user_id), compact_spaces(note), int(item_id)),
            )
            self.conn.commit()

    def set_match_override(self, kinozal_id: str, tmdb_id: int, media_type: str, source: str = "admin") -> None:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        media_type = compact_spaces(str(media_type or "")).lower()
        if not kinozal_id or not media_type:
            return
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO match_overrides(kinozal_id, tmdb_id, media_type, source, created_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(kinozal_id)
                DO UPDATE SET tmdb_id = EXCLUDED.tmdb_id,
                              media_type = EXCLUDED.media_type,
                              source = EXCLUDED.source,
                              updated_at = EXCLUDED.updated_at
                """,
                (kinozal_id, int(tmdb_id), media_type, compact_spaces(source) or "admin", ts, ts),
            )
            self.conn.commit()

    def get_match_override(self, kinozal_id: str) -> Optional[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return None
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM match_overrides WHERE kinozal_id = ? LIMIT 1",
                (kinozal_id,),
            ).fetchone()
            return dict(row) if row else None

    def delete_match_override(self, kinozal_id: str) -> None:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return
        with self.lock:
            self.conn.execute(
                "DELETE FROM match_overrides WHERE kinozal_id = ?",
                (kinozal_id,),
            )
            self.conn.commit()

    def add_match_rejection(self, kinozal_id: str, tmdb_id: int, note: str = "") -> None:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO match_rejections(kinozal_id, tmdb_id, created_at, note)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(kinozal_id, tmdb_id) DO NOTHING
                """,
                (kinozal_id, int(tmdb_id), utc_ts(), compact_spaces(note)),
            )
            self.conn.commit()

    def is_match_rejected(self, kinozal_id: str, tmdb_id: int) -> bool:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return False
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1
                FROM match_rejections
                WHERE kinozal_id = ? AND tmdb_id = ?
                LIMIT 1
                """,
                (kinozal_id, int(tmdb_id)),
            ).fetchone()
            return row is not None
