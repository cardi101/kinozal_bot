import random
import string
from typing import Any, Dict, List, Optional

from config import ACCESS_EXPIRY_UNSET, CFG
from utils import utc_ts

from .base import BaseRepository


class UsersRepository(BaseRepository):
    def ensure_user(
        self,
        tg_user_id: int,
        username: str,
        first_name: str,
        auto_grant: bool = False,
    ) -> Dict[str, Any]:
        ts = utc_ts()
        with self.lock:
            existing = self.conn.execute(
                "SELECT * FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            if existing:
                access_granted = int(existing["access_granted"])
                if auto_grant and not access_granted:
                    access_granted = 1
                self.conn.execute(
                    """
                    UPDATE users
                    SET username = ?, first_name = ?, access_granted = ?, updated_at = ?
                    WHERE tg_user_id = ?
                    """,
                    (username, first_name, access_granted, ts, tg_user_id),
                )
            else:
                self.conn.execute(
                    """
                    INSERT INTO users(tg_user_id, username, first_name, access_granted, access_expires_at, created_at, updated_at)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (tg_user_id, username, first_name, 1 if auto_grant else 0, None, ts, ts),
                )
            self.conn.commit()
            return self.db.get_user(tg_user_id)

    def get_user(self, tg_user_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            return self.row_to_dict(row)

    def user_has_access(self, tg_user_id: int) -> bool:
        if tg_user_id in CFG.admin_ids:
            return True
        user = self.db.get_user(tg_user_id)
        if not user or not user["is_active"] or not user["access_granted"]:
            return False
        access_expires_at = user.get("access_expires_at")
        if access_expires_at is not None and int(access_expires_at) <= utc_ts():
            return False
        return True

    def set_user_access(
        self,
        tg_user_id: int,
        value: bool,
        access_expires_at: Any = ACCESS_EXPIRY_UNSET,
    ) -> None:
        ts = utc_ts()
        with self.lock:
            assignments = ["access_granted = ?", "updated_at = ?"]
            params: List[Any] = [1 if value else 0, ts]
            if access_expires_at is not ACCESS_EXPIRY_UNSET:
                assignments.append("access_expires_at = ?")
                params.append(int(access_expires_at) if access_expires_at is not None else None)
            params.append(tg_user_id)
            self.conn.execute(
                f"UPDATE users SET {', '.join(assignments)} WHERE tg_user_id = ?",
                tuple(params),
            )
            self.conn.commit()

    def extend_user_access_days(self, tg_user_id: int, days: int) -> Optional[Dict[str, Any]]:
        days = int(days)
        if days <= 0:
            return self.db.get_user(tg_user_id)
        user = self.db.get_user(tg_user_id)
        base_ts = utc_ts()
        if user and user.get("access_expires_at") and int(user["access_expires_at"]) > base_ts:
            base_ts = int(user["access_expires_at"])
        expires_at = base_ts + days * 86400
        self.db.set_user_access(tg_user_id, True, access_expires_at=expires_at)
        return self.db.get_user(tg_user_id)

    def list_users_with_stats(self, limit: int = 25, offset: int = 0) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    u.*,
                    COUNT(s.id) AS subscriptions_total,
                    SUM(CASE WHEN s.is_enabled = 1 THEN 1 ELSE 0 END) AS subscriptions_enabled
                FROM users u
                LEFT JOIN subscriptions s ON s.tg_user_id = u.tg_user_id
                GROUP BY u.tg_user_id, u.username, u.first_name, u.is_active, u.access_granted, u.access_expires_at, u.created_at, u.updated_at
                ORDER BY u.created_at DESC, u.tg_user_id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            return [dict(x) for x in rows]

    def count_users(self) -> int:
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()
            return int((row or {}).get("cnt") or 0)

    def list_broadcast_user_ids(
        self,
        active_only: bool = True,
        include_admins: bool = False,
    ) -> List[int]:
        conditions = []
        params: List[Any] = []
        if active_only:
            conditions.extend([
                "is_active = 1",
                "access_granted = 1",
                "(access_expires_at IS NULL OR access_expires_at > ?)",
            ])
            params.append(utc_ts())
        where_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
        with self.lock:
            rows = self.conn.execute(
                f"SELECT tg_user_id FROM users{where_sql} ORDER BY tg_user_id ASC",
                tuple(params),
            ).fetchall()
        user_ids = [int(row["tg_user_id"]) for row in rows]
        if include_admins:
            merged = {int(x) for x in user_ids}
            merged.update(int(x) for x in CFG.admin_ids)
            return sorted(merged)
        admin_ids = {int(x) for x in CFG.admin_ids}
        return [user_id for user_id in user_ids if user_id not in admin_ids]

    def get_user_with_subscriptions(self, tg_user_id: int) -> Optional[Dict[str, Any]]:
        user = self.db.get_user(tg_user_id)
        if not user:
            return None
        user["subscriptions"] = self.db.list_user_subscriptions(tg_user_id)
        return user

    def create_invite(self, created_by: int, uses_left: int, expires_days: int, note: str) -> Dict[str, Any]:
        code = "".join(random.choices(string.ascii_letters + string.digits, k=24))
        expires_at = utc_ts() + expires_days * 86400 if expires_days > 0 else None
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO invites(code, uses_left, expires_at, note, created_by, created_at)
                VALUES(?, ?, ?, ?, ?, ?)
                """,
                (code, uses_left, expires_at, note, created_by, ts),
            )
            self.conn.commit()
        return self.db.get_invite(code)

    def get_invite(self, code: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute("SELECT * FROM invites WHERE code = ?", (code,)).fetchone()
            return self.row_to_dict(row)

    def redeem_invite(self, code: str, tg_user_id: int) -> bool:
        ts = utc_ts()
        with self.lock:
            invite = self.conn.execute("SELECT * FROM invites WHERE code = ?", (code,)).fetchone()
            if not invite:
                return False
            if invite["expires_at"] and ts > int(invite["expires_at"]):
                return False
            if int(invite["uses_left"]) <= 0:
                return False
            self.conn.execute(
                "UPDATE invites SET uses_left = uses_left - 1 WHERE code = ?",
                (code,),
            )
            self.conn.execute(
                """
                UPDATE users
                SET access_granted = 1, access_expires_at = NULL, updated_at = ?
                WHERE tg_user_id = ?
                """,
                (ts, tg_user_id),
            )
            self.conn.commit()
            return True

    def list_invites(self, limit: int = 20) -> List[Dict[str, Any]]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT * FROM invites
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
            return [dict(x) for x in rows]

    def set_user_quiet_hours(self, tg_user_id: int, start_hour: Any, end_hour: Any) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                "UPDATE users SET quiet_start_hour = ?, quiet_end_hour = ?, updated_at = ? WHERE tg_user_id = ?",
                (start_hour, end_hour, ts, tg_user_id),
            )
            self.conn.commit()

    def get_user_quiet_hours(self, tg_user_id: int) -> tuple[Optional[int], Optional[int]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT quiet_start_hour, quiet_end_hour FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            if not row:
                return (None, None)
            return (row["quiet_start_hour"], row["quiet_end_hour"])

    def set_user_quiet_timezone(self, tg_user_id: int, timezone_name: Any) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                "UPDATE users SET quiet_timezone = ?, updated_at = ? WHERE tg_user_id = ?",
                (str(timezone_name or ""), ts, tg_user_id),
            )
            self.conn.commit()

    def get_user_quiet_profile(self, tg_user_id: int) -> tuple[Optional[int], Optional[int], str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT quiet_start_hour, quiet_end_hour, quiet_timezone FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            if not row:
                return (None, None, "")
            return (row["quiet_start_hour"], row["quiet_end_hour"], str(row.get("quiet_timezone") or ""))
