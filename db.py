import json
import logging
import random
import string
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from psycopg import connect, OperationalError, InterfaceError
from psycopg.rows import dict_row

from config import CFG, ACCESS_EXPIRY_UNSET
from country_helpers import COUNTRY_NAMES_RU, parse_country_codes, country_name_ru
from release_versioning import extract_kinozal_id, resolve_item_kinozal_id, build_item_variant_signature, format_variant_summary, build_version_signature, get_item_variant_components
from subscription_matching import match_subscription
from subscription_presets import subscription_presets, detect_subscription_preset_key
from utils import utc_ts, compact_spaces

try:
    import pycountry
except Exception:
    pycountry = None


log = logging.getLogger("kinozal-news-bot")


def item_duplicate_quality_score(item: Dict[str, Any]) -> int:
    score = 0
    if item.get("tmdb_id"):
        score += 100
    if compact_spaces(str(item.get("imdb_id") or "")):
        score += 25
    if compact_spaces(str(item.get("tmdb_overview") or "")):
        score += 10
    if compact_spaces(str(item.get("source_description") or "")):
        score += 8
    if compact_spaces(str(item.get("tmdb_poster_url") or "")):
        score += 5
    if parse_country_codes(item.get("tmdb_countries")):
        score += 4
    if compact_spaces(str(item.get("tmdb_status") or "")):
        score += 3
    if compact_spaces(str(item.get("manual_bucket") or "")):
        score += 2
    if int(item.get("tmdb_vote_count") or 0) > 0:
        score += 2
    return score


class DummyCursor:
    def fetchone(self):
        return None

    def fetchall(self):
        return []


class PGCompatConnection:
    def __init__(self, dsn):
        self.dsn = dsn
        self._lock = threading.RLock()
        self.raw = connect(dsn, row_factory=dict_row, autocommit=True)
    def _connect_raw(self) -> None:
        self.raw = connect(self.dsn, row_factory=dict_row, autocommit=True)

    def _close_raw_quietly(self) -> None:
        raw = getattr(self, "raw", None)
        self.raw = None
        if raw is None:
            return
        try:
            raw.close()
        except Exception:
            pass

    def _normalize_sql(self, sql: str) -> str:
        return sql.replace("?", "%s")

    def _ensure_connection(self) -> None:
        raw = getattr(self, "raw", None)
        bad = raw is None
        if not bad:
            try:
                bad = bool(raw.closed) or bool(getattr(raw, "broken", False))
            except Exception:
                bad = True
        if not bad:
            return

        with self._lock:
            raw = getattr(self, "raw", None)
            bad = raw is None
            if not bad:
                try:
                    bad = bool(raw.closed) or bool(getattr(raw, "broken", False))
                except Exception:
                    bad = True
            if bad:
                self._close_raw_quietly()
                self._connect_raw()

    def reconnect(self) -> None:
        with self._lock:
            self._close_raw_quietly()
            self._connect_raw()

    def execute(self, query, params=None):
        last_error = None
        sql = self._normalize_sql(query)
        bind = params or ()

        for _ in range(2):
            try:
                self._ensure_connection()
                cur = self.raw.cursor()
                cur.execute(sql, bind)
                return cur
            except (OperationalError, InterfaceError) as exc:
                last_error = exc
                self.reconnect()
                time.sleep(0.2)

        if last_error is not None:
            raise last_error
        raise RuntimeError("DB execute failed without explicit exception")

    def executemany(self, sql: str, seq: Sequence[Sequence[Any]]):
        last_error = None
        norm_sql = self._normalize_sql(sql)
        for _ in range(2):
            try:
                self._ensure_connection()
                with self.raw.cursor() as cur:
                    cur.executemany(norm_sql, seq)
                return DummyCursor()
            except (OperationalError, InterfaceError) as exc:
                last_error = exc
                self.reconnect()
                time.sleep(0.2)
        if last_error is not None:
            raise last_error
        raise RuntimeError("DB executemany failed without explicit exception")

    def executescript(self, script: str) -> None:
        statements = [x.strip() for x in script.split(";") if x.strip()]
        with self.raw.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)

    def commit(self) -> None:
        return None


class DB:
    def __init__(self, dsn: str):
        self.lock = threading.RLock()
        last_error: Optional[Exception] = None
        for attempt in range(1, CFG.startup_db_retries + 1):
            try:
                self.conn = PGCompatConnection(dsn)
                self.init_schema()
                return
            except Exception as exc:
                last_error = exc
                log.warning("Postgres not ready yet (%s/%s): %s", attempt, CFG.startup_db_retries, exc)
                time.sleep(CFG.startup_db_retry_delay)
        raise RuntimeError(f"Не удалось подключиться к Postgres: {last_error}")

    def init_schema(self) -> None:
        with self.lock:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    tg_user_id BIGINT PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    access_granted INTEGER NOT NULL DEFAULT 0,
                    access_expires_at BIGINT,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS invites (
                    code TEXT PRIMARY KEY,
                    uses_left INTEGER NOT NULL,
                    expires_at BIGINT,
                    note TEXT,
                    created_by BIGINT NOT NULL,
                    created_at BIGINT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL,
                    name TEXT NOT NULL,
                    media_type TEXT NOT NULL DEFAULT 'any',
                    year_from INTEGER,
                    year_to INTEGER,
                    allow_720 INTEGER NOT NULL DEFAULT 0,
                    allow_1080 INTEGER NOT NULL DEFAULT 0,
                    allow_2160 INTEGER NOT NULL DEFAULT 0,
                    min_tmdb_rating DOUBLE PRECISION,
                    include_keywords TEXT NOT NULL DEFAULT '',
                    exclude_keywords TEXT NOT NULL DEFAULT '',
                    content_filter TEXT NOT NULL DEFAULT 'any',
                    country_codes TEXT NOT NULL DEFAULT '',
                    exclude_country_codes TEXT NOT NULL DEFAULT '',
                    preset_key TEXT NOT NULL DEFAULT '',
                    is_enabled INTEGER NOT NULL DEFAULT 1,
                    created_at BIGINT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    CONSTRAINT fk_subscriptions_user FOREIGN KEY(tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS subscription_genres (
                    subscription_id BIGINT NOT NULL,
                    genre_id INTEGER NOT NULL,
                    PRIMARY KEY (subscription_id, genre_id),
                    CONSTRAINT fk_subscription_genres_sub FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS genres (
                    media_type TEXT NOT NULL,
                    genre_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    updated_at BIGINT NOT NULL,
                    PRIMARY KEY (media_type, genre_id)
                );

                CREATE TABLE IF NOT EXISTS items (
                    id BIGSERIAL PRIMARY KEY,
                    source_uid TEXT NOT NULL,
                    version_signature TEXT NOT NULL,
                    source_title TEXT NOT NULL,
                    source_link TEXT,
                    kinozal_id TEXT,
                    source_published_at BIGINT,
                    source_year INTEGER,
                    source_format TEXT,
                    source_description TEXT,
                    source_episode_progress TEXT,
                    source_audio_tracks TEXT,
                    imdb_id TEXT,
                    mal_id TEXT,
                    cleaned_title TEXT,
                    source_category_id TEXT,
                    source_category_name TEXT,
                    media_type TEXT,
                    tmdb_id INTEGER,
                    tmdb_title TEXT,
                    tmdb_original_title TEXT,
                    tmdb_original_language TEXT,
                    tmdb_rating DOUBLE PRECISION,
                    tmdb_vote_count INTEGER,
                    tmdb_release_date TEXT,
                    tmdb_overview TEXT,
                    tmdb_poster_url TEXT,
                    tmdb_status TEXT,
                    tmdb_age_rating TEXT,
                    tmdb_countries TEXT,
                    tmdb_number_of_seasons INTEGER,
                    tmdb_number_of_episodes INTEGER,
                    tmdb_next_episode_name TEXT,
                    tmdb_next_episode_air_date TEXT,
                    tmdb_next_episode_season_number INTEGER,
                    tmdb_next_episode_episode_number INTEGER,
                    tmdb_last_episode_name TEXT,
                    tmdb_last_episode_air_date TEXT,
                    tmdb_last_episode_season_number INTEGER,
                    tmdb_last_episode_episode_number INTEGER,
                    manual_bucket TEXT NOT NULL DEFAULT '',
                    manual_country_codes TEXT NOT NULL DEFAULT '',
                    raw_json JSONB NOT NULL,
                    created_at BIGINT NOT NULL,
                    UNIQUE(source_uid, version_signature)
                );

                CREATE TABLE IF NOT EXISTS items_archive (
                    archive_id BIGSERIAL PRIMARY KEY,
                    original_item_id BIGINT NOT NULL,
                    kinozal_id TEXT,
                    source_uid TEXT NOT NULL,
                    version_signature TEXT NOT NULL,
                    source_title TEXT NOT NULL,
                    source_link TEXT,
                    media_type TEXT,
                    source_published_at BIGINT,
                    source_year INTEGER,
                    source_format TEXT,
                    source_description TEXT,
                    source_episode_progress TEXT,
                    source_audio_tracks TEXT,
                    imdb_id TEXT,
                    cleaned_title TEXT,
                    source_category_id TEXT,
                    source_category_name TEXT,
                    tmdb_id INTEGER,
                    tmdb_title TEXT,
                    tmdb_original_title TEXT,
                    tmdb_original_language TEXT,
                    tmdb_rating DOUBLE PRECISION,
                    tmdb_vote_count INTEGER,
                    tmdb_release_date TEXT,
                    tmdb_status TEXT,
                    tmdb_countries TEXT,
                    manual_bucket TEXT NOT NULL DEFAULT '',
                    manual_country_codes TEXT NOT NULL DEFAULT '',
                    genre_ids TEXT,
                    item_json JSONB NOT NULL,
                    original_created_at BIGINT,
                    archived_at BIGINT NOT NULL,
                    archive_reason TEXT NOT NULL,
                    merged_into_item_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS deliveries_archive (
                    archive_id BIGSERIAL PRIMARY KEY,
                    original_delivery_id BIGINT,
                    tg_user_id BIGINT NOT NULL,
                    original_item_id BIGINT NOT NULL,
                    kinozal_id TEXT,
                    source_uid TEXT,
                    media_type TEXT,
                    version_signature TEXT,
                    source_title TEXT,
                    subscription_id BIGINT,
                    matched_subscription_ids TEXT,
                    delivered_at BIGINT NOT NULL,
                    archived_at BIGINT NOT NULL,
                    archive_reason TEXT NOT NULL,
                    merged_into_item_id BIGINT
                );

                CREATE TABLE IF NOT EXISTS item_genres (
                    item_id BIGINT NOT NULL,
                    genre_id INTEGER NOT NULL,
                    PRIMARY KEY (item_id, genre_id),
                    CONSTRAINT fk_item_genres_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS deliveries (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL,
                    item_id BIGINT NOT NULL,
                    subscription_id BIGINT,
                    delivered_at BIGINT NOT NULL,
                    UNIQUE(tg_user_id, item_id),
                    CONSTRAINT fk_deliveries_user FOREIGN KEY(tg_user_id) REFERENCES users(tg_user_id) ON DELETE CASCADE,
                    CONSTRAINT fk_deliveries_item FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE,
                    CONSTRAINT fk_deliveries_sub FOREIGN KEY(subscription_id) REFERENCES subscriptions(id) ON DELETE SET NULL
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at BIGINT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_subscriptions_user ON subscriptions(tg_user_id);
                CREATE INDEX IF NOT EXISTS idx_items_created_at ON items(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_items_source_uid ON items(source_uid);
                CREATE INDEX IF NOT EXISTS idx_deliveries_user_item ON deliveries(tg_user_id, item_id);

                ALTER TABLE users ADD COLUMN IF NOT EXISTS access_expires_at BIGINT;
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS country_codes TEXT NOT NULL DEFAULT '';
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS exclude_country_codes TEXT NOT NULL DEFAULT '';
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS content_filter TEXT NOT NULL DEFAULT 'any';
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS preset_key TEXT NOT NULL DEFAULT '';
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_countries TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS source_category_id TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS source_category_name TEXT;
                ALTER TABLE items_archive ADD COLUMN IF NOT EXISTS source_category_id TEXT;
                ALTER TABLE items_archive ADD COLUMN IF NOT EXISTS source_category_name TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS kinozal_id TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_original_language TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_vote_count INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_status TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_age_rating TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_number_of_seasons INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_number_of_episodes INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_name TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_air_date TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_season_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_next_episode_episode_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_name TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_air_date TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_season_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS tmdb_last_episode_episode_number INTEGER;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS manual_bucket TEXT NOT NULL DEFAULT '';
                ALTER TABLE items ADD COLUMN IF NOT EXISTS manual_country_codes TEXT NOT NULL DEFAULT '';
                ALTER TABLE deliveries ADD COLUMN IF NOT EXISTS matched_subscription_ids TEXT;
                ALTER TABLE items ADD COLUMN IF NOT EXISTS source_release_text TEXT NOT NULL DEFAULT '';
                ALTER TABLE items ADD COLUMN IF NOT EXISTS mal_id TEXT;
                CREATE INDEX IF NOT EXISTS idx_users_access_state ON users(access_granted, access_expires_at);
                CREATE INDEX IF NOT EXISTS idx_items_source_link ON items(source_link);
                CREATE INDEX IF NOT EXISTS idx_items_media_source ON items(media_type, source_uid);

                UPDATE items
                SET kinozal_id = regexp_replace(source_uid, '^kinozal:([0-9]+)$', '\1')
                WHERE (kinozal_id IS NULL OR kinozal_id = '')
                  AND source_uid ~ '^kinozal:[0-9]+$';

                UPDATE items
                SET kinozal_id = regexp_replace(source_uid, '^.*details\\.php\\?id=([0-9]+).*$','\1')
                WHERE (kinozal_id IS NULL OR kinozal_id = '')
                  AND source_uid ~ 'details\\.php\\?id=[0-9]+';

                UPDATE items
                SET kinozal_id = regexp_replace(source_link, '^.*details\\.php\\?id=([0-9]+).*$','\1')
                WHERE (kinozal_id IS NULL OR kinozal_id = '')
                  AND COALESCE(source_link, '') ~ 'details\\.php\\?id=[0-9]+';

                CREATE INDEX IF NOT EXISTS idx_items_kinozal_id ON items(kinozal_id);
                CREATE INDEX IF NOT EXISTS idx_items_archive_kinozal_id ON items_archive(kinozal_id, archived_at DESC);
                CREATE INDEX IF NOT EXISTS idx_deliveries_archive_user_kinozal ON deliveries_archive(tg_user_id, kinozal_id, delivered_at DESC);
                CREATE TABLE IF NOT EXISTS muted_titles (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL,
                    tmdb_id INTEGER NOT NULL,
                    created_at BIGINT NOT NULL DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    UNIQUE (tg_user_id, tmdb_id)
                );
                ALTER TABLE users ADD COLUMN IF NOT EXISTS quiet_start_hour INTEGER;
                ALTER TABLE users ADD COLUMN IF NOT EXISTS quiet_end_hour INTEGER;
                ALTER TABLE debounce_queue ADD COLUMN IF NOT EXISTS reset_count INTEGER NOT NULL DEFAULT 0;
                CREATE TABLE IF NOT EXISTS pending_deliveries (
                    id BIGSERIAL PRIMARY KEY,
                    tg_user_id BIGINT NOT NULL,
                    item_id BIGINT NOT NULL,
                    matched_sub_ids TEXT NOT NULL DEFAULT '',
                    old_release_text TEXT NOT NULL DEFAULT '',
                    is_release_text_change INTEGER NOT NULL DEFAULT 0,
                    queued_at BIGINT NOT NULL,
                    UNIQUE (tg_user_id, item_id)
                );
                CREATE TABLE IF NOT EXISTS debounce_queue (
                    tg_user_id BIGINT NOT NULL,
                    kinozal_id TEXT NOT NULL,
                    item_id BIGINT NOT NULL,
                    matched_sub_ids TEXT NOT NULL DEFAULT '',
                    deliver_after_ts BIGINT NOT NULL,
                    reset_count INTEGER NOT NULL DEFAULT 0,
                    UNIQUE (tg_user_id, kinozal_id)
                );
                """
            )

    def row_to_dict(self, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        return dict(row) if row else None

    def ensure_user(self, tg_user_id: int, username: str, first_name: str, auto_grant: bool = False) -> Dict[str, Any]:
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
            return self.get_user(tg_user_id)

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
        user = self.get_user(tg_user_id)
        if not user or not user["is_active"] or not user["access_granted"]:
            return False
        access_expires_at = user.get("access_expires_at")
        if access_expires_at is not None and int(access_expires_at) <= utc_ts():
            return False
        return True

    def set_user_access(self, tg_user_id: int, value: bool, access_expires_at: Any = ACCESS_EXPIRY_UNSET) -> None:
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
            return self.get_user(tg_user_id)
        user = self.get_user(tg_user_id)
        base_ts = utc_ts()
        if user and user.get("access_expires_at") and int(user["access_expires_at"]) > base_ts:
            base_ts = int(user["access_expires_at"])
        expires_at = base_ts + days * 86400
        self.set_user_access(tg_user_id, True, access_expires_at=expires_at)
        return self.get_user(tg_user_id)

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


    def list_broadcast_user_ids(self, active_only: bool = True, include_admins: bool = False) -> List[int]:
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
        user = self.get_user(tg_user_id)
        if not user:
            return None
        user["subscriptions"] = self.list_user_subscriptions(tg_user_id)
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
        return self.get_invite(code)

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

    def create_subscription(self, tg_user_id: int, name: Optional[str] = None) -> Dict[str, Any]:
        ts = utc_ts()
        if not name:
            count = len(self.list_user_subscriptions(tg_user_id)) + 1
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
        return self.get_subscription(sub_id)

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
        if self.get_meta(meta_key) == rollout_version:
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
            self.update_subscription(int(sub["id"]), **fields)
            self.set_subscription_genres(int(sub["id"]), spec.get("genre_ids", []))
            updated += 1

        self.set_meta(meta_key, rollout_version)
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
            data["genre_ids"] = self.get_subscription_genres(sub_id)
            data["country_codes_list"] = self.get_subscription_country_codes(sub_id)
            data["exclude_country_codes_list"] = self.get_subscription_exclude_country_codes(sub_id)
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
        genre_ids = sorted({int(g) for g in genre_ids})
        with self.lock:
            self.conn.execute("DELETE FROM subscription_genres WHERE subscription_id = ?", (sub_id,))
            if genre_ids:
                self.conn.executemany(
                    "INSERT INTO subscription_genres(subscription_id, genre_id) VALUES(?, ?)",
                    [(sub_id, gid) for gid in genre_ids],
                )
            self.conn.commit()

    def toggle_subscription_genre(self, sub_id: int, genre_id: int) -> None:
        current = set(self.get_subscription_genres(sub_id))
        if genre_id in current:
            current.remove(genre_id)
        else:
            current.add(genre_id)
        self.set_subscription_genres(sub_id, current)

    def get_subscription_country_codes(self, sub_id: int) -> List[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT country_codes FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            return parse_country_codes(row["country_codes"] if row else "")

    def set_subscription_country_codes(self, sub_id: int, country_codes: Iterable[str]) -> None:
        normalized = ",".join(parse_country_codes(list(country_codes)))
        self.update_subscription(sub_id, country_codes=normalized)

    def toggle_subscription_country_code(self, sub_id: int, country_code: str) -> None:
        current = set(self.get_subscription_country_codes(sub_id))
        code = compact_spaces(str(country_code or "")).upper()
        if not code:
            return
        if code in current:
            current.remove(code)
        else:
            current.add(code)
        self.set_subscription_country_codes(sub_id, sorted(current))

    def get_subscription_exclude_country_codes(self, sub_id: int) -> List[str]:
        with self.lock:
            row = self.conn.execute(
                "SELECT exclude_country_codes FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
            return parse_country_codes(row["exclude_country_codes"] if row else "")

    def set_subscription_exclude_country_codes(self, sub_id: int, country_codes: Iterable[str]) -> None:
        normalized = ",".join(parse_country_codes(list(country_codes)))
        self.update_subscription(sub_id, exclude_country_codes=normalized)

    def toggle_subscription_exclude_country_code(self, sub_id: int, country_code: str) -> None:
        current = set(self.get_subscription_exclude_country_codes(sub_id))
        code = compact_spaces(str(country_code or "")).upper()
        if not code:
            return
        if code in current:
            current.remove(code)
        else:
            current.add(code)
        self.set_subscription_exclude_country_codes(sub_id, sorted(current))

    def get_known_country_codes(self) -> List[str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT tmdb_countries FROM items WHERE tmdb_countries IS NOT NULL AND tmdb_countries <> ''"
            ).fetchall()

        codes = set(COUNTRY_NAMES_RU.keys())
        for row in rows:
            codes.update(parse_country_codes(row["tmdb_countries"]))

        if pycountry is not None:
            try:
                codes.update(str(country.alpha_2).upper() for country in pycountry.countries if getattr(country, "alpha_2", None))
            except Exception:
                pass

        return sorted(codes, key=lambda code: country_name_ru(code).lower())

    def upsert_genres(self, media_type: str, genre_map: Dict[int, str]) -> None:
        ts = utc_ts()
        with self.lock:
            for genre_id, name in genre_map.items():
                self.conn.execute(
                    """
                    INSERT INTO genres(media_type, genre_id, name, updated_at)
                    VALUES(?, ?, ?, ?)
                    ON CONFLICT(media_type, genre_id) DO UPDATE SET
                        name = excluded.name,
                        updated_at = excluded.updated_at
                    """,
                    (media_type, int(genre_id), name, ts),
                )
            self.conn.commit()

    def get_genres(self, media_type: str) -> Dict[int, str]:
        with self.lock:
            rows = self.conn.execute(
                "SELECT genre_id, name FROM genres WHERE media_type = ? ORDER BY name ASC",
                (media_type,),
            ).fetchall()
            return {int(row["genre_id"]): row["name"] for row in rows}

    def get_all_genres_merged(self) -> Dict[int, str]:
        result: Dict[int, str] = {}
        for media_type in ("movie", "tv"):
            result.update(self.get_genres(media_type))
        return dict(sorted(result.items(), key=lambda x: x[1].lower()))

    def get_meta(self, key: str) -> Optional[str]:
        with self.lock:
            row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else None

    def set_meta(self, key: str, value: str) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO meta(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, ts),
            )
            self.conn.commit()

    def _find_existing_item_for_upsert(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            "SELECT * FROM items WHERE source_uid = ? AND version_signature = ?",
            (data["source_uid"], data["version_signature"]),
        ).fetchone()
        if row:
            return row

        kinozal_id = compact_spaces(str(data.get("kinozal_id") or "")) or extract_kinozal_id(data.get("source_uid")) or extract_kinozal_id(data.get("source_link"))
        target_variant_sig = build_item_variant_signature(data)

        if kinozal_id:
            source_uid_norm = f"kinozal:{kinozal_id}"
            like_pattern = f"%details.php?id={kinozal_id}%"
            rows = self.conn.execute(
                """
                SELECT * FROM items
                WHERE source_uid = ? OR source_uid LIKE ? OR source_link LIKE ?
                ORDER BY CASE WHEN source_uid = ? THEN 0 ELSE 1 END, created_at DESC
                """,
                (source_uid_norm, like_pattern, like_pattern, source_uid_norm),
            ).fetchall()
            for candidate in rows:
                if build_item_variant_signature(dict(candidate)) == target_variant_sig:
                    return candidate
            return None

        source_uid = compact_spaces(str(data.get("source_uid") or ""))
        if not source_uid:
            return None
        rows = self.conn.execute(
            """
            SELECT * FROM items
            WHERE source_uid = ?
            ORDER BY created_at DESC
            """,
            (source_uid,),
        ).fetchall()
        for candidate in rows:
            if build_item_variant_signature(dict(candidate)) == target_variant_sig:
                return candidate
        return None

    def delivered_equivalent(self, tg_user_id: int, item: Dict[str, Any]) -> bool:
        media_type = str(item.get("media_type") or "movie")
        target_variant_sig = build_item_variant_signature(item)
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
        with self.lock:
            if kinozal_id:
                source_uid_norm = f"kinozal:{kinozal_id}"
                like_pattern = f"%details.php?id={kinozal_id}%"
                rows = self.conn.execute(
                    """
                    SELECT i.*
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE d.tg_user_id = ?
                      AND (i.source_uid = ? OR i.source_uid LIKE ? OR i.source_link LIKE ?)
                    ORDER BY d.delivered_at DESC, i.id DESC
                    """,
                    (tg_user_id, source_uid_norm, like_pattern, like_pattern),
                ).fetchall()
                for row in rows:
                    if build_item_variant_signature(dict(row)) == target_variant_sig:
                        return True
                row = self.conn.execute(
                    """
                    SELECT 1
                    FROM deliveries_archive
                    WHERE tg_user_id = ?
                      AND kinozal_id = ?
                      AND COALESCE(version_signature, '') = ?
                    LIMIT 1
                    """,
                    (tg_user_id, kinozal_id, compact_spaces(str(item.get("version_signature") or ""))),
                ).fetchone()
                return row is not None

            source_uid = compact_spaces(str(item.get("source_uid") or ""))
            if not source_uid:
                return False
            rows = self.conn.execute(
                """
                SELECT i.*
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ?
                  AND i.source_uid = ?
                ORDER BY d.delivered_at DESC, i.id DESC
                """,
                (tg_user_id, source_uid),
            ).fetchall()
            for row in rows:
                if build_item_variant_signature(dict(row)) == target_variant_sig:
                    return True
            row = self.conn.execute(
                """
                SELECT 1
                FROM deliveries_archive
                WHERE tg_user_id = ?
                  AND source_uid = ?
                  AND COALESCE(version_signature, '') = ?
                LIMIT 1
                """,
                (tg_user_id, source_uid, compact_spaces(str(item.get("version_signature") or ""))),
            ).fetchone()
            return row is not None

    def get_latest_delivered_related_item(self, tg_user_id: int, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        media_type = str(item.get("media_type") or "movie")
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
        with self.lock:
            if kinozal_id:
                source_uid_norm = f"kinozal:{kinozal_id}"
                like_pattern = f"%details.php?id={kinozal_id}%"
                row = self.conn.execute(
                    """
                    SELECT i.*
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE d.tg_user_id = ?
                      AND (i.source_uid = ? OR i.source_uid LIKE ? OR i.source_link LIKE ?)
                    ORDER BY d.delivered_at DESC, i.id DESC
                    LIMIT 1
                    """,
                    (tg_user_id, source_uid_norm, like_pattern, like_pattern),
                ).fetchone()
                if row:
                    return dict(row)
                archived = self.conn.execute(
                    """
                    SELECT ia.item_json
                    FROM deliveries_archive da
                    JOIN items_archive ia ON ia.original_item_id = da.original_item_id
                    WHERE da.tg_user_id = ?
                      AND da.kinozal_id = ?
                    ORDER BY da.delivered_at DESC, ia.archived_at DESC, ia.archive_id DESC
                    LIMIT 1
                    """,
                    (tg_user_id, kinozal_id),
                ).fetchone()
                if archived and archived.get("item_json"):
                    try:
                        return json.loads(archived["item_json"])
                    except Exception:
                        pass
                return None

            source_uid = compact_spaces(str(item.get("source_uid") or ""))
            if not source_uid:
                return None
            row = self.conn.execute(
                """
                SELECT i.*
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ?
                  AND i.source_uid = ?
                ORDER BY d.delivered_at DESC, i.id DESC
                LIMIT 1
                """,
                (tg_user_id, source_uid),
            ).fetchone()
            if row:
                return dict(row)
            archived = self.conn.execute(
                """
                SELECT ia.item_json
                FROM deliveries_archive da
                JOIN items_archive ia ON ia.original_item_id = da.original_item_id
                WHERE da.tg_user_id = ?
                  AND da.source_uid = ?
                ORDER BY da.delivered_at DESC, ia.archived_at DESC, ia.archive_id DESC
                LIMIT 1
                """,
                (tg_user_id, source_uid),
            ).fetchone()
            if archived and archived.get("item_json"):
                try:
                    return json.loads(archived["item_json"])
                except Exception:
                    pass
            return None


    def save_item(self, item: Dict[str, Any]) -> Tuple[int, bool, bool]:
        # Fallback for source adapters that provide raw items without precomputed version fields.
        if not item.get("variant_signature"):
            try:
                item["variant_signature"] = build_item_variant_signature(item)
            except Exception:
                item["variant_signature"] = ""

        if not item.get("variant_components"):
            try:
                item["variant_components"] = get_item_variant_components(item)
            except Exception:
                item["variant_components"] = {}

        if not item.get("version_signature"):
            try:
                item["version_signature"] = build_version_signature(
                    source_uid=item.get("source_uid"),
                    media_type=item.get("media_type"),
                    source_title=item.get("source_title"),
                    source_episode_progress=item.get("source_episode_progress"),
                    source_format=item.get("source_format"),
                    source_audio_tracks=item.get("source_audio_tracks"),
                )
            except Exception:
                item["version_signature"] = ""

        data = {
            "source_uid": item["source_uid"],
            "version_signature": item["version_signature"],
            "source_title": item["source_title"],
            "source_link": item.get("source_link"),
            "kinozal_id": resolve_item_kinozal_id(item),
            "source_published_at": item.get("source_published_at"),
            "source_year": item.get("source_year"),
            "source_format": item.get("source_format"),
            "source_description": item.get("source_description"),
            "source_episode_progress": item.get("source_episode_progress"),
            "source_audio_tracks": json.dumps(item.get("source_audio_tracks", []), ensure_ascii=False),
            "imdb_id": item.get("imdb_id"),
            "mal_id": item.get("mal_id"),
            "cleaned_title": item.get("cleaned_title"),
            "source_category_id": item.get("source_category_id"),
            "source_category_name": item.get("source_category_name"),
            "media_type": item.get("media_type"),
            "tmdb_id": item.get("tmdb_id"),
            "tmdb_title": item.get("tmdb_title"),
            "tmdb_original_title": item.get("tmdb_original_title"),
            "tmdb_original_language": item.get("tmdb_original_language"),
            "tmdb_rating": item.get("tmdb_rating"),
            "tmdb_vote_count": item.get("tmdb_vote_count"),
            "tmdb_release_date": item.get("tmdb_release_date"),
            "tmdb_overview": item.get("tmdb_overview"),
            "tmdb_poster_url": item.get("tmdb_poster_url"),
            "tmdb_status": item.get("tmdb_status"),
            "tmdb_age_rating": item.get("tmdb_age_rating"),
            "tmdb_countries": json.dumps(parse_country_codes(item.get("tmdb_countries", [])), ensure_ascii=False),
            "tmdb_number_of_seasons": item.get("tmdb_number_of_seasons"),
            "tmdb_number_of_episodes": item.get("tmdb_number_of_episodes"),
            "tmdb_next_episode_name": item.get("tmdb_next_episode_name"),
            "tmdb_next_episode_air_date": item.get("tmdb_next_episode_air_date"),
            "tmdb_next_episode_season_number": item.get("tmdb_next_episode_season_number"),
            "tmdb_next_episode_episode_number": item.get("tmdb_next_episode_episode_number"),
            "tmdb_last_episode_name": item.get("tmdb_last_episode_name"),
            "tmdb_last_episode_air_date": item.get("tmdb_last_episode_air_date"),
            "tmdb_last_episode_season_number": item.get("tmdb_last_episode_season_number"),
            "tmdb_last_episode_episode_number": item.get("tmdb_last_episode_episode_number"),
            "manual_bucket": item.get("manual_bucket") or "",
            "manual_country_codes": ",".join(parse_country_codes(item.get("manual_country_codes"))) if item.get("manual_country_codes") is not None else "",
            "source_release_text": item.get("source_release_text") or "",
            "raw_json": json.dumps(item.get("raw_json", {}), ensure_ascii=False, sort_keys=True),
            "created_at": utc_ts(),
        }

        def pick_value(new_value: Any, old_value: Any) -> Any:
            if new_value is None:
                return old_value
            if isinstance(new_value, str):
                return new_value if compact_spaces(new_value) else old_value
            if isinstance(new_value, (list, tuple, set, dict)):
                return new_value if new_value else old_value
            return new_value

        with self.lock:
            existing = self._find_existing_item_for_upsert(data)
            if existing:
                existing_id = int(existing["id"])
                existing_data = dict(existing)
                merged = dict(data)
                for key in data:
                    if key == "created_at":
                        merged[key] = existing_data.get(key, data[key])
                        continue
                    merged[key] = pick_value(data.get(key), existing_data.get(key))

                fields_to_update = [
                    "source_uid",
                    "version_signature",
                    "source_title",
                    "source_link",
                    "kinozal_id",
                    "source_published_at",
                    "source_year",
                    "source_format",
                    "source_description",
                    "source_episode_progress",
                    "source_audio_tracks",
                    "imdb_id",
                    "mal_id",
                    "cleaned_title",
                    "source_category_id",
                    "source_category_name",
                    "media_type",
                    "tmdb_id",
                    "tmdb_title",
                    "tmdb_original_title",
                    "tmdb_original_language",
                    "tmdb_rating",
                    "tmdb_vote_count",
                    "tmdb_release_date",
                    "tmdb_overview",
                    "tmdb_poster_url",
                    "tmdb_status",
                    "tmdb_age_rating",
                    "tmdb_countries",
                    "tmdb_number_of_seasons",
                    "tmdb_number_of_episodes",
                    "tmdb_next_episode_name",
                    "tmdb_next_episode_air_date",
                    "tmdb_next_episode_season_number",
                    "tmdb_next_episode_episode_number",
                    "tmdb_last_episode_name",
                    "tmdb_last_episode_air_date",
                    "tmdb_last_episode_season_number",
                    "tmdb_last_episode_episode_number",
                    "manual_bucket",
                    "manual_country_codes",
                    "source_release_text",
                    "raw_json",
                ]
                values = [merged[field] for field in fields_to_update]
                values.append(existing_id)

                current_genres = [
                    int(row["genre_id"])
                    for row in self.conn.execute(
                        "SELECT genre_id FROM item_genres WHERE item_id = ? ORDER BY genre_id",
                        (existing_id,),
                    ).fetchall()
                ]
                incoming_genres = sorted({int(x) for x in item.get("genre_ids", [])})
                final_genres = incoming_genres or current_genres

                materially_changed = False
                for field in fields_to_update:
                    if field in {"source_uid", "version_signature"}:
                        continue
                    old_value = existing_data.get(field)
                    new_value = merged.get(field)
                    if old_value != new_value:
                        materially_changed = True
                        break
                if current_genres != final_genres:
                    materially_changed = True

                self.conn.execute(
                    f"UPDATE items SET {', '.join(f'{field} = ?' for field in fields_to_update)} WHERE id = ?",
                    values,
                )

                self.conn.execute("DELETE FROM item_genres WHERE item_id = ?", (existing_id,))
                if final_genres:
                    self.conn.executemany(
                        "INSERT INTO item_genres(item_id, genre_id) VALUES(?, ?)",
                        [(existing_id, gid) for gid in final_genres],
                    )
                self.conn.commit()
                return existing_id, False, materially_changed

            fields = ", ".join(data.keys())
            marks = ", ".join("?" for _ in data)
            row = self.conn.execute(
                f"INSERT INTO items({fields}) VALUES({marks}) RETURNING id",
                list(data.values()),
            ).fetchone()
            item_id = int(row["id"])

            genre_ids = sorted({int(x) for x in item.get("genre_ids", [])})
            if genre_ids:
                self.conn.executemany(
                    "INSERT INTO item_genres(item_id, genre_id) VALUES(?, ?)",
                    [(item_id, gid) for gid in genre_ids],
                )
            self.conn.commit()
            return item_id, True, True

    def get_item(self, item_id: int) -> Optional[Dict[str, Any]]:
        with self.lock:
            row = self.conn.execute(
                "SELECT * FROM items WHERE id = ?",
                (item_id,),
            ).fetchone()
            if not row:
                return None
            data = dict(row)
            genres = self.conn.execute(
                "SELECT genre_id FROM item_genres WHERE item_id = ? ORDER BY genre_id",
                (item_id,),
            ).fetchall()
            data["genre_ids"] = [int(x["genre_id"]) for x in genres]
            data["tmdb_countries"] = parse_country_codes(data.get("tmdb_countries"))
            data["manual_country_codes"] = parse_country_codes(data.get("manual_country_codes"))
            if not compact_spaces(str(data.get("kinozal_id") or "")):
                data["kinozal_id"] = resolve_item_kinozal_id(data)
            return data

    def find_item_by_kinozal_id(self, kinozal_id: str) -> Optional[Dict[str, Any]]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return None
        with self.lock:
            row = self.conn.execute(
                """
                SELECT id FROM items
                WHERE kinozal_id = ? OR source_uid = ? OR source_uid LIKE ? OR source_link LIKE ?
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                LIMIT 1
                """,
                (kinozal_id, f"kinozal:{kinozal_id}", f"%details.php?id={kinozal_id}%", f"%id={kinozal_id}%"),
            ).fetchone()
            return self.get_item(int(row["id"])) if row else None

    def list_items_for_rematch(self, limit: int = 50, only_unmatched: bool = True) -> List[Dict[str, Any]]:
        fetch_limit = max(1, min(int(limit or 50), 500))
        where_clause = "WHERE COALESCE(kinozal_id, '') <> ''"
        if only_unmatched:
            where_clause += " AND tmdb_id IS NULL"
        with self.lock:
            rows = self.conn.execute(
                f"""
                SELECT id FROM items
                {where_clause}
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                LIMIT ?
                """,
                (fetch_limit,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        for row in rows:
            item = self.get_item(int(row["id"]))
            if item:
                result.append(item)
        return result

    def set_item_manual_routing(self, item_id: int, bucket: str = "", country_codes: Optional[Iterable[str]] = None) -> None:
        bucket_norm = str(bucket or "").strip().lower()
        if bucket_norm not in {"", "anime", "dorama", "regular"}:
            bucket_norm = ""
        countries_norm = ",".join(parse_country_codes(country_codes or []))
        with self.lock:
            self.conn.execute(
                "UPDATE items SET manual_bucket = ?, manual_country_codes = ? WHERE id = ?",
                (bucket_norm, countries_norm, int(item_id)),
            )
            self.conn.commit()

    def get_last_items(self, limit: int = 10) -> List[Dict[str, Any]]:
        fetch_limit = max(limit * 50, 200)
        with self.lock:
            rows = self.conn.execute(
                "SELECT id FROM items ORDER BY COALESCE(source_published_at, 0) DESC, id DESC LIMIT ?",
                (fetch_limit,),
            ).fetchall()
        result: List[Dict[str, Any]] = []
        seen = set()
        for row in rows:
            item = self.get_item(int(row["id"]))
            if not item:
                continue
            kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
            key = kinozal_id or compact_spaces(str(item.get("source_uid") or "")) or f"item:{int(item.get('id') or 0)}"
            if key in seen:
                continue
            seen.add(key)
            result.append(item)
            if len(result) >= limit:
                break
        return result

    def get_last_items_for_subscription(self, sub_id: int, limit: int = 5) -> List[Dict[str, Any]]:
        items = self.get_last_items(limit=max(100, limit * 30))
        sub = self.get_subscription(sub_id)
        if not sub:
            return []
        matched = [item for item in items if item and match_subscription(self, sub, item)]
        matched.sort(key=lambda item: (int(item.get("source_published_at") or 0), int(item.get("id") or 0)), reverse=True)
        return matched[:limit]

    def _archive_item_locked(self, item: Dict[str, Any], reason: str, merged_into_item_id: Optional[int] = None) -> bool:
        item_id = int(item["id"])
        existing = self.conn.execute("SELECT 1 FROM items WHERE id = ?", (item_id,)).fetchone()
        if not existing:
            return False
        full_item = self.get_item(item_id) or dict(item)
        kinozal_id = compact_spaces(str(full_item.get("kinozal_id") or "")) or resolve_item_kinozal_id(full_item)
        genre_ids = sorted({int(x) for x in full_item.get("genre_ids", [])})
        archived_at = utc_ts()
        self.conn.execute(
            """
            INSERT INTO items_archive(
                original_item_id, kinozal_id, source_uid, version_signature, source_title, source_link, media_type,
                source_published_at, source_year, source_format, source_description, source_episode_progress,
                source_audio_tracks, imdb_id, cleaned_title, source_category_id, source_category_name, tmdb_id, tmdb_title, tmdb_original_title,
                tmdb_original_language, tmdb_rating, tmdb_vote_count, tmdb_release_date, tmdb_status, tmdb_countries,
                manual_bucket, manual_country_codes, genre_ids, item_json, original_created_at, archived_at,
                archive_reason, merged_into_item_id
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?)
            """,
            (
                item_id,
                kinozal_id,
                full_item.get("source_uid"),
                full_item.get("version_signature"),
                full_item.get("source_title"),
                full_item.get("source_link"),
                full_item.get("media_type"),
                full_item.get("source_published_at"),
                full_item.get("source_year"),
                full_item.get("source_format"),
                full_item.get("source_description"),
                full_item.get("source_episode_progress"),
                json.dumps(full_item.get("source_audio_tracks", []), ensure_ascii=False),
                full_item.get("imdb_id"),
                full_item.get("cleaned_title"),
                full_item.get("source_category_id"),
                full_item.get("source_category_name"),
                full_item.get("tmdb_id"),
                full_item.get("tmdb_title"),
                full_item.get("tmdb_original_title"),
                full_item.get("tmdb_original_language"),
                full_item.get("tmdb_rating"),
                full_item.get("tmdb_vote_count"),
                full_item.get("tmdb_release_date"),
                full_item.get("tmdb_status"),
                json.dumps(parse_country_codes(full_item.get("tmdb_countries")), ensure_ascii=False),
                full_item.get("manual_bucket") or "",
                ",".join(parse_country_codes(full_item.get("manual_country_codes"))),
                json.dumps(genre_ids, ensure_ascii=False),
                json.dumps(full_item, ensure_ascii=False, sort_keys=True),
                full_item.get("created_at"),
                archived_at,
                compact_spaces(reason or "archive"),
                merged_into_item_id,
            ),
        )
        delivery_rows = self.conn.execute(
            "SELECT id, tg_user_id, item_id, subscription_id, matched_subscription_ids, delivered_at FROM deliveries WHERE item_id = ? ORDER BY delivered_at ASC, id ASC",
            (item_id,),
        ).fetchall()
        for row in delivery_rows:
            delivery = dict(row)
            self.conn.execute(
                """
                INSERT INTO deliveries_archive(
                    original_delivery_id, tg_user_id, original_item_id, kinozal_id, source_uid, media_type, version_signature,
                    source_title, subscription_id, matched_subscription_ids, delivered_at, archived_at, archive_reason, merged_into_item_id
                )
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    delivery.get("id"),
                    delivery.get("tg_user_id"),
                    item_id,
                    kinozal_id,
                    full_item.get("source_uid"),
                    full_item.get("media_type"),
                    full_item.get("version_signature"),
                    full_item.get("source_title"),
                    delivery.get("subscription_id"),
                    delivery.get("matched_subscription_ids"),
                    delivery.get("delivered_at"),
                    archived_at,
                    compact_spaces(reason or "archive"),
                    merged_into_item_id,
                ),
            )
        self.conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
        return True

    def archive_item(self, item_id: int, reason: str, merged_into_item_id: Optional[int] = None) -> bool:
        with self.lock:
            item = self.get_item(int(item_id))
            if not item:
                return False
            archived = self._archive_item_locked(item, reason=reason, merged_into_item_id=merged_into_item_id)
            self.conn.commit()
            return archived

    def get_version_timeline(self, kinozal_id: str, limit: int = 10) -> Dict[str, Any]:
        kinozal_id = compact_spaces(str(kinozal_id or ""))
        if not kinozal_id:
            return {"kinozal_id": "", "active_count": 0, "archived_count": 0, "versions": []}
        with self.lock:
            active_rows = [dict(x) for x in self.conn.execute(
                """
                SELECT
                    id AS record_id,
                    kinozal_id,
                    source_uid,
                    version_signature,
                    source_title,
                    source_link,
                    media_type,
                    source_published_at,
                    source_year,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    created_at,
                    NULL::BIGINT AS archived_at,
                    'active' AS state
                FROM items
                WHERE kinozal_id = ?
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """,
                (kinozal_id,),
            ).fetchall()]
            archived_rows = [dict(x) for x in self.conn.execute(
                """
                SELECT
                    original_item_id AS record_id,
                    kinozal_id,
                    source_uid,
                    version_signature,
                    source_title,
                    source_link,
                    media_type,
                    source_published_at,
                    source_year,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    original_created_at AS created_at,
                    archived_at,
                    'archived' AS state
                FROM items_archive
                WHERE kinozal_id = ?
                ORDER BY COALESCE(source_published_at, 0) DESC, original_created_at DESC, archive_id DESC
                """,
                (kinozal_id,),
            ).fetchall()]

        def sort_key(entry: Dict[str, Any]) -> tuple[int, int, int]:
            return (
                int(entry.get("source_published_at") or 0),
                int(entry.get("created_at") or 0),
                int(entry.get("record_id") or 0),
            )

        grouped: Dict[str, Dict[str, Any]] = {}
        for row in active_rows + archived_rows:
            row["kinozal_id"] = kinozal_id
            sig = compact_spaces(str(row.get("version_signature") or "")) or build_item_variant_signature(row)
            existing = grouped.get(sig)
            if not existing or sort_key(row) > sort_key(existing):
                row["version_duplicates"] = int(existing.get("version_duplicates") or 1) + 1 if existing else 1
                grouped[sig] = row
            elif existing:
                existing["version_duplicates"] = int(existing.get("version_duplicates") or 1) + 1

        versions = sorted(grouped.values(), key=sort_key, reverse=True)
        for entry in versions:
            entry["variant_summary"] = format_variant_summary(entry)
        return {
            "kinozal_id": kinozal_id,
            "active_count": len(active_rows),
            "archived_count": len(archived_rows),
            "versions": versions[: max(1, int(limit or 10))],
        }

    def cleanup_old_versions(self, keep_last: int = 3, dry_run: bool = True, preview_limit: int = 15) -> Dict[str, Any]:
        keep_last = max(1, int(keep_last or 1))
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    id,
                    kinozal_id,
                    source_uid,
                    source_link,
                    source_title,
                    media_type,
                    version_signature,
                    source_published_at,
                    created_at,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks
                FROM items
                WHERE COALESCE(kinozal_id, '') <> ''
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """
            ).fetchall()
            by_release: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = {}
            for row in rows:
                item = dict(row)
                kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item)
                if not kinozal_id:
                    continue
                media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
                version_sig = compact_spaces(str(item.get("version_signature") or "")) or build_item_variant_signature(item)
                by_release.setdefault((kinozal_id, media_type), {}).setdefault(version_sig, []).append(item)

            groups: List[Dict[str, Any]] = []
            versions_to_archive = 0
            items_to_archive = 0

            def item_sort_key(x: Dict[str, Any]) -> tuple[int, int, int]:
                return (
                    int(x.get("source_published_at") or 0),
                    int(x.get("created_at") or 0),
                    int(x.get("id") or 0),
                )

            for (kinozal_id, media_type), versions_map in by_release.items():
                version_groups: List[Dict[str, Any]] = []
                for version_sig, items in versions_map.items():
                    ordered_items = sorted(items, key=item_sort_key, reverse=True)
                    version_groups.append({
                        "version_signature": version_sig,
                        "items": ordered_items,
                        "representative": ordered_items[0],
                    })
                version_groups.sort(key=lambda g: item_sort_key(g["representative"]), reverse=True)
                if len(version_groups) <= keep_last:
                    continue
                keep_groups = version_groups[:keep_last]
                archive_groups = version_groups[keep_last:]
                archive_items = [item for group in archive_groups for item in group["items"]]
                groups.append({
                    "kinozal_id": kinozal_id,
                    "media_type": media_type,
                    "title": compact_spaces(str((keep_groups[0]["representative"].get("source_title") if keep_groups else archive_groups[0]["representative"].get("source_title")) or "")),
                    "keep_ids": [int(g["representative"]["id"]) for g in keep_groups],
                    "archive_ids": [int(item["id"]) for item in archive_items],
                    "versions_total": len(version_groups),
                    "versions_keep": len(keep_groups),
                    "versions_archive": len(archive_groups),
                })
                versions_to_archive += len(archive_groups)
                items_to_archive += len(archive_items)
                if not dry_run:
                    for archive_group in archive_groups:
                        for item in archive_group["items"]:
                            self._archive_item_locked(item, reason=f"keep_last_versions:{keep_last}", merged_into_item_id=None)

            if not dry_run:
                self.conn.commit()

            groups.sort(key=lambda x: (x["versions_archive"], x["kinozal_id"]), reverse=True)
            summary = {
                "dry_run": dry_run,
                "groups": len(groups),
                "versions_to_archive": versions_to_archive,
                "items_to_archive": items_to_archive,
                "keep_last": keep_last,
                "sample_groups": groups[: max(1, int(preview_limit or 15))],
            }
            log.info(
                "Old versions cleanup dry_run=%s keep_last=%s groups=%s versions_to_archive=%s items_to_archive=%s",
                dry_run,
                keep_last,
                summary["groups"],
                summary["versions_to_archive"],
                summary["items_to_archive"],
            )
            return summary

    def cleanup_exact_duplicate_items(self, dry_run: bool = True, preview_limit: int = 15) -> Dict[str, Any]:
        with self.lock:
            rows = self.conn.execute(
                """
                SELECT
                    id,
                    source_uid,
                    source_link,
                    source_title,
                    media_type,
                    source_published_at,
                    created_at,
                    source_format,
                    source_episode_progress,
                    source_audio_tracks,
                    imdb_id,
                    tmdb_id,
                    tmdb_overview,
                    tmdb_poster_url,
                    tmdb_status,
                    tmdb_vote_count,
                    tmdb_countries,
                    source_description,
                    manual_bucket
                FROM items
                ORDER BY COALESCE(source_published_at, 0) DESC, created_at DESC, id DESC
                """
            ).fetchall()

            groups: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
            for row in rows:
                item = dict(row)
                kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))
                if not kinozal_id:
                    continue
                media_type = compact_spaces(str(item.get("media_type") or "movie")).lower() or "movie"
                variant_sig = build_item_variant_signature(item)
                groups.setdefault((kinozal_id, media_type, variant_sig), []).append(item)

            duplicate_groups: List[Dict[str, Any]] = []
            items_to_delete = 0
            deliveries_to_migrate = 0

            for (kinozal_id, media_type, _variant_sig), items in groups.items():
                if len(items) < 2:
                    continue
                ordered = sorted(
                    items,
                    key=lambda x: (
                        item_duplicate_quality_score(x),
                        int(x.get("source_published_at") or 0),
                        int(x.get("created_at") or 0),
                        int(x.get("id") or 0),
                    ),
                    reverse=True,
                )
                keeper = ordered[0]
                losers = ordered[1:]
                moved_for_group = 0
                for loser in losers:
                    row = self.conn.execute(
                        "SELECT COUNT(*) AS cnt FROM deliveries WHERE item_id = ?",
                        (int(loser["id"]),),
                    ).fetchone()
                    moved_for_group += int((row or {}).get("cnt") or 0)

                duplicate_groups.append({
                    "kinozal_id": kinozal_id,
                    "media_type": media_type,
                    "title": compact_spaces(str(keeper.get("source_title") or losers[0].get("source_title") or "")),
                    "keep_id": int(keeper["id"]),
                    "remove_ids": [int(x["id"]) for x in losers],
                    "count": len(items),
                    "deliveries_to_migrate": moved_for_group,
                })
                items_to_delete += len(losers)
                deliveries_to_migrate += moved_for_group

                if not dry_run:
                    for loser in losers:
                        keeper_id = int(keeper["id"])
                        loser_id = int(loser["id"])
                        self.conn.execute(
                            """
                            INSERT INTO deliveries(tg_user_id, item_id, subscription_id, matched_subscription_ids, delivered_at)
                            SELECT tg_user_id, ?, subscription_id, matched_subscription_ids, delivered_at
                            FROM deliveries
                            WHERE item_id = ?
                            ON CONFLICT(tg_user_id, item_id) DO NOTHING
                            """,
                            (keeper_id, loser_id),
                        )
                        self._archive_item_locked(loser, reason="exact_duplicate_cleanup", merged_into_item_id=keeper_id)

            duplicate_groups.sort(key=lambda x: (x["count"], x["kinozal_id"]), reverse=True)
            summary = {
                "dry_run": dry_run,
                "groups": len(duplicate_groups),
                "items_to_delete": items_to_delete,
                "deliveries_to_migrate": deliveries_to_migrate,
                "sample_groups": duplicate_groups[: max(1, int(preview_limit or 15))],
            }
            log.info(
                "Duplicate cleanup dry_run=%s groups=%s items_to_delete=%s deliveries_to_migrate=%s",
                dry_run,
                summary["groups"],
                summary["items_to_delete"],
                summary["deliveries_to_migrate"],
            )
            return summary


    def delivered(self, tg_user_id: int, item_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM deliveries WHERE tg_user_id = ? AND item_id = ?",
                (tg_user_id, item_id),
            ).fetchone()
            return row is not None

    def record_delivery(self, tg_user_id: int, item_id: int, sub_id: Optional[int], matched_sub_ids: Optional[Iterable[int]] = None) -> None:
        matched_ids_csv = None
        if matched_sub_ids:
            normalized_ids = sorted({int(x) for x in matched_sub_ids})
            matched_ids_csv = ",".join(str(x) for x in normalized_ids) if normalized_ids else None
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO deliveries(tg_user_id, item_id, subscription_id, matched_subscription_ids, delivered_at)
                VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(tg_user_id, item_id) DO NOTHING
                """,
                (tg_user_id, item_id, sub_id, matched_ids_csv, utc_ts()),
            )
            self.conn.commit()

    def recently_delivered(self, tg_user_id: int, item_id: int, cooldown_seconds: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM deliveries WHERE tg_user_id = ? AND item_id = ? AND delivered_at > ?",
                (tg_user_id, item_id, utc_ts() - cooldown_seconds),
            ).fetchone()
            return row is not None

    def upsert_debounce(self, tg_user_id: int, kinozal_id: str, item_id: int,
                        matched_sub_ids: str, delay_seconds: int) -> None:
        after_ts = utc_ts() + delay_seconds
        with self.lock:
            self.conn.execute(
                """INSERT INTO debounce_queue (tg_user_id, kinozal_id, item_id, matched_sub_ids, deliver_after_ts, reset_count)
                   VALUES (?, ?, ?, ?, ?, 0)
                   ON CONFLICT (tg_user_id, kinozal_id) DO UPDATE SET
                       item_id = excluded.item_id,
                       matched_sub_ids = excluded.matched_sub_ids,
                       deliver_after_ts = CASE
                           WHEN debounce_queue.reset_count < 2 THEN excluded.deliver_after_ts
                           ELSE debounce_queue.deliver_after_ts
                       END,
                       reset_count = CASE
                           WHEN debounce_queue.reset_count < 2 THEN debounce_queue.reset_count + 1
                           ELSE debounce_queue.reset_count
                       END""",
                (tg_user_id, kinozal_id, item_id, matched_sub_ids or "", after_ts),
            )
            self.conn.commit()

    def pop_due_debounce(self) -> List[Dict[str, Any]]:
        now = utc_ts()
        with self.lock:
            rows = self.conn.execute(
                "DELETE FROM debounce_queue WHERE deliver_after_ts <= ? RETURNING *",
                (now,),
            ).fetchall()
            if rows:
                self.conn.commit()
            return [dict(r) for r in rows]

    def recently_delivered_kinozal_id(self, tg_user_id: int, kinozal_id: str, cooldown_seconds: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                """
                SELECT 1 FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE d.tg_user_id = ? AND i.kinozal_id = ? AND d.delivered_at > ?
                LIMIT 1
                """,
                (tg_user_id, kinozal_id, utc_ts() - cooldown_seconds),
            ).fetchone()
            return row is not None

    def was_delivered_to_anyone(self, item_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM deliveries WHERE item_id = ? LIMIT 1",
                (item_id,),
            ).fetchone()
            return row is not None

    def update_item_release_text(self, item_id: int, text: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE items SET source_release_text = ? WHERE id = ?",
                (text or "", item_id),
            )
            self.conn.commit()

    def mute_title(self, tg_user_id: int, tmdb_id: int) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                "INSERT INTO muted_titles (tg_user_id, tmdb_id, created_at) VALUES (?, ?, ?) ON CONFLICT (tg_user_id, tmdb_id) DO NOTHING",
                (tg_user_id, tmdb_id, ts),
            )
            self.conn.commit()

    def unmute_title(self, tg_user_id: int, tmdb_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "DELETE FROM muted_titles WHERE tg_user_id = ? AND tmdb_id = ?",
                (tg_user_id, tmdb_id),
            )
            self.conn.commit()

    def is_title_muted(self, tg_user_id: int, tmdb_id: int) -> bool:
        with self.lock:
            row = self.conn.execute(
                "SELECT 1 FROM muted_titles WHERE tg_user_id = ? AND tmdb_id = ? LIMIT 1",
                (tg_user_id, tmdb_id),
            ).fetchone()
            return row is not None

    def list_muted_titles(self, tg_user_id: int, limit: int = 30) -> list:
        with self.lock:
            rows = self.conn.execute(
                """SELECT mt.tmdb_id,
                          COALESCE(
                              (SELECT i.tmdb_title FROM items i
                               WHERE i.tmdb_id = mt.tmdb_id AND i.tmdb_title IS NOT NULL
                               ORDER BY i.id DESC LIMIT 1),
                              (SELECT i.source_title FROM items i
                               WHERE i.tmdb_id = mt.tmdb_id
                               ORDER BY i.id DESC LIMIT 1)
                          ) AS title,
                          (SELECT i.media_type FROM items i
                           WHERE i.tmdb_id = mt.tmdb_id
                           ORDER BY i.id DESC LIMIT 1) AS media_type,
                          mt.created_at
                   FROM muted_titles mt
                   WHERE mt.tg_user_id = ?
                   ORDER BY mt.created_at DESC
                   LIMIT ?""",
                (tg_user_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_user_delivery_history(self, tg_user_id: int, limit: int = 15) -> list:
        with self.lock:
            rows = self.conn.execute(
                """SELECT i.id, i.source_title, i.source_link, i.tmdb_title, i.media_type,
                          d.delivered_at
                   FROM deliveries d
                   JOIN items i ON d.item_id = i.id
                   WHERE d.tg_user_id = ?
                   ORDER BY d.delivered_at DESC
                   LIMIT ?""",
                (tg_user_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]

    def set_user_quiet_hours(self, tg_user_id: int, start_hour, end_hour) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                "UPDATE users SET quiet_start_hour = ?, quiet_end_hour = ?, updated_at = ? WHERE tg_user_id = ?",
                (start_hour, end_hour, ts, tg_user_id),
            )
            self.conn.commit()

    def get_user_quiet_hours(self, tg_user_id: int):
        with self.lock:
            row = self.conn.execute(
                "SELECT quiet_start_hour, quiet_end_hour FROM users WHERE tg_user_id = ?",
                (tg_user_id,),
            ).fetchone()
            if not row:
                return (None, None)
            return (row["quiet_start_hour"], row["quiet_end_hour"])

    def queue_pending_delivery(self, tg_user_id: int, item_id: int, matched_sub_ids: str,
                                old_release_text: str, is_release_text_change: bool) -> None:
        ts = utc_ts()
        with self.lock:
            self.conn.execute(
                """INSERT INTO pending_deliveries
                   (tg_user_id, item_id, matched_sub_ids, old_release_text, is_release_text_change, queued_at)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT (tg_user_id, item_id) DO NOTHING""",
                (tg_user_id, item_id, matched_sub_ids or "", old_release_text or "",
                 1 if is_release_text_change else 0, ts),
            )
            self.conn.commit()

    def pop_due_pending_deliveries(self, current_hour: int) -> dict:
        with self.lock:
            rows = self.conn.execute(
                """SELECT pd.tg_user_id, pd.item_id, pd.matched_sub_ids, pd.old_release_text,
                          pd.is_release_text_change, u.quiet_start_hour, u.quiet_end_hour
                   FROM pending_deliveries pd
                   JOIN users u ON pd.tg_user_id = u.tg_user_id
                   ORDER BY pd.queued_at ASC""",
            ).fetchall()
        result: dict = {}
        for row in rows:
            r = dict(row)
            start_h = r.get("quiet_start_hour")
            end_h = r.get("quiet_end_hour")
            if start_h is not None and end_h is not None:
                if start_h < end_h:
                    still_quiet = start_h <= current_hour < end_h
                else:
                    still_quiet = current_hour >= start_h or current_hour < end_h
                if still_quiet:
                    continue
            result.setdefault(r["tg_user_id"], []).append(r)
        return result

    def delete_pending_delivery(self, tg_user_id: int, item_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "DELETE FROM pending_deliveries WHERE tg_user_id = ? AND item_id = ?",
                (tg_user_id, item_id),
            )
            self.conn.commit()
