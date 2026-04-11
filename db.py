import logging
import threading
import time
from typing import Any, Dict, Optional, Sequence

from db_migrations import apply_schema_migrations
from psycopg import InterfaceError, OperationalError, connect
from psycopg.rows import dict_row

from config import CFG
from repositories import (
    DeliveryRepository,
    ItemsRepository,
    MatchReviewRepository,
    MetaRepository,
    SubscriptionsRepository,
    UsersRepository,
)

log = logging.getLogger("kinozal-news-bot")


class DummyCursor:
    def fetchone(self) -> None:
        return None

    def fetchall(self) -> list:
        return []


class PGCompatConnection:
    def __init__(self, dsn: str):
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

    def execute(self, query: str, params: Optional[Sequence[Any]] = None) -> Any:
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

    def executemany(self, sql: str, seq: Sequence[Sequence[Any]]) -> DummyCursor:
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
        statements = [statement.strip() for statement in script.split(";") if statement.strip()]
        with self.raw.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)

    def commit(self) -> None:
        return None

    def close(self) -> None:
        with self._lock:
            self._close_raw_quietly()


class DB:
    def __init__(self, dsn: str):
        self.lock = threading.RLock()
        last_error: Optional[Exception] = None
        for attempt in range(1, CFG.startup_db_retries + 1):
            try:
                self.conn = PGCompatConnection(dsn)
                self.init_schema()
                self._init_repositories()
                return
            except Exception as exc:
                last_error = exc
                log.warning(
                    "Postgres not ready yet (%s/%s): %s",
                    attempt,
                    CFG.startup_db_retries,
                    exc,
                )
                time.sleep(CFG.startup_db_retry_delay)
        raise RuntimeError(f"Не удалось подключиться к Postgres: {last_error}")

    def _init_repositories(self) -> None:
        self.users = UsersRepository(self)
        self.subscriptions = SubscriptionsRepository(self)
        self.meta = MetaRepository(self)
        self.items = ItemsRepository(self)
        self.deliveries = DeliveryRepository(self)
        self.match_reviews = MatchReviewRepository(self)
        self._repositories = (
            self.users,
            self.subscriptions,
            self.meta,
            self.items,
            self.deliveries,
            self.match_reviews,
        )

    def __getattr__(self, name: str) -> Any:
        repos = object.__getattribute__(self, "_repositories")
        for repo in repos:
            if hasattr(repo, name):
                return getattr(repo, name)
        raise AttributeError(f"{self.__class__.__name__!s} has no attribute {name!r}")

    def init_schema(self) -> None:
        with self.lock:
            apply_schema_migrations(self.conn)

    def row_to_dict(self, row: Optional[Any]) -> Optional[Dict[str, Any]]:
        return dict(row) if row else None

    def close(self) -> None:
        with self.lock:
            self.conn.close()
