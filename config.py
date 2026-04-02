import os
from dataclasses import dataclass
from typing import Sequence

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    bot_token: str = os.getenv("BOT_TOKEN", "").strip()
    admin_ids: Sequence[int] = tuple(
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()
    )
    allow_mode: str = os.getenv("ALLOW_MODE", "invite").strip().lower()
    tmdb_token: str = os.getenv("TMDB_TOKEN", "").strip()
    poll_seconds: int = int(os.getenv("POLL_SECONDS", "60"))
    request_timeout: int = int(os.getenv("REQUEST_TIMEOUT", "40"))
    database_url: str = os.getenv("DATABASE_URL", "").strip()
    redis_url: str = os.getenv("REDIS_URL", "").strip()
    disable_preview: bool = os.getenv("DISABLE_WEB_PAGE_PREVIEW", "1").lower() not in {"0", "false", "no"}
    start_fetch_as_read: bool = os.getenv("BOOTSTRAP_AS_READ", "1").lower() not in {"0", "false", "no"}
    source_fetch_limit: int = int(os.getenv("SOURCE_FETCH_LIMIT", "50"))
    cleanup_duplicates_preview_limit: int = int(os.getenv("CLEANUP_DUPLICATES_PREVIEW_LIMIT", "15"))
    cleanup_versions_preview_limit: int = int(os.getenv("CLEANUP_VERSIONS_PREVIEW_LIMIT", "15"))
    cleanup_versions_keep_last: int = int(os.getenv("CLEANUP_VERSIONS_KEEP_LAST", "3"))
    deep_link_bot_username: str = os.getenv("DEEP_LINK_BOT_USERNAME", "").strip()
    language: str = os.getenv("TMDB_LANGUAGE", "ru-RU").strip()
    startup_db_retries: int = int(os.getenv("STARTUP_DB_RETRIES", "30"))
    startup_db_retry_delay: int = int(os.getenv("STARTUP_DB_RETRY_DELAY", "2"))
    tmdb_cache_ttl: int = int(os.getenv("TMDB_CACHE_TTL", str(7 * 86400)))
    tmdb_negative_cache_ttl: int = int(os.getenv("TMDB_NEGATIVE_CACHE_TTL", str(6 * 3600)))

    anime_resolver_enabled: bool = os.getenv("ANIME_RESOLVER_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
    anime_resolver_log_only: bool = os.getenv("ANIME_RESOLVER_LOG_ONLY", "1").strip().lower() not in {"0", "false", "no", "off"}
    anime_mappings_dir: str = os.getenv("ANIME_MAPPINGS_DIR", "data/anime-mappings").strip()

    source_error_alert_threshold: int = int(os.getenv("SOURCE_ERROR_ALERT_THRESHOLD", "3"))
    source_error_alert_repeat_minutes: int = int(os.getenv("SOURCE_ERROR_ALERT_REPEAT_MINUTES", "180"))


CFG = Config()

ACCESS_EXPIRY_UNSET = object()

if not CFG.bot_token:
    raise RuntimeError("Не задан BOT_TOKEN")
if not CFG.database_url:
    raise RuntimeError("Не задан DATABASE_URL")
if CFG.allow_mode not in {"open", "invite", "manual"}:
    raise RuntimeError("ALLOW_MODE должен быть одним из: open, invite, manual")
