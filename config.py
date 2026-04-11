import os
from dataclasses import dataclass
from typing import Sequence

from dotenv import load_dotenv

load_dotenv()


def _env_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    if not stripped:
        return default
    return int(stripped)


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    if not stripped:
        return default
    return float(stripped)


@dataclass
class Config:
    bot_token: str = _env_str("BOT_TOKEN")
    admin_ids: Sequence[int] = tuple(
        int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()
    )
    allow_mode: str = _env_str("ALLOW_MODE", "invite").lower()
    tmdb_token: str = _env_str("TMDB_TOKEN")
    poll_seconds: int = _env_int("POLL_SECONDS", 60)
    request_timeout: int = _env_int("REQUEST_TIMEOUT", 40)
    database_url: str = _env_str("DATABASE_URL")
    redis_url: str = _env_str("REDIS_URL")
    disable_preview: bool = os.getenv("DISABLE_WEB_PAGE_PREVIEW", "1").lower() not in {"0", "false", "no"}
    start_fetch_as_read: bool = os.getenv("BOOTSTRAP_AS_READ", "1").lower() not in {"0", "false", "no"}
    source_fetch_limit: int = _env_int("SOURCE_FETCH_LIMIT", 50)
    cleanup_duplicates_preview_limit: int = _env_int("CLEANUP_DUPLICATES_PREVIEW_LIMIT", 15)
    cleanup_versions_preview_limit: int = _env_int("CLEANUP_VERSIONS_PREVIEW_LIMIT", 15)
    cleanup_versions_keep_last: int = _env_int("CLEANUP_VERSIONS_KEEP_LAST", 3)
    deep_link_bot_username: str = _env_str("DEEP_LINK_BOT_USERNAME")
    language: str = _env_str("TMDB_LANGUAGE", "ru-RU")
    startup_db_retries: int = _env_int("STARTUP_DB_RETRIES", 30)
    startup_db_retry_delay: int = _env_int("STARTUP_DB_RETRY_DELAY", 2)
    tmdb_cache_ttl: int = _env_int("TMDB_CACHE_TTL", 7 * 86400)
    tmdb_negative_cache_ttl: int = _env_int("TMDB_NEGATIVE_CACHE_TTL", 6 * 3600)

    anime_resolver_enabled: bool = _env_str("ANIME_RESOLVER_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
    anime_resolver_log_only: bool = _env_str("ANIME_RESOLVER_LOG_ONLY", "1").lower() not in {"0", "false", "no", "off"}
    anime_mappings_dir: str = _env_str("ANIME_MAPPINGS_DIR", "data/anime-mappings")

    source_error_alert_threshold: int = _env_int("SOURCE_ERROR_ALERT_THRESHOLD", 3)
    source_error_alert_repeat_minutes: int = _env_int("SOURCE_ERROR_ALERT_REPEAT_MINUTES", 180)
    sentry_dsn: str = _env_str("SENTRY_DSN")
    sentry_environment: str = _env_str("SENTRY_ENVIRONMENT", "production")
    sentry_release: str = _env_str("SENTRY_RELEASE")
    sentry_traces_sample_rate: float = _env_float("SENTRY_TRACES_SAMPLE_RATE", 0.0)
    api_host: str = _env_str("API_HOST", "0.0.0.0")
    api_port: int = _env_int("API_PORT", 8000)
    admin_http_token: str = _env_str("ADMIN_HTTP_TOKEN")


CFG = Config()

if CFG.allow_mode == "all":
    CFG.allow_mode = "open"

ACCESS_EXPIRY_UNSET = object()

if not CFG.bot_token:
    raise RuntimeError("Не задан BOT_TOKEN")
if not CFG.database_url:
    raise RuntimeError("Не задан DATABASE_URL")
if CFG.allow_mode not in {"open", "invite", "manual"}:
    raise RuntimeError("ALLOW_MODE должен быть одним из: open, invite, manual")
