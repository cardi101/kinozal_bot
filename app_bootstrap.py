import logging
import os
from dataclasses import dataclass
from typing import Any

from aiogram import F, Router
from aiogram.types import CallbackQuery

from admin_access_handlers import register_admin_access_handlers
from admin_match_handlers import register_admin_match_handlers
from config import CFG
from db import DB
from history_handlers import register_history_handlers
from kinozal_source import KinozalSource
from menu_handlers import register_menu_handlers
from mute_title_handlers import register_mute_title_handlers
from muted_list_handlers import register_muted_list_handlers
from quiet_hours_handlers import register_quiet_hours_handlers
from redis_cache import RedisCache
from runtime_app import AppRuntime
from runtime_poller import poller
from subscription_basic_handlers import register_subscription_basic_handlers
from subscription_filter_handlers import register_subscription_filter_handlers
from subscription_input_handlers import register_subscription_input_handlers
from subscription_presets import PRESET_ROLLOUT_VERSION
from subscription_test_handlers import register_subscription_test_handlers
from subscription_wizard_handlers import register_subscription_wizard_handlers
from tmdb_client import TMDBClient
from user_handlers import register_user_handlers

ADMIN_USERS_PAGE_SIZE = 12


@dataclass(slots=True)
class AppContainer:
    db: Any
    cache: Any
    tmdb: Any
    source: Any
    router: Router
    runtime: AppRuntime
    log: logging.Logger


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("kinozal-news-bot")


def build_router(db: Any, source: Any, tmdb: Any) -> Router:
    router = Router()

    register_menu_handlers(router, db, source, tmdb, ADMIN_USERS_PAGE_SIZE)
    register_subscription_basic_handlers(router, db)
    register_subscription_filter_handlers(router, db)
    register_subscription_input_handlers(router, db)
    register_subscription_wizard_handlers(router, db)
    register_subscription_test_handlers(router, db, source, tmdb)
    register_mute_title_handlers(router, db)
    register_muted_list_handlers(router, db)
    register_history_handlers(router, db)
    register_quiet_hours_handlers(router, db)
    register_user_handlers(router, db, source, tmdb)
    register_admin_match_handlers(router, db, tmdb)
    register_admin_access_handlers(router, db, ADMIN_USERS_PAGE_SIZE)

    @router.callback_query(F.data == "noop")
    async def cb_noop(callback: CallbackQuery) -> None:
        await callback.answer()

    return router


def build_app() -> AppContainer:
    log = configure_logging()
    db = DB(CFG.database_url)
    cache = RedisCache(CFG.redis_url)
    tmdb = TMDBClient(CFG, db, cache, CFG.tmdb_token, CFG.language, log)
    source = KinozalSource()
    router = build_router(db, source, tmdb)
    runtime = AppRuntime(
        CFG,
        router,
        db,
        source,
        tmdb,
        cache,
        poller,
        log,
        PRESET_ROLLOUT_VERSION,
    )
    return AppContainer(
        db=db,
        cache=cache,
        tmdb=tmdb,
        source=source,
        router=router,
        runtime=runtime,
        log=log,
    )
