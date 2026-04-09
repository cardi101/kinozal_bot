import asyncio
import logging
import os

from aiogram import F, Router
from aiogram.types import CallbackQuery
from config import CFG
from subscription_presets import PRESET_ROLLOUT_VERSION
from menu_handlers import register_menu_handlers
from subscription_basic_handlers import register_subscription_basic_handlers
from subscription_filter_handlers import register_subscription_filter_handlers
from subscription_input_handlers import register_subscription_input_handlers
from subscription_wizard_handlers import register_subscription_wizard_handlers
from mute_title_handlers import register_mute_title_handlers
from muted_list_handlers import register_muted_list_handlers
from history_handlers import register_history_handlers
from quiet_hours_handlers import register_quiet_hours_handlers
from subscription_test_handlers import register_subscription_test_handlers
from user_handlers import register_user_handlers
from admin_match_handlers import register_admin_match_handlers
from admin_access_handlers import register_admin_access_handlers
from runtime_poller import poller
from runtime_app import AppRuntime
from redis_cache import RedisCache
from tmdb_client import TMDBClient
from kinozal_source import KinozalSource
from db import DB

try:
    import pycountry
except Exception:
    pycountry = None

logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("kinozal-news-bot")


db = DB(CFG.database_url)


cache = RedisCache(CFG.redis_url)


tmdb = TMDBClient(CFG, db, cache, CFG.tmdb_token, CFG.language, log)


source = KinozalSource()


router = Router()
ADMIN_USERS_PAGE_SIZE = 12


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


if __name__ == "__main__":
    asyncio.run(runtime.main())
