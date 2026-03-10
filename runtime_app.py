import asyncio
import inspect
import logging
from typing import Any, Optional

from aiogram import Bot, Dispatcher, Router


class AppRuntime:
    def __init__(
        self,
        cfg: Any,
        router: Router,
        db: Any,
        source: Any,
        tmdb: Any,
        cache: Any,
        poller_fn: Any,
        log: logging.Logger,
        preset_rollout_version: int,
    ) -> None:
        self.cfg = cfg
        self.router = router
        self.db = db
        self.source = source
        self.tmdb = tmdb
        self.cache = cache
        self.poller_fn = poller_fn
        self.log = log
        self.preset_rollout_version = preset_rollout_version
        self.bot_instance: Optional[Bot] = None
        self.poller_task: Optional[asyncio.Task] = None

    async def on_startup(self, bot: Bot) -> None:
        if self.cfg.tmdb_token:
            try:
                await self.tmdb.ensure_genres(force=False)
            except Exception:
                self.log.exception("TMDB genre sync failed on startup")

        try:
            updated_preset_subs = self.db.rollout_existing_preset_subscriptions(self.preset_rollout_version)
            if updated_preset_subs:
                self.log.info("Preset rollout applied to %s existing subscriptions", updated_preset_subs)
        except Exception:
            self.log.exception("Preset rollout failed on startup")

        params_count = len(inspect.signature(self.poller_fn).parameters)
        if params_count == 4:
            self.poller_task = asyncio.create_task(
                self.poller_fn(self.db, self.source, self.tmdb, bot)
            )
        elif params_count == 1:
            self.poller_task = asyncio.create_task(self.poller_fn(bot))
        else:
            raise RuntimeError(f"Unsupported poller signature: expected 1 or 4 params, got {params_count}")

        self.log.info("Bot started")

    async def on_shutdown(self, *_: Any) -> None:
        if self.poller_task:
            self.poller_task.cancel()
            try:
                await self.poller_task
            except Exception:
                pass

        await self.tmdb.close()
        await self.cache.close()
        await self.source.close()
        self.log.info("Bot stopped")

    async def main(self) -> None:
        self.bot_instance = Bot(self.cfg.bot_token)
        dp = Dispatcher()
        dp.include_router(self.router)
        dp.startup.register(self.on_startup)
        dp.shutdown.register(self.on_shutdown)
        await dp.start_polling(self.bot_instance)
