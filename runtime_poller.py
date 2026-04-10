from typing import Any

from aiogram import Bot

from repositories import WorkerRepository
from services import DeliveryService, KinozalService, SubscriptionService, TMDBService, WorkerService


def _build_worker_service(db: Any, source: Any, tmdb: Any, bot: Bot) -> WorkerService:
    repository = WorkerRepository(db)
    return WorkerService(
        repository=repository,
        kinozal_service=KinozalService(source),
        tmdb_service=TMDBService(tmdb),
        subscription_service=SubscriptionService(repository),
        delivery_service=DeliveryService(repository, bot),
        bot=bot,
    )


async def process_new_items(db: Any, source: Any, tmdb: Any, bot: Bot) -> None:
    await _build_worker_service(db, source, tmdb, bot).process_new_items()


async def poller(db: Any, source: Any, tmdb: Any, bot: Bot) -> None:
    await _build_worker_service(db, source, tmdb, bot).poll_forever()
