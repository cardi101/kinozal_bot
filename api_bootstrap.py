import logging
import os
from dataclasses import dataclass
from typing import Any

from config import CFG
from db import DB
from kinozal_source import KinozalSource
from redis_cache import RedisCache
from services.admin_api_service import AdminApiService
from services.kinozal_service import KinozalService
from services.tmdb_service import TMDBService
from tmdb_client import TMDBClient


@dataclass(slots=True)
class ApiContainer:
    db: Any
    cache: Any
    tmdb: Any
    source: Any
    tmdb_service: TMDBService
    kinozal_service: KinozalService
    admin_api_service: AdminApiService
    log: logging.Logger
    admin_http_token: str


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("kinozal-api")


def build_api_container() -> ApiContainer:
    log = configure_logging()
    db = DB(CFG.database_url)
    cache = RedisCache(CFG.redis_url)
    tmdb = TMDBClient(CFG, db, cache, CFG.tmdb_token, CFG.language, log)
    source = KinozalSource()
    tmdb_service = TMDBService(tmdb)
    kinozal_service = KinozalService(source)
    admin_api_service = AdminApiService(db, tmdb_service, kinozal_service)
    return ApiContainer(
        db=db,
        cache=cache,
        tmdb=tmdb,
        source=source,
        tmdb_service=tmdb_service,
        kinozal_service=kinozal_service,
        admin_api_service=admin_api_service,
        log=log,
        admin_http_token=CFG.admin_http_token,
    )
