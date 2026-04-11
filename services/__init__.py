from importlib import import_module
from typing import TYPE_CHECKING, Any

__all__ = [
    "DeliveryService",
    "KinozalService",
    "SubscriptionService",
    "TMDBService",
    "WorkerService",
]

_MODULE_BY_EXPORT = {
    "DeliveryService": "services.delivery_service",
    "KinozalService": "services.kinozal_service",
    "SubscriptionService": "services.subscription_service",
    "TMDBService": "services.tmdb_service",
    "WorkerService": "services.worker_service",
}


if TYPE_CHECKING:
    from .delivery_service import DeliveryService
    from .kinozal_service import KinozalService
    from .subscription_service import SubscriptionService
    from .tmdb_service import TMDBService
    from .worker_service import WorkerService


def __getattr__(name: str) -> Any:
    module_name = _MODULE_BY_EXPORT.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
