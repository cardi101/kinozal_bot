from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI

from app_version import APP_VERSION
from services.alertmanager_webhook_service import AlertmanagerWebhookService


def create_alert_webhook_app(service: Optional[AlertmanagerWebhookService] = None) -> FastAPI:
    runtime_service = service
    if runtime_service is None:
        from alert_webhook_bootstrap import build_alertmanager_webhook_service

        runtime_service = build_alertmanager_webhook_service()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await runtime_service.close()

    app = FastAPI(
        title="Kinozal Alertmanager Webhook",
        version=APP_VERSION,
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/webhooks/alertmanager")
    async def alertmanager_webhook(payload: dict[str, Any]) -> Any:
        return await runtime_service.handle_webhook(payload)

    return app
