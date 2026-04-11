from dataclasses import dataclass

import httpx
import pytest

from alert_webhook_app import create_alert_webhook_app


class FakeAlertWebhookService:
    def __init__(self) -> None:
        self.calls = []

    async def handle_webhook(self, payload):
        self.calls.append(payload)
        return {"ok": True, "alerts": len(payload.get("alerts") or [])}

    async def close(self) -> None:
        return None


@dataclass(slots=True)
class FakeContainer:
    service: FakeAlertWebhookService


def build_test_client(service: FakeAlertWebhookService | None = None) -> httpx.AsyncClient:
    runtime_service = service or FakeAlertWebhookService()
    transport = httpx.ASGITransport(app=create_alert_webhook_app(runtime_service))
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.anyio
async def test_alert_webhook_health_endpoint() -> None:
    async with build_test_client() as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@pytest.mark.anyio
async def test_alert_webhook_forwards_payload() -> None:
    service = FakeAlertWebhookService()
    async with build_test_client(service) as client:
        response = await client.post(
            "/webhooks/alertmanager",
            json={
                "status": "firing",
                "alerts": [{"status": "firing", "labels": {"alertname": "KinozalApiDown"}}],
            },
        )
    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert len(service.calls) == 1
    assert service.calls[0]["status"] == "firing"
