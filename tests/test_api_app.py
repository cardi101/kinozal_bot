from dataclasses import dataclass

import httpx
import pytest
from prometheus_client import CONTENT_TYPE_LATEST

from api_app import create_api_app


class FakeAdminApiService:
    def get_health(self):
        return {"status": "ok", "database_ok": True}

    def get_metrics_payload(self) -> bytes:
        return b"# HELP kinozal_bot_users_total Total users in database\nkinozal_bot_users_total 1.0\n"

    def get_user_subscriptions(self, user_id: int):
        if user_id == 404:
            raise LookupError("not found")
        return {"tg_user_id": user_id, "subscriptions": [{"id": 1}]}

    async def build_match_debug(self, kinozal_id: str, live: bool = True):
        if kinozal_id == "404":
            raise LookupError("not found")
        return {"kinozal_id": kinozal_id, "live": live, "explanation_html": "<b>ok</b>"}

    async def reparse_release(self, kinozal_id: str):
        if kinozal_id == "404":
            raise LookupError("not found")
        return {"kinozal_id": kinozal_id, "release_text_changed": True}


class FakeAsyncCloser:
    async def close(self):
        return None


@dataclass(slots=True)
class FakeContainer:
    admin_api_service: FakeAdminApiService
    tmdb: FakeAsyncCloser
    cache: FakeAsyncCloser
    source: FakeAsyncCloser
    admin_http_token: str


def build_test_client(admin_http_token: str = "secret") -> httpx.AsyncClient:
    container = FakeContainer(
        admin_api_service=FakeAdminApiService(),
        tmdb=FakeAsyncCloser(),
        cache=FakeAsyncCloser(),
        source=FakeAsyncCloser(),
        admin_http_token=admin_http_token,
    )
    transport = httpx.ASGITransport(app=create_api_app(container))
    return httpx.AsyncClient(transport=transport, base_url="http://testserver")


@pytest.mark.anyio
async def test_health_endpoint() -> None:
    async with build_test_client() as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.anyio
async def test_metrics_endpoint() -> None:
    async with build_test_client() as client:
        response = await client.get("/metrics")
    assert response.status_code == 200
    assert "kinozal_bot_users_total 1.0" in response.text
    assert response.headers["content-type"] == CONTENT_TYPE_LATEST


@pytest.mark.anyio
async def test_admin_endpoint_requires_token() -> None:
    async with build_test_client() as client:
        response = await client.get("/admin/subscriptions/1")
    assert response.status_code == 401


@pytest.mark.anyio
async def test_admin_endpoint_disabled_without_token_config() -> None:
    async with build_test_client(admin_http_token="") as client:
        response = await client.get("/admin/subscriptions/1")
    assert response.status_code == 503


@pytest.mark.anyio
async def test_admin_subscriptions_success() -> None:
    async with build_test_client() as client:
        response = await client.get("/admin/subscriptions/123", headers={"X-Admin-Token": "secret"})
    assert response.status_code == 200
    assert response.json()["tg_user_id"] == 123


@pytest.mark.anyio
async def test_admin_match_debug_success() -> None:
    async with build_test_client() as client:
        response = await client.get(
            "/admin/match-debug",
            params={"kinozal_id": "12345", "live": "true"},
            headers={"X-Admin-Token": "secret"},
        )
    assert response.status_code == 200
    assert response.json()["kinozal_id"] == "12345"


@pytest.mark.anyio
async def test_admin_reparse_success() -> None:
    async with build_test_client() as client:
        response = await client.post(
            "/admin/reparse/12345",
            headers={"X-Admin-Token": "secret"},
        )
    assert response.status_code == 200
    assert response.json()["release_text_changed"] is True
