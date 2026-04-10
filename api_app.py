from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, Optional

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.responses import PlainTextResponse

if TYPE_CHECKING:
    from api_bootstrap import ApiContainer


def create_api_app(container: Optional["ApiContainer"] = None) -> FastAPI:
    runtime_container = container
    if runtime_container is None:
        from api_bootstrap import build_api_container

        runtime_container = build_api_container()

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            await runtime_container.tmdb.close()
            await runtime_container.cache.close()
            await runtime_container.source.close()

    app = FastAPI(
        title="Kinozal Bot Admin API",
        version="1.0.0",
        lifespan=lifespan,
    )

    async def require_admin_token(x_admin_token: Optional[str] = Header(default=None)) -> None:
        expected = runtime_container.admin_http_token.strip()
        if not expected:
            raise HTTPException(status_code=503, detail="admin http endpoints disabled")
        if x_admin_token != expected:
            raise HTTPException(status_code=401, detail="invalid admin token")

    @app.get("/health")
    async def health() -> Any:
        return runtime_container.admin_api_service.get_health()

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return runtime_container.admin_api_service.get_metrics_text()

    @app.get("/admin/subscriptions/{user_id}", dependencies=[Depends(require_admin_token)])
    async def get_user_subscriptions(user_id: int) -> Any:
        try:
            return runtime_container.admin_api_service.get_user_subscriptions(user_id)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.get("/admin/match-debug", dependencies=[Depends(require_admin_token)])
    async def match_debug(
        kinozal_id: str = Query(..., min_length=1),
        live: bool = Query(True),
    ) -> Any:
        try:
            return await runtime_container.admin_api_service.build_match_debug(kinozal_id, live=live)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @app.post("/admin/reparse/{kinozal_id}", dependencies=[Depends(require_admin_token)])
    async def reparse_release(kinozal_id: str) -> Any:
        try:
            return await runtime_container.admin_api_service.reparse_release(kinozal_id)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    return app
