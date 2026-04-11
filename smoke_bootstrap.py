import argparse
import asyncio
from typing import Any

from api_bootstrap import build_api_container
from app_bootstrap import build_app


async def _close_container(container: Any) -> None:
    try:
        await container.tmdb.close()
    finally:
        try:
            await container.cache.close()
        finally:
            try:
                await container.source.close()
            finally:
                if hasattr(container.db, "close"):
                    container.db.close()


async def _run(target: str) -> None:
    if target == "app":
        container = build_app()
    elif target == "api":
        container = build_api_container()
    else:
        raise RuntimeError(f"Unsupported smoke target: {target}")

    try:
        print(f"{target} bootstrap ok")
    finally:
        await _close_container(container)


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-check app and api bootstrap paths")
    parser.add_argument("target", choices=("app", "api"))
    args = parser.parse_args()
    asyncio.run(_run(args.target))


if __name__ == "__main__":
    main()
