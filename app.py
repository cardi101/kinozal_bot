import asyncio

from app_bootstrap import build_app


if __name__ == "__main__":
    asyncio.run(build_app().runtime.main())
