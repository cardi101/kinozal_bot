import logging
from typing import Any, Dict, Optional, Sequence

import httpx
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.types import BufferedInputFile

from config import CFG
from delivery_formatting import item_message
from text_access import html_to_plain_text
from utils import short

log = logging.getLogger("kinozal-news-bot")


async def _build_poster_file(poster_url: str, item_id: Any) -> Optional[BufferedInputFile]:
    try:
        timeout = httpx.Timeout(CFG.request_timeout)
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(
                poster_url,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            resp.raise_for_status()

            content_type = (resp.headers.get("content-type") or "").lower()
            if not content_type.startswith("image/"):
                log.info(
                    "Skip poster with non-image content-type for item=%s url=%s content-type=%s",
                    item_id,
                    poster_url,
                    content_type,
                )
                return None

            content = resp.content
            if not content:
                log.info(
                    "Skip empty poster body for item=%s url=%s",
                    item_id,
                    poster_url,
                )
                return None

            ext = "jpg"
            if "png" in content_type:
                ext = "png"
            elif "webp" in content_type:
                ext = "webp"
            elif "jpeg" in content_type or "jpg" in content_type:
                ext = "jpg"

            filename = f"poster_{item_id}.{ext}"
            return BufferedInputFile(content, filename=filename)
    except Exception:
        log.warning(
            "Failed to fetch poster for item=%s url=%s",
            item_id,
            poster_url,
            exc_info=True,
        )
        return None


async def send_item_to_user(
    db: Any,
    bot: Bot,
    tg_user_id: int,
    item: Dict[str, Any],
    subs: Optional[Sequence[Dict[str, Any]]],
) -> None:
    text = item_message(db, item, subs)
    plain_text = html_to_plain_text(text)
    poster_url = item.get("tmdb_poster_url")
    full_html_text = short(text, 3900)
    full_plain_text = short(plain_text, 3900)
    caption_html = short(text, 1000)

    if poster_url:
        poster_file = await _build_poster_file(str(poster_url), item.get("id"))
        if poster_file:
            try:
                await bot.send_photo(
                    tg_user_id,
                    photo=poster_file,
                    caption=caption_html,
                    parse_mode=ParseMode.HTML,
                )
                return
            except Exception:
                log.warning(
                    "send_photo(uploaded poster) failed for user=%s item=%s",
                    tg_user_id,
                    item.get("id"),
                    exc_info=True,
                )

    try:
        await bot.send_message(
            tg_user_id,
            text=full_html_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
    except Exception:
        log.warning(
            "send_message HTML failed for user=%s item=%s",
            tg_user_id,
            item.get("id"),
            exc_info=True,
        )
        await bot.send_message(
            tg_user_id,
            text=full_plain_text,
            disable_web_page_preview=CFG.disable_preview,
        )
