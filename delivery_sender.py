import logging
from typing import Any, Dict, Optional, Sequence

from aiogram import Bot
from aiogram.enums import ParseMode

from config import CFG
from delivery_formatting import item_message
from text_access import html_to_plain_text
from utils import short

log = logging.getLogger("kinozal-news-bot")


async def send_item_to_user(db: Any, bot: Bot, tg_user_id: int, item: Dict[str, Any], subs: Optional[Sequence[Dict[str, Any]]]) -> None:
    text = item_message(db, item, subs)
    plain_text = html_to_plain_text(text)
    poster = item.get("tmdb_poster_url")
    full_html_text = short(text, 3900)
    full_plain_text = short(plain_text, 3900)
    caption_html = short(text, 1000)
    caption_plain = short(plain_text, 1000)

    if poster:
        try:
            await bot.send_photo(
                tg_user_id,
                photo=poster,
                caption=caption_html,
                parse_mode=ParseMode.HTML,
            )
            return
        except Exception:
            log.warning("send_photo failed for user=%s item=%s", tg_user_id, item.get("id"), exc_info=True)
            try:
                await bot.send_photo(
                    tg_user_id,
                    photo=poster,
                    caption=caption_plain,
                )
                return
            except Exception:
                log.warning("send_photo plain fallback failed for user=%s item=%s", tg_user_id, item.get("id"), exc_info=True)

    try:
        await bot.send_message(
            tg_user_id,
            text=full_html_text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )
    except Exception:
        log.warning("send_message HTML failed for user=%s item=%s", tg_user_id, item.get("id"), exc_info=True)
        await bot.send_message(
            tg_user_id,
            text=full_plain_text,
            disable_web_page_preview=CFG.disable_preview,
        )
