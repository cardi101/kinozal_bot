import logging
from typing import Optional

from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import CallbackQuery, InlineKeyboardMarkup

from utils import compact_spaces, short

log = logging.getLogger("kinozal-news-bot")


def _cfg():
    from config import CFG

    return CFG


async def safe_edit(callback: CallbackQuery, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None) -> None:
    cfg = _cfg()
    try:
        await callback.message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=cfg.disable_preview,
        )
    except TelegramBadRequest:
        await callback.message.answer(
            text,
            reply_markup=reply_markup,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=cfg.disable_preview,
        )


def _exc_brief(exc: Exception) -> str:
    text = compact_spaces(f"{type(exc).__name__}: {exc}")
    return short(text, 500)


async def send_admins_text(bot: Bot, text: str) -> None:
    cfg = _cfg()
    if not cfg.admin_ids:
        return
    for admin_id in cfg.admin_ids:
        try:
            await bot.send_message(
                int(admin_id),
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=cfg.disable_preview,
            )
        except Exception:
            log.warning("admin alert send failed admin=%s", admin_id, exc_info=True)
