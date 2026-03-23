import html
from datetime import datetime, timezone
from typing import Any

from aiogram import Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from access_helpers import ensure_access_for_message


def register_history_handlers(router: Router, db: Any) -> None:
    @router.message(Command("history"))
    async def cmd_history(message: Message) -> None:
        if not await ensure_access_for_message(db, message):
            return
        items = db.get_user_delivery_history(message.from_user.id, limit=15)
        if not items:
            await message.answer("История доставок пуста.")
            return
        lines = ["📋 <b>Последние уведомления:</b>\n"]
        for i, item in enumerate(items, 1):
            title = item.get("tmdb_title") or item.get("source_title") or "Без названия"
            try:
                dt = datetime.fromtimestamp(int(item["delivered_at"]), tz=timezone.utc)
                date_str = dt.strftime("%d.%m")
            except (TypeError, ValueError, OSError):
                date_str = "?"
            link = item.get("source_link")
            if link:
                lines.append(
                    f'{i}. <a href="{html.escape(link, quote=True)}">{html.escape(title)}</a> • {date_str}'
                )
            else:
                lines.append(f"{i}. {html.escape(title)} • {date_str}")
        await message.answer(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
