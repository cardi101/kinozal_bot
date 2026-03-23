import html
from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

from access_helpers import ensure_access_for_callback, ensure_access_for_message
from service_helpers import safe_edit
from text_access import human_media_type


def _muted_text_and_kb(items: list):
    if not items:
        text = "🔔 Список заглушённых названий пуст.\n\nКнопка 🔕 появляется под каждым уведомлением."
        kb = InlineKeyboardBuilder()
        kb.button(text="◀️ В меню", callback_data="menu:root")
        return text, kb.as_markup()

    lines = ["🔕 <b>Заглушённые названия</b>\n"]
    kb = InlineKeyboardBuilder()
    for item in items:
        title = item.get("title") or f"TMDB #{item['tmdb_id']}"
        media_type = item.get("media_type") or "movie"
        media = human_media_type(media_type)
        lines.append(f"  • {html.escape(title)} <i>({media})</i>")
        kb.button(
            text=f"🔔 {title[:35]}",
            callback_data=f"muted:remove:{item['tmdb_id']}",
        )

    lines.append("\nНажми кнопку ниже чтобы снять заглушку:")
    kb.button(text="◀️ В меню", callback_data="menu:root")
    kb.adjust(1)
    return "\n".join(lines), kb.as_markup()


def register_muted_list_handlers(router: Router, db: Any) -> None:
    @router.message(Command("muted"))
    async def cmd_muted(message: Message) -> None:
        if not await ensure_access_for_message(db, message):
            return
        items = db.list_muted_titles(message.from_user.id)
        text, kb = _muted_text_and_kb(items)
        await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=kb)

    @router.callback_query(F.data == "menu:muted")
    async def cb_menu_muted(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        items = db.list_muted_titles(callback.from_user.id)
        text, kb = _muted_text_and_kb(items)
        await safe_edit(callback, text, kb)
        await callback.answer()

    @router.callback_query(F.data.startswith("muted:remove:"))
    async def cb_muted_remove(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            tmdb_id = int(callback.data.split(":")[2])
        except (IndexError, ValueError):
            await callback.answer("Неверный формат", show_alert=True)
            return
        db.unmute_title(callback.from_user.id, tmdb_id)
        items = db.list_muted_titles(callback.from_user.id)
        text, kb = _muted_text_and_kb(items)
        try:
            await callback.message.edit_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception:
            pass
        await callback.answer("🔔 Заглушка снята")
