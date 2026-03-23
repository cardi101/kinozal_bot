import html
from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery

from access_helpers import ensure_access_for_callback
from keyboards import unmute_title_kb


def register_mute_title_handlers(router: Router, db: Any) -> None:
    @router.callback_query(F.data.startswith("mute_title:"))
    async def cb_mute_title(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            tmdb_id = int(callback.data.split(":")[1])
        except (IndexError, ValueError):
            await callback.answer("Неверный формат", show_alert=True)
            return

        db.mute_title(callback.from_user.id, tmdb_id)

        try:
            await callback.message.edit_reply_markup(reply_markup=unmute_title_kb(tmdb_id))
        except Exception:
            pass

        await callback.answer("🔕 Уведомления об этом названии отключены", show_alert=False)

    @router.callback_query(F.data.startswith("unmute_title:"))
    async def cb_unmute_title(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            tmdb_id = int(callback.data.split(":")[1])
        except (IndexError, ValueError):
            await callback.answer("Неверный формат", show_alert=True)
            return

        db.unmute_title(callback.from_user.id, tmdb_id)

        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass

        await callback.answer("🔔 Уведомления восстановлены", show_alert=False)
