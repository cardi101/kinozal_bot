from typing import Any

from aiogram import F, Router
from aiogram.types import CallbackQuery

from access_helpers import ensure_access_for_callback
from dynamic_keyboards import genres_kb
from keyboards import format_kb, wizard_rating_kb, wizard_years_kb
from service_helpers import safe_edit


def register_subscription_wizard_handlers(router: Router, db: Any) -> None:
    @router.callback_query(F.data.startswith("wiztype:"))
    async def cb_wiz_type(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            _, sub_id_str, media_type = callback.data.split(":")
            sub_id = int(sub_id_str)
        except (ValueError, IndexError):
            await callback.answer("Неверный формат", show_alert=True)
            return
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Подписка не найдена", show_alert=True)
            return
        db.update_subscription(sub_id, media_type=media_type)
        sub = db.get_subscription(sub_id)
        await safe_edit(
            callback,
            "Шаг 2/5: выбери форматы. Можно отметить несколько.",
            format_kb(sub_id, sub, "wiz"),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("wizfmtdone:"))
    async def cb_wiz_fmt_done(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            sub_id = int(callback.data.split(":")[1])
        except (ValueError, IndexError):
            await callback.answer("Неверный формат", show_alert=True)
            return
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Подписка не найдена", show_alert=True)
            return
        await safe_edit(callback, "Шаг 3/5: выбери годы.", wizard_years_kb(sub_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("wizyear:"))
    async def cb_wiz_year(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            _, sub_id_str, code = callback.data.split(":")
            sub_id = int(sub_id_str)
        except (ValueError, IndexError):
            await callback.answer("Неверный формат", show_alert=True)
            return
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Подписка не найдена", show_alert=True)
            return
        if code == "any" or code == "manualskip":
            db.update_subscription(sub_id, year_from=None, year_to=None)
        else:
            db.update_subscription(sub_id, year_from=int(code), year_to=2100)
        await safe_edit(callback, "Шаг 4/5: минимальный рейтинг TMDB.", wizard_rating_kb(sub_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("wizrating:"))
    async def cb_wiz_rating(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            _, sub_id_str, code = callback.data.split(":")
            sub_id = int(sub_id_str)
        except (ValueError, IndexError):
            await callback.answer("Неверный формат", show_alert=True)
            return
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Подписка не найдена", show_alert=True)
            return
        db.update_subscription(sub_id, min_tmdb_rating=None if code == "none" else float(code))
        await safe_edit(callback, "Шаг 5/5: выбери жанры или сразу жми «Готово».", genres_kb(db, sub_id, 0))
        await callback.answer()
