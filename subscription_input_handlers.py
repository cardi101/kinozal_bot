from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message

from access_helpers import ensure_access_for_callback, ensure_access_for_message
from keyboards import sub_view_kb
from keyword_filters import normalize_keywords_input
from states import EditInputState
from subscription_text import sub_summary
from utils import compact_spaces, short


def register_subscription_input_handlers(router: Router, db: Any) -> None:
    @router.callback_query(F.data.startswith("sub:ask_years:"))
    async def cb_sub_ask_years(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await state.set_state(EditInputState.waiting_years)
        await state.update_data(sub_id=sub_id)
        await callback.message.answer(
            "Пришли диапазон лет в виде:\n"
            "<code>2020 2026</code>\n"
            "или слово <code>any</code>",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()

    @router.message(EditInputState.waiting_years)
    async def st_waiting_years(message: Message, state: FSMContext) -> None:
        if not await ensure_access_for_message(db, message):
            return
        data = await state.get_data()
        sub_id = int(data["sub_id"])
        if not db.subscription_belongs_to(sub_id, message.from_user.id):
            await state.clear()
            await message.answer("Подписка не найдена.")
            return
        raw = compact_spaces(message.text or "")
        if raw.lower() == "any":
            db.update_subscription(sub_id, year_from=None, year_to=None)
            await state.clear()
            await message.answer("Годы сброшены.")
            return
        parts = raw.split()
        if len(parts) != 2 or not all(p.isdigit() for p in parts):
            await message.answer("Нужно прислать два года через пробел, например: <code>2020 2026</code>", parse_mode=ParseMode.HTML)
            return
        year_from, year_to = int(parts[0]), int(parts[1])
        if year_from > year_to:
            year_from, year_to = year_to, year_from
        db.update_subscription(sub_id, year_from=year_from, year_to=year_to)
        await state.clear()
        sub = db.get_subscription(sub_id)
        await message.answer(sub_summary(db, sub), parse_mode=ParseMode.HTML, reply_markup=sub_view_kb(sub_id, sub))

    @router.callback_query(F.data.startswith("sub:edit_keywords:"))
    async def cb_sub_edit_keywords(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await state.set_state(EditInputState.waiting_keywords)
        await state.update_data(sub_id=sub_id)
        await callback.message.answer(
            "Пришли ключевые слова.\n"
            "Формат:\n"
            "<code>+marvel +space -cam -ts</code>\n\n"
            "Плюс — обязательно должно встретиться,\n"
            "минус — исключить.\n"
            "Для сброса пришли: <code>clear</code>",
            parse_mode=ParseMode.HTML,
        )
        await callback.answer()

    @router.message(EditInputState.waiting_keywords)
    async def st_waiting_keywords(message: Message, state: FSMContext) -> None:
        if not await ensure_access_for_message(db, message):
            return
        data = await state.get_data()
        sub_id = int(data["sub_id"])
        if not db.subscription_belongs_to(sub_id, message.from_user.id):
            await state.clear()
            await message.answer("Подписка не найдена.")
            return
        raw = compact_spaces(message.text or "")
        if raw.lower() == "clear":
            db.update_subscription(sub_id, include_keywords="", exclude_keywords="")
        else:
            include, exclude = normalize_keywords_input(raw)
            db.update_subscription(sub_id, include_keywords=include, exclude_keywords=exclude)
        await state.clear()
        sub = db.get_subscription(sub_id)
        await message.answer(sub_summary(db, sub), parse_mode=ParseMode.HTML, reply_markup=sub_view_kb(sub_id, sub))

    @router.callback_query(F.data.startswith("sub:rename:"))
    async def cb_sub_rename(callback: CallbackQuery, state: FSMContext) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await state.set_state(EditInputState.waiting_name)
        await state.update_data(sub_id=sub_id)
        await callback.message.answer("Пришли новое имя подписки.")
        await callback.answer()

    @router.message(EditInputState.waiting_name)
    async def st_waiting_name(message: Message, state: FSMContext) -> None:
        if not await ensure_access_for_message(db, message):
            return
        data = await state.get_data()
        sub_id = int(data["sub_id"])
        if not db.subscription_belongs_to(sub_id, message.from_user.id):
            await state.clear()
            await message.answer("Подписка не найдена.")
            return
        new_name = short(compact_spaces(message.text or ""), 100)
        if not new_name:
            await message.answer("Имя не может быть пустым.")
            return
        db.update_subscription(sub_id, name=new_name)
        await state.clear()
        sub = db.get_subscription(sub_id)
        await message.answer(sub_summary(db, sub), parse_mode=ParseMode.HTML, reply_markup=sub_view_kb(sub_id, sub))
