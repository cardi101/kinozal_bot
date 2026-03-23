import html
from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from access_helpers import ensure_access_for_callback, ensure_access_for_message
from keyboards import quiet_hours_kb


def _quiet_status(q_start, q_end) -> str:
    if q_start is not None and q_end is not None:
        return f"Сейчас: {q_start:02d}:00 – {q_end:02d}:00 UTC"
    return "Сейчас: отключён"


def register_quiet_hours_handlers(router: Router, db: Any) -> None:
    @router.message(Command("quiet"))
    async def cmd_quiet(message: Message) -> None:
        if not await ensure_access_for_message(db, message):
            return
        args = (message.text or "").split()[1:]
        if args and args[0].lower() in ("off", "отключить", "0"):
            db.set_user_quiet_hours(message.from_user.id, None, None)
            await message.answer("🔔 Тихий режим отключён.")
            return
        if len(args) >= 2:
            try:
                start_h = int(args[0]) % 24
                end_h = int(args[1]) % 24
                if start_h == end_h:
                    await message.answer("Начало и конец не могут совпадать.")
                    return
                db.set_user_quiet_hours(message.from_user.id, start_h, end_h)
                await message.answer(
                    f"🌙 Тихий режим: <b>{start_h:02d}:00 – {end_h:02d}:00 UTC</b>\n"
                    f"Уведомления будут накапливаться и придут после {end_h:02d}:00 UTC.",
                    parse_mode=ParseMode.HTML,
                )
                return
            except ValueError:
                pass
        q_start, q_end = db.get_user_quiet_hours(message.from_user.id)
        status = _quiet_status(q_start, q_end)
        await message.answer(
            f"🌙 <b>Тихий режим</b>\n{status}\n\n"
            f"Выбери пресет или введи: <code>/quiet ЧЧ ЧЧ</code> (UTC)\n"
            f"Отключить: <code>/quiet off</code>",
            parse_mode=ParseMode.HTML,
            reply_markup=quiet_hours_kb(q_start, q_end),
        )

    @router.callback_query(F.data.startswith("quiet:"))
    async def cb_quiet(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        parts = callback.data.split(":")
        if len(parts) < 2:
            await callback.answer("Неверный формат", show_alert=True)
            return
        if parts[1] == "off":
            db.set_user_quiet_hours(callback.from_user.id, None, None)
            q_start, q_end = None, None
        else:
            try:
                start_h = int(parts[1]) % 24
                end_h = int(parts[2]) % 24
            except (IndexError, ValueError):
                await callback.answer("Неверный формат", show_alert=True)
                return
            db.set_user_quiet_hours(callback.from_user.id, start_h, end_h)
            q_start, q_end = start_h, end_h

        status = _quiet_status(q_start, q_end)
        try:
            await callback.message.edit_text(
                f"🌙 <b>Тихий режим</b>\n{status}\n\n"
                f"Выбери пресет или введи: <code>/quiet ЧЧ ЧЧ</code> (UTC)\n"
                f"Отключить: <code>/quiet off</code>",
                parse_mode=ParseMode.HTML,
                reply_markup=quiet_hours_kb(q_start, q_end),
            )
        except Exception:
            pass
        await callback.answer("Сохранено")
