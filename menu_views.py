from typing import Any, Optional

from aiogram.types import CallbackQuery, Message

from admin_helpers import is_admin
from keyboards import main_menu_kb
from service_helpers import safe_edit


async def show_main_menu(target: Message | CallbackQuery, db: Optional[Any] = None) -> None:
    uid = target.from_user.id
    quiet_active = False
    if db is not None:
        q_start, q_end = db.get_user_quiet_hours(uid)
        quiet_active = q_start is not None and q_end is not None
    text = "Главное меню"
    kb = main_menu_kb(is_admin(uid), quiet_active=quiet_active)
    if isinstance(target, CallbackQuery):
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)
