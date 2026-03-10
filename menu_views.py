from aiogram.types import CallbackQuery, Message

from admin_helpers import is_admin
from keyboards import main_menu_kb
from service_helpers import safe_edit


async def show_main_menu(target: Message | CallbackQuery) -> None:
    uid = target.from_user.id if isinstance(target, CallbackQuery) else target.from_user.id
    text = "Главное меню"
    kb = main_menu_kb(is_admin(uid))
    if isinstance(target, CallbackQuery):
        await safe_edit(target, text, kb)
    else:
        await target.answer(text, reply_markup=kb)
