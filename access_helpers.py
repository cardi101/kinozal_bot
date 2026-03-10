from typing import Any

from aiogram.types import CallbackQuery, Message

from admin_helpers import is_admin
from config import CFG
from text_access import require_access_message


async def ensure_access_for_message(db: Any, message: Message) -> bool:
    user_id = message.from_user.id
    db.ensure_user(
        user_id,
        message.from_user.username or "",
        message.from_user.first_name or "",
        auto_grant=(CFG.allow_mode == "open" or is_admin(user_id)),
    )
    if db.user_has_access(user_id):
        return True
    await message.answer(require_access_message())
    return False


async def ensure_access_for_callback(db: Any, callback: CallbackQuery) -> bool:
    user_id = callback.from_user.id
    db.ensure_user(
        user_id,
        callback.from_user.username or "",
        callback.from_user.first_name or "",
        auto_grant=(CFG.allow_mode == "open" or is_admin(user_id)),
    )
    if db.user_has_access(user_id):
        return True
    await callback.answer("Нет доступа", show_alert=True)
    return False
