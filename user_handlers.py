import html
from typing import Any

from aiogram import Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject, CommandStart
from aiogram.types import Message

from access_helpers import ensure_access_for_message
from admin_helpers import is_admin
from config import CFG
from delivery_sender import send_item_to_user
from keyboards import main_menu_kb
from text_access import format_access_expiry, require_access_message, user_access_state


def register_user_handlers(router: Router, db: Any) -> None:
    @router.message(CommandStart(deep_link=True))
    @router.message(CommandStart())
    async def cmd_start(message: Message, command: CommandObject) -> None:
        user_id = message.from_user.id
        db.ensure_user(
            user_id,
            message.from_user.username or "",
            message.from_user.first_name or "",
            auto_grant=(CFG.allow_mode == "open" or is_admin(user_id)),
        )

        code = (command.args or "").strip() if command else ""
        if code and CFG.allow_mode == "invite" and not db.user_has_access(user_id):
            if db.redeem_invite(code, user_id):
                await message.answer("✅ Доступ активирован. Добро пожаловать.", reply_markup=main_menu_kb(is_admin(user_id)))
                return
            await message.answer("❌ Инвайт не подошёл: просрочен, исчерпан или неверный.")
            return

        if not db.user_has_access(user_id):
            await message.answer(
                "Привет. Это бот для персональных новостей Kinozal.\n\n" + require_access_message()
            )
            return

        text = (
            "Привет ✨\n"
            "Тут можно настроить личные выборки новинок с Kinozal,\n"
            "дотянуть жанры/рейтинг/постер из TMDB и получать только то, что подходит тебе."
        )
        await message.answer(text, reply_markup=main_menu_kb(is_admin(user_id)))

    @router.message(Command("whoami"))
    async def cmd_whoami(message: Message) -> None:
        db.ensure_user(
            message.from_user.id,
            message.from_user.username or "",
            message.from_user.first_name or "",
            auto_grant=(CFG.allow_mode == "open" or is_admin(message.from_user.id)),
        )
        user = db.get_user(message.from_user.id) or {}
        await message.answer(
            f"Твой Telegram user_id: <code>{message.from_user.id}</code>\n"
            f"Статус доступа: {html.escape(user_access_state(user))}\n"
            f"Доступ до: {html.escape(format_access_expiry(user.get('access_expires_at')))}",
            parse_mode=ParseMode.HTML,
        )

    @router.message(Command("latest"))
    async def cmd_latest(message: Message) -> None:
        if not await ensure_access_for_message(db, message):
            return
        items = db.get_last_items(5)
        if not items:
            await message.answer("Пока ещё нет сохранённых релизов.")
            return
        for item in items:
            await send_item_to_user(db, message.bot, message.chat.id, item, None)
