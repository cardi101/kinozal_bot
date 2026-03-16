import html
from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from access_helpers import ensure_access_for_callback, ensure_access_for_message
from admin_helpers import is_admin, format_admin_user_line
from delivery_sender import send_item_to_user
from keyboards import main_menu_kb, subscriptions_list_kb, preset_kb, admin_invites_kb, admin_users_kb
from menu_views import show_main_menu
from latest_live_helpers import get_live_latest_items
from service_helpers import safe_edit
from subscription_text import sub_summary
from text_access import format_dt, user_access_state, format_access_expiry


def register_menu_handlers(router: Router, db: Any, source: Any, tmdb: Any, admin_users_page_size: int = 12) -> None:
    @router.message(Command("menu"))
    async def cmd_menu(message: Message) -> None:
        if not await ensure_access_for_message(db, message):
            return
        await message.answer("Главное меню", reply_markup=main_menu_kb(is_admin(message.from_user.id)))

    @router.callback_query(F.data == "menu:root")
    async def cb_menu_root(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        await show_main_menu(callback)
        await callback.answer()

    @router.callback_query(F.data == "menu:subs")
    async def cb_menu_subs(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        subs = db.list_user_subscriptions(callback.from_user.id)
        if not subs:
            await safe_edit(callback, "У тебя пока нет подписок.", main_menu_kb(is_admin(callback.from_user.id)))
            await callback.answer()
            return
        text = "Твои подписки:\n\n" + "\n\n".join(sub_summary(db, db.get_subscription(int(x["id"]))) for x in subs[:10])
        await safe_edit(callback, text, subscriptions_list_kb(subs))
        await callback.answer()

    @router.callback_query(F.data == "menu:new")
    async def cb_menu_new(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub = db.create_subscription(callback.from_user.id)
        text = (
            "Создаём новую подписку ✨\n\n"
            "Выбери готовый пресет или открой свою настройку."
        )
        await safe_edit(callback, text, preset_kb(int(sub["id"]), "new"))
        await callback.answer()

    @router.callback_query(F.data == "menu:latest")
    async def cb_menu_latest(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        items = await get_live_latest_items(source, tmdb, limit=5)
        if not items:
            await callback.answer("Пока пусто", show_alert=True)
            return
        await callback.message.answer("Последние сохранённые релизы:")
        for item in items:
            await send_item_to_user(db, callback.bot, callback.message.chat.id, item, None)
        await callback.answer()

    @router.callback_query(F.data == "menu:whoami")
    async def cb_menu_whoami(callback: CallbackQuery) -> None:
        await callback.answer()
        user = db.get_user(callback.from_user.id) or {}
        await callback.message.answer(
            f"Твой Telegram user_id: <code>{callback.from_user.id}</code>\n"
            f"Статус доступа: {html.escape(user_access_state(user))}\n"
            f"Доступ до: {html.escape(format_access_expiry(user.get('access_expires_at')))}",
            parse_mode=ParseMode.HTML,
        )

    @router.callback_query(F.data == "menu:admin_invites")
    async def cb_menu_admin_invites(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Недоступно", show_alert=True)
            return
        await safe_edit(
            callback,
            "Админ-раздел по доступам.\n\n"
            "Команды:\n"
            "<code>/create_invite 1 30 имя</code>\n"
            "<code>/grant USER_ID</code>\n"
            "<code>/revoke USER_ID</code>\n"
            "<code>/invites</code>",
            admin_invites_kb(),
        )
        await callback.answer()

    @router.callback_query(F.data == "admin:invites")
    async def cb_admin_invites(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Недоступно", show_alert=True)
            return
        invites = db.list_invites(10)
        if not invites:
            await safe_edit(callback, "Инвайтов пока нет.", admin_invites_kb())
            await callback.answer()
            return
        lines = ["Последние инвайты:\n"]
        for inv in invites:
            expires = format_dt(inv["expires_at"]) if inv["expires_at"] else "без срока"
            lines.append(
                f"<code>{inv['code']}</code>\n"
                f"uses={inv['uses_left']} | exp={expires}\n"
                f"{html.escape(inv.get('note') or '—')}\n"
            )
        await safe_edit(callback, "\n".join(lines), admin_invites_kb())
        await callback.answer()

    @router.callback_query(F.data == "menu:admin_users")
    async def cb_menu_admin_users(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Недоступно", show_alert=True)
            return
        users = db.list_users_with_stats(limit=admin_users_page_size, offset=0)
        total = db.count_users()
        if not users:
            await safe_edit(callback, "Пользователей пока нет.", admin_users_kb(0, False, False))
            await callback.answer()
            return
        pages = max(1, (total + admin_users_page_size - 1) // admin_users_page_size)
        lines = [f"👥 Пользователи — страница 1/{pages}", ""]
        lines.extend(format_admin_user_line(user) for user in users)
        await safe_edit(callback, "\n\n".join(lines), admin_users_kb(0, False, total > admin_users_page_size))
        await callback.answer()

    @router.callback_query(F.data.startswith("admin:users:"))
    async def cb_admin_users(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Недоступно", show_alert=True)
            return
        try:
            page = max(0, int(callback.data.split(":")[2]))
        except Exception:
            page = 0
        offset = page * admin_users_page_size
        total = db.count_users()
        users = db.list_users_with_stats(limit=admin_users_page_size, offset=offset)
        if not users and page > 0:
            page = max(0, (max(1, total) - 1) // admin_users_page_size)
            offset = page * admin_users_page_size
            users = db.list_users_with_stats(limit=admin_users_page_size, offset=offset)
        pages = max(1, (total + admin_users_page_size - 1) // admin_users_page_size)
        lines = [f"👥 Пользователи — страница {page + 1}/{pages}", ""]
        if users:
            lines.extend(format_admin_user_line(user) for user in users)
        else:
            lines.append("Пользователей пока нет.")
        await safe_edit(callback, "\n\n".join(lines), admin_users_kb(page, page > 0, (page + 1) * admin_users_page_size < total))
        await callback.answer()
