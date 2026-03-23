from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from access_helpers import ensure_access_for_callback, ensure_access_for_message
from admin_helpers import is_admin
from keyboards import main_menu_kb, subscriptions_list_kb, preset_kb, sub_view_kb, wizard_type_kb
from service_helpers import safe_edit
from subscription_presets import apply_subscription_preset
from subscription_text import sub_summary


def register_subscription_basic_handlers(router: Router, db: Any) -> None:
    @router.message(Command("subs"))
    async def cmd_subs(message: Message) -> None:
        if not await ensure_access_for_message(db, message):
            return
        subs = db.list_user_subscriptions(message.from_user.id)
        if not subs:
            await message.answer("У тебя пока нет подписок.", reply_markup=main_menu_kb(is_admin(message.from_user.id)))
            return
        text = "Твои подписки:\n\n" + "\n\n".join(sub_summary(db, db.get_subscription(int(x["id"]))) for x in subs[:10])
        await message.answer(text, reply_markup=subscriptions_list_kb(subs), parse_mode=ParseMode.HTML)

    @router.callback_query(F.data == "menu:root")
    @router.callback_query(F.data == "menu:subs")
    @router.callback_query(F.data == "menu:new")
    @router.callback_query(F.data == "menu:latest")
    @router.callback_query(F.data == "menu:whoami")
    @router.callback_query(F.data == "menu:admin_invites")
    @router.callback_query(F.data == "admin:invites")
    @router.callback_query(F.data == "menu:admin_users")
    @router.callback_query(F.data.startswith("admin:users:"))
    @router.callback_query(F.data.startswith("sub:view:"))
    async def cb_sub_view(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
        await callback.answer()

    @router.callback_query(F.data.startswith("sub:toggle:"))
    async def cb_sub_toggle(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        sub = db.get_subscription(sub_id)
        db.update_subscription(sub_id, is_enabled=0 if sub.get("is_enabled") else 1)
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
        await callback.answer("Готово")

    @router.callback_query(F.data.startswith("sub:delete:"))
    async def cb_sub_delete(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        db.delete_subscription(sub_id)
        subs = db.list_user_subscriptions(callback.from_user.id)
        if subs:
            await safe_edit(callback, "Подписка удалена.", subscriptions_list_kb(subs))
        else:
            await safe_edit(callback, "Подписка удалена. Список пуст.", main_menu_kb(is_admin(callback.from_user.id)))
        await callback.answer("Удалено")

    @router.callback_query(F.data.startswith("sub:edit_presets:"))
    async def cb_sub_edit_presets(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери готовый пресет или оставь свою ручную настройку.", preset_kb(sub_id, "edit"))
        await callback.answer()

    @router.callback_query(F.data.startswith("subpreset:"))
    async def cb_sub_preset_apply(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        try:
            _, sub_id_str, flow, preset_key = callback.data.split(":")
            sub_id = int(sub_id_str)
        except (ValueError, IndexError):
            await callback.answer("Неверный формат", show_alert=True)
            return
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        if preset_key == "custom":
            if flow == "new":
                db.update_subscription(sub_id, content_filter="any", country_codes="", exclude_country_codes="", preset_key="")
                await safe_edit(
                    callback,
                    "Создаём новую подписку ✨\n\nШаг 1/5: выбери, что ловить.",
                    wizard_type_kb(sub_id),
                )
                await callback.answer("Переходим к своей настройке")
                return
            sub = db.get_subscription(sub_id)
            await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
            await callback.answer("Оставил текущую настройку")
            return

        sub = apply_subscription_preset(db, sub_id, preset_key)
        if not sub:
            await callback.answer("Пресет не найден", show_alert=True)
            return

        suffix = "Пресет применён. Подправь что нужно вручную." if flow == "edit" else "Пресет создан. Можно пользоваться сразу или подправить вручную."
        await safe_edit(callback, f"{sub_summary(db, sub)}\n\n<i>{suffix}</i>", sub_view_kb(sub_id, sub))
        await callback.answer("Пресет применён")
