import asyncio
import html
import logging
from typing import Any, List

from aiogram import Router
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from admin_helpers import is_admin, format_admin_user_line, format_admin_user_details, parse_command_payload
from config import CFG
from text_access import format_dt, format_access_expiry
from utils import compact_spaces, short, utc_ts


log = logging.getLogger(__name__)


def register_admin_access_handlers(router: Router, db: Any, admin_users_page_size: int = 12) -> None:
    @router.message(Command("cleanup_versions"))
    async def cmd_cleanup_versions(message: Message) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        parts = compact_spaces(message.text or "").split()
        confirm = any(part.lower() in {"confirm", "run", "apply", "yes"} for part in parts[1:])
        keep_last = CFG.cleanup_versions_keep_last
        for part in parts[1:]:
            if part.isdigit():
                keep_last = max(1, int(part))
                break

        summary = db.cleanup_old_versions(
            keep_last=keep_last,
            dry_run=not confirm,
            preview_limit=CFG.cleanup_versions_preview_limit,
        )

        lines = [
            ("🧪 Предпросмотр чистки старых версий" if not confirm else "🧹 Чистка старых версий выполнена"),
            f"Keep-last: {summary['keep_last']}",
            f"Групп релизов: {summary['groups']}",
            f"Версий к архивированию: {summary['versions_to_archive']}",
            f"Items к архивированию: {summary['items_to_archive']}",
        ]

        samples = summary.get("sample_groups") or []
        if samples:
            lines.append("")
            lines.append("Примеры:")
            for group in samples[: CFG.cleanup_versions_preview_limit]:
                title = short(compact_spaces(str(group.get("title") or "")), 90)
                lines.append(
                    f"• {group['kinozal_id']} | {group['media_type']} | keep {group['keep_ids']} | archive {group['archive_ids']}\n  {html.escape(title)}"
                )
        else:
            lines.append("")
            lines.append("Старых версий не найдено.")

        if not confirm:
            lines.append("")
            lines.append(f"Для выполнения: /cleanup_versions {keep_last} confirm")

        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)

    @router.message(Command("cleanup_duplicates"))
    async def cmd_cleanup_duplicates(message: Message) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        parts = compact_spaces(message.text or "").split()
        confirm = any(part.lower() in {"confirm", "run", "apply", "yes"} for part in parts[1:])
        summary = db.cleanup_exact_duplicate_items(
            dry_run=not confirm,
            preview_limit=CFG.cleanup_duplicates_preview_limit,
        )

        lines = [
            ("🧪 Предпросмотр чистки дублей" if not confirm else "🧹 Чистка дублей выполнена"),
            f"Групп дублей: {summary['groups']}",
            f"Лишних items: {summary['items_to_delete']}",
            f"Доставок к переносу: {summary['deliveries_to_migrate']}",
        ]

        samples = summary.get("sample_groups") or []
        if samples:
            lines.append("")
            lines.append("Примеры:")
            for group in samples[: CFG.cleanup_duplicates_preview_limit]:
                title = short(compact_spaces(str(group.get("title") or "")), 90)
                lines.append(
                    f"• {group['kinozal_id']} | {group['media_type']} | keep #{group['keep_id']} | del {group['remove_ids']}\n  {html.escape(title)}"
                )
        else:
            lines.append("")
            lines.append("Дублей не найдено.")

        if not confirm:
            lines.append("")
            lines.append("Для выполнения: /cleanup_duplicates confirm")

        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)

    @router.message(Command("create_invite"))
    async def cmd_create_invite(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        parts = compact_spaces(message.text or "").split(maxsplit=3)
        uses = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
        days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 30
        note = parts[3] if len(parts) > 3 else ""
        invite = db.create_invite(message.from_user.id, uses, days, note)
        deep = ""
        if CFG.deep_link_bot_username:
            deep = f"\nСсылка: https://t.me/{CFG.deep_link_bot_username}?start={invite['code']}"
        expires = format_dt(invite["expires_at"]) if invite["expires_at"] else "без срока"
        await message.answer(
            f"✅ Инвайт создан\n"
            f"Код: <code>{invite['code']}</code>\n"
            f"Использований: {invite['uses_left']}\n"
            f"Истекает: {expires}\n"
            f"Примечание: {html.escape(invite.get('note') or '—')}{deep}",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    @router.message(Command("invites"))
    async def cmd_invites(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        invites = db.list_invites(15)
        if not invites:
            await message.answer("Инвайтов пока нет.")
            return
        lines = ["Последние инвайты:"]
        for inv in invites:
            expires = format_dt(inv["expires_at"]) if inv["expires_at"] else "без срока"
            lines.append(
                f"<code>{inv['code']}</code> | uses={inv['uses_left']} | exp={expires} | {html.escape(inv.get('note') or '')}"
            )
        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)

    @router.message(Command("grant"))
    async def cmd_grant(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        parts = compact_spaces(message.text or "").split()
        if len(parts) < 2 or not parts[1].isdigit():
            await message.answer("Использование: /grant USER_ID [DAYS]")
            return
        target = int(parts[1])
        days = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else None
        db.ensure_user(target, "", "", auto_grant=False)
        expires_at = utc_ts() + days * 86400 if days and days > 0 else None
        db.set_user_access(target, True, access_expires_at=expires_at)
        suffix = f" до {format_dt(expires_at)}" if expires_at is not None else " без срока"
        await message.answer(f"✅ Доступ выдан пользователю <code>{target}</code>{suffix}", parse_mode=ParseMode.HTML)

    @router.message(Command("extend"))
    async def cmd_extend(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        parts = compact_spaces(message.text or "").split()
        if len(parts) < 3 or not parts[1].isdigit() or not parts[2].isdigit():
            await message.answer("Использование: /extend USER_ID DAYS")
            return
        target = int(parts[1])
        days = int(parts[2])
        db.ensure_user(target, "", "", auto_grant=False)
        user = db.extend_user_access_days(target, days)
        await message.answer(
            f"✅ Доступ пользователя <code>{target}</code> продлён на {days} дн.\n"
            f"Теперь до: {html.escape(format_access_expiry((user or {}).get('access_expires_at')))}",
            parse_mode=ParseMode.HTML,
        )

    @router.message(Command("revoke"))
    async def cmd_revoke(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        parts = compact_spaces(message.text or "").split()
        if len(parts) < 2 or not parts[1].isdigit():
            await message.answer("Использование: /revoke USER_ID")
            return
        target = int(parts[1])
        db.set_user_access(target, False, access_expires_at=None)
        await message.answer(f"⛔ Доступ отозван у пользователя <code>{target}</code>", parse_mode=ParseMode.HTML)

    @router.message(Command("users"))
    async def cmd_users(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        parts = compact_spaces(message.text or "").split()
        page = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 1
        page = max(1, page)
        offset = (page - 1) * admin_users_page_size
        users = db.list_users_with_stats(limit=admin_users_page_size, offset=offset)
        total = db.count_users()
        if not users:
            await message.answer("Пользователей пока нет.")
            return
        pages = max(1, (total + admin_users_page_size - 1) // admin_users_page_size)
        lines = [f"👥 Пользователи — страница {page}/{pages}", ""]
        lines.extend(format_admin_user_line(user) for user in users)
        await message.answer("\n\n".join(lines), parse_mode=ParseMode.HTML)

    @router.message(Command("user"))
    async def cmd_user(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        parts = compact_spaces(message.text or "").split()
        if len(parts) < 2 or not parts[1].isdigit():
            await message.answer("Использование: /user USER_ID")
            return
        target = int(parts[1])
        user = db.get_user_with_subscriptions(target)
        if not user:
            await message.answer("Пользователь не найден.")
            return
        await message.answer(format_admin_user_details(db, user), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)

    @router.message(Command("broadcast"))
    async def cmd_broadcast(message: Message) -> None:
        if not is_admin(message.from_user.id):
            return
        payload = parse_command_payload(message.text)
        if not payload:
            await message.answer(
                "Использование: /broadcast ТЕКСТ\n\n"
                "Пример:\n"
                "/broadcast ⚠️ Внимание! API временно недоступно. Мы уже чиним и сообщим, когда всё восстановится."
            )
            return
        user_ids = db.list_broadcast_user_ids(active_only=True, include_admins=False)
        if not user_ids:
            await message.answer("Некому отправлять: активных пользователей с доступом не найдено.")
            return

        sent = 0
        failed = 0
        failed_ids: List[int] = []
        for user_id in user_ids:
            try:
                await message.bot.send_message(
                    user_id,
                    payload,
                    disable_web_page_preview=CFG.disable_preview,
                )
                sent += 1
            except Exception:
                failed += 1
                failed_ids.append(int(user_id))
                log.warning("broadcast send failed user=%s", user_id, exc_info=True)
            await asyncio.sleep(0.04)

        lines = [
            "📢 Рассылка завершена",
            f"Отправлено: {sent}",
            f"Ошибок: {failed}",
            f"Получателей всего: {len(user_ids)}",
        ]
        if failed_ids:
            preview = ", ".join(str(x) for x in failed_ids[:20])
            more = " …" if len(failed_ids) > 20 else ""
            lines.append(f"Не доставлено user_id: <code>{html.escape(preview + more)}</code>")
        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
