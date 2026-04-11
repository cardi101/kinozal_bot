import html
import logging
from typing import Any, List

from aiogram import Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import Message

from admin_match_review_helpers import deliver_item_to_matching_subscriptions
from admin_helpers import is_admin, extract_kinozal_id_from_text, parse_admin_route_target
from config import CFG
from country_helpers import COUNTRY_NAMES_RU, parse_country_codes
from delivery_sender import send_item_to_user
from match_debug_helpers import build_match_explanation, rematch_item_live, _strip_existing_match_fields
from release_versioning import describe_variant_change, extract_kinozal_id, format_variant_summary
from subscription_matching import match_subscription
from text_access import format_dt, human_media_type
from utils import compact_spaces, short


log = logging.getLogger(__name__)


def register_admin_match_handlers(router: Router, db: Any, tmdb: Any) -> None:
    @router.message(Command("matchqueue"))
    async def cmd_matchqueue(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        raw = compact_spaces(str(command.args or ""))
        limit = 10
        if raw.isdigit():
            limit = max(1, min(int(raw), 30))

        reviews = db.list_pending_match_reviews(limit=limit)
        if not reviews:
            await message.answer("Очередь match review пуста.")
            return

        lines = [f"🧪 Pending match review: {len(reviews)}"]
        for review in reviews:
            item = db.get_item(int(review["item_id"]))
            if not item:
                continue
            confidence = compact_spaces(str(item.get("tmdb_match_confidence") or "unmatched")) or "unmatched"
            lines.append(
                f"• <code>{html.escape(str(review.get('kinozal_id') or '—'))}</code> "
                f"| <code>{html.escape(confidence)}</code> "
                f"| {html.escape(short(compact_spaces(str(item.get('source_title') or '—')), 80))}"
            )
        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    @router.message(Command("matchcandidates"))
    async def cmd_matchcandidates(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id and message.reply_to_message:
            replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
            kinozal_id = extract_kinozal_id_from_text(replied_text)
        if not kinozal_id:
            await message.answer("Используй: /matchcandidates <kinozal_id>")
            return

        item = db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
            return

        candidates = await tmdb.search_candidates_for_item(_strip_existing_match_fields(item), limit=8)
        if not candidates:
            await message.answer(
                "\n".join(
                    [
                        f"Кандидаты TMDB для {kinozal_id} не найдены.",
                        f"Следующий шаг: /overridematch {kinozal_id} <tmdb_id> <movie|tv>",
                    ]
                ),
                disable_web_page_preview=True,
            )
            return

        lines = [
            f"🔎 TMDB candidates for Kinozal ID {kinozal_id}",
            f"Заголовок: {html.escape(compact_spaces(str(item.get('source_title') or '—')))}",
            "",
        ]
        for row in candidates:
            release_year = short(compact_spaces(str(row.get("release_date") or "")), 10) or "—"
            original = compact_spaces(str(row.get("original_title") or ""))
            line = (
                f"• <code>{int(row['tmdb_id'])}</code> [{html.escape(str(row.get('media_type') or 'movie'))}] "
                f"{html.escape(compact_spaces(str(row.get('title') or '—')))} | year={html.escape(release_year)} | "
                f"confidence=<code>{html.escape(compact_spaces(str(row.get('confidence') or '—')))}</code>"
            )
            lines.append(line)
            if original and original != row.get("title"):
                lines.append(f"  original: {html.escape(original)}")
            lines.append(f"  query: <code>{html.escape(compact_spaces(str(row.get('query') or '—')))}</code>")
            lines.append(f"  evidence: {html.escape(compact_spaces(str(row.get('evidence') or '—')))}")

        lines.append("")
        lines.append(f"Override: <code>/overridematch {html.escape(str(kinozal_id))} &lt;tmdb_id&gt; &lt;movie|tv&gt;</code>")
        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    @router.message(Command("approvematch"))
    async def cmd_approvematch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id:
            await message.answer("Используй: /approvematch <kinozal_id>")
            return

        review = db.get_pending_match_review(kinozal_id)
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not review or not item:
            await message.answer(f"Pending review для Kinozal ID {kinozal_id} не найден.")
            return
        if not item.get("tmdb_id"):
            await message.answer(f"У релиза {kinozal_id} сейчас нет TMDB-матча. Используй /overridematch.")
            return

        db.set_match_override(kinozal_id, int(item["tmdb_id"]), str(item.get("media_type") or "movie"), source="admin_approve")
        db.resolve_match_review(int(review["item_id"]), "approved", message.from_user.id, note="approved current match")
        matched_users, delivered_count = await deliver_item_to_matching_subscriptions(db, message.bot, item)
        await message.answer(
            "\n".join(
                [
                    f"✅ Match approved for Kinozal ID {kinozal_id}",
                    f"TMDB: {compact_spaces(str(item.get('tmdb_title') or item.get('tmdb_original_title') or '—'))} (id={int(item['tmdb_id'])})",
                    f"Подходящих подписок: {matched_users}",
                    f"Новых уведомлений отправлено: {delivered_count}",
                ]
            ),
            disable_web_page_preview=True,
        )

    @router.message(Command("rejectmatch"))
    async def cmd_rejectmatch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id:
            await message.answer("Используй: /rejectmatch <kinozal_id>")
            return

        review = db.get_pending_match_review(kinozal_id)
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not review or not item:
            await message.answer(f"Pending review для Kinozal ID {kinozal_id} не найден.")
            return

        rejected_tmdb_id = item.get("tmdb_id")
        rejected_title = compact_spaces(str(item.get("tmdb_title") or item.get("tmdb_original_title") or "")) or "—"
        if rejected_tmdb_id:
            db.add_match_rejection(kinozal_id, int(rejected_tmdb_id), note="admin rejected current match")
        db.clear_item_match(int(item["id"]))
        db.resolve_match_review(int(review["item_id"]), "rejected", message.from_user.id, note="rejected current match")
        await message.answer(
            "\n".join(
                [
                    f"⛔ Match rejected for Kinozal ID {kinozal_id}",
                    f"TMDB было: {rejected_title}" + (f" (id={int(rejected_tmdb_id)})" if rejected_tmdb_id else ""),
                    "Текущий матч очищен, доставка пользователям не будет выполнена.",
                    f"Следующий шаг: /overridematch {kinozal_id} <tmdb_id> <movie|tv>",
                ]
            ),
            disable_web_page_preview=True,
        )

    @router.message(Command("overridematch"))
    async def cmd_overridematch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        parts = compact_spaces(str(command.args or "")).split()
        if len(parts) < 2:
            await message.answer("Используй: /overridematch <kinozal_id> <tmdb_id> <movie|tv>")
            return
        kinozal_id = extract_kinozal_id(parts[0] or "")
        tmdb_id_raw = compact_spaces(parts[1])
        media_type = compact_spaces(parts[2] if len(parts) > 2 else "movie").lower()
        if not kinozal_id or not tmdb_id_raw.isdigit() or media_type not in {"movie", "tv"}:
            await message.answer("Используй: /overridematch <kinozal_id> <tmdb_id> <movie|tv>")
            return

        item = db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
            return

        try:
            details = await tmdb.get_details(media_type, int(tmdb_id_raw))
        except Exception:
            log.exception("Admin override match failed kinozal_id=%s tmdb_id=%s", kinozal_id, tmdb_id_raw)
            await message.answer("Не удалось получить details из TMDB. Смотри лог app.")
            return

        updated_item = dict(item)
        updated_item.update(details)
        updated_item["tmdb_match_path"] = "admin_override"
        updated_item["tmdb_match_confidence"] = "verified"
        updated_item["tmdb_match_evidence"] = f"admin override by {message.from_user.id}"
        item_id, _, _ = db.save_item(updated_item)
        refreshed = db.get_item(item_id) or updated_item
        db.set_match_override(kinozal_id, int(tmdb_id_raw), media_type, source="admin_override")
        review = db.get_pending_match_review(kinozal_id)
        if review:
            db.resolve_match_review(int(review["item_id"]), "overridden", message.from_user.id, note=f"override to {tmdb_id_raw}/{media_type}")
        matched_users, delivered_count = await deliver_item_to_matching_subscriptions(db, message.bot, refreshed)
        title = compact_spaces(str(refreshed.get("tmdb_title") or refreshed.get("tmdb_original_title") or "")) or "—"
        await message.answer(
            "\n".join(
                [
                    f"✅ Override saved for Kinozal ID {kinozal_id}",
                    f"TMDB: {title} (id={int(tmdb_id_raw)}, media={media_type})",
                    f"Подходящих подписок: {matched_users}",
                    f"Новых уведомлений отправлено: {delivered_count}",
                ]
            ),
            disable_web_page_preview=True,
        )

    @router.message(Command("route"))
    async def cmd_route(message: Message) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return
        if not message.reply_to_message:
            await message.answer("Ответь этой командой на уведомление бота. Пример: /route dorama")
            return
        parts = compact_spaces(message.text or "").split(maxsplit=1)
        target_raw = parts[1] if len(parts) > 1 else ""
        bucket, country_codes, label = parse_admin_route_target(target_raw)
        if not bucket:
            await message.answer("Используй: /route anime | dorama | turkey | world")
            return
        replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
        kinozal_id = extract_kinozal_id_from_text(replied_text)
        if not kinozal_id:
            await message.answer("Не смог найти Kinozal ID в сообщении. Ответь именно на уведомление бота.")
            return
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
            return
        db.set_item_manual_routing(int(item["id"]), bucket=bucket, country_codes=country_codes)
        item = db.get_item(int(item["id"])) or item

        delivered_count = 0
        matched_users = 0
        for sub in db.list_enabled_subscriptions():
            sub_full = db.get_subscription(int(sub["id"]))
            if not sub_full:
                continue
            if not match_subscription(db, sub_full, item):
                continue
            matched_users += 1
            tg_user_id = int(sub_full["tg_user_id"])
            if db.delivered(tg_user_id, int(item["id"])) or db.delivered_equivalent(tg_user_id, item):
                continue
            try:
                previous_item = db.get_latest_delivered_related_item(tg_user_id, item)
                if previous_item:
                    log.info(
                        "Admin route delivering updated release item=%s to user=%s source_uid=%s reason=%s prev_item_id=%s",
                        item.get("id"),
                        tg_user_id,
                        item.get("source_uid"),
                        describe_variant_change(previous_item, item),
                        previous_item.get("id"),
                    )
                else:
                    log.info(
                        "Admin route delivering new release item=%s to user=%s source_uid=%s",
                        item.get("id"),
                        tg_user_id,
                        item.get("source_uid"),
                    )
                await send_item_to_user(db, message.bot, tg_user_id, item, [sub_full])
                db.record_delivery(tg_user_id, int(item["id"]), int(sub_full["id"]), [int(sub_full["id"])])
                delivered_count += 1
            except Exception:
                log.exception("Admin route delivery failed item=%s user=%s", item.get("id"), tg_user_id)

        await message.answer(
            f"✅ Релиз перенаправлен как: {label}.\n"
            f"Kinozal ID: {kinozal_id}\n"
            f"Подходящих подписок: {matched_users}\n"
            f"Новых уведомлений отправлено: {delivered_count}"
        )

    @router.message(Command("explainmatch"))
    async def cmd_explainmatch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id and message.reply_to_message:
            replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
            kinozal_id = extract_kinozal_id_from_text(replied_text)
        if not kinozal_id:
            await message.answer("Используй: /explainmatch <kinozal_id> или ответь этой командой на уведомление бота.")
            return

        item = db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
            return

        live_item = dict(item)
        try:
            live_item = await tmdb.enrich_item(_strip_existing_match_fields(item))
        except Exception:
            log.exception("Live explain TMDB recompute failed for kinozal_id=%s", kinozal_id)
            live_item = dict(item)

        await message.answer(build_match_explanation(db, item, live_item), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)

    @router.message(Command("rematch"))
    async def cmd_rematch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id and message.reply_to_message:
            replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
            kinozal_id = extract_kinozal_id_from_text(replied_text)
        if not kinozal_id:
            await message.answer("Используй: /rematch <kinozal_id> или ответь этой командой на уведомление бота.")
            return

        item = db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
            return

        before, after, ok = await rematch_item_live(db, tmdb, item)
        if not ok or not after:
            await message.answer(f"Не удалось перематчить Kinozal ID {kinozal_id}. Смотри лог app.")
            return

        before_tmdb = before.get("tmdb_id")
        after_tmdb = after.get("tmdb_id")
        before_title = compact_spaces(str(before.get("tmdb_title") or before.get("tmdb_original_title") or "")) or "—"
        after_title = compact_spaces(str(after.get("tmdb_title") or after.get("tmdb_original_title") or "")) or "—"
        country_names = [COUNTRY_NAMES_RU.get(code, code) for code in parse_country_codes(after.get("tmdb_countries"))]
        lines = [
            f"♻️ Rematch — Kinozal ID {html.escape(str(kinozal_id))}",
            f"Заголовок: {html.escape(compact_spaces(str(after.get('source_title') or '—')))}",
            f"TMDB было: {html.escape(before_title)}" + (f" (id={int(before_tmdb)})" if before_tmdb else ""),
            f"TMDB стало: {html.escape(after_title)}" + (f" (id={int(after_tmdb)})" if after_tmdb else ""),
            f"Страны: {html.escape(', '.join(country_names or ['—']))}",
            "Старые доставки не переотправляются. Обновлена только карточка релиза в БД.",
        ]
        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    @router.message(Command("rematch_unmatched"))
    async def cmd_rematch_unmatched(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        raw = compact_spaces(str(command.args or ""))
        limit = 50
        if raw.isdigit():
            limit = max(1, min(int(raw), 500))

        items = db.list_items_for_rematch(limit=limit, only_unmatched=True)
        if not items:
            await message.answer("Для рематча ничего не найдено: unmatched items с kinozal_id закончились.")
            return

        updated = 0
        matched_now = 0
        still_unmatched = 0
        errors = 0
        samples: List[str] = []

        for item in items:
            before, after, ok = await rematch_item_live(db, tmdb, item)
            if not ok or not after:
                errors += 1
                continue
            before_tmdb = before.get("tmdb_id")
            after_tmdb = after.get("tmdb_id")
            if before_tmdb != after_tmdb:
                updated += 1
            if after_tmdb:
                matched_now += 1
                if len(samples) < 8:
                    samples.append(
                        f"• {html.escape(str(after.get('kinozal_id') or '—'))} — {html.escape(compact_spaces(str(after.get('tmdb_title') or after.get('tmdb_original_title') or after.get('source_title') or '—')))}"
                    )
            else:
                still_unmatched += 1

        lines = [
            f"♻️ Batch rematch unmatched: {len(items)}",
            f"Обновлено записей: {updated}",
            f"Теперь есть TMDB: {matched_now}",
            f"Остались без TMDB: {still_unmatched}",
            f"Ошибок: {errors}",
            "Старые доставки не переотправляются.",
        ]
        if samples:
            lines.append("")
            lines.append("Примеры новых матчей:")
            lines.extend(samples)
        await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

    @router.message(Command("why"))
    async def cmd_why(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id and message.reply_to_message:
            replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
            kinozal_id = extract_kinozal_id_from_text(replied_text)
        if not kinozal_id:
            await message.answer("Используй: /why <kinozal_id> или ответь этой командой на уведомление бота.")
            return

        report = db.get_version_timeline(kinozal_id, limit=10)
        versions = report.get("versions") or []
        if not versions:
            await message.answer(f"Не нашёл версий по Kinozal ID {kinozal_id}.")
            return

        lines = [
            f"🔎 История релиза Kinozal ID {kinozal_id}",
            f"Активных items: {report['active_count']}",
            f"В архиве: {report['archived_count']}",
            f"Показано версий: {len(versions)}",
            "",
        ]
        for idx, entry in enumerate(versions):
            icon = "🟢" if entry.get("state") == "active" else "📦"
            ts = int(entry.get("source_published_at") or entry.get("created_at") or entry.get("archived_at") or 0)
            title = short(compact_spaces(str(entry.get("source_title") or "")), 90)
            lines.append(
                f"{icon} #{entry.get('record_id')} | {format_dt(ts)} | {human_media_type(str(entry.get('media_type') or 'movie'))}\n"
                f"{html.escape(title)}\n"
                f"{html.escape(entry.get('variant_summary') or format_variant_summary(entry))}"
            )
            if idx + 1 < len(versions):
                older = versions[idx + 1]
                lines.append(f"↳ Изменение к этой версии: {html.escape(describe_variant_change(older, entry))}")
            if int(entry.get("version_duplicates") or 1) > 1:
                lines.append(f"↳ Дубликатов этой версии: {int(entry.get('version_duplicates') or 1)}")
            lines.append("")

        await message.answer("\n".join(lines).strip(), parse_mode=ParseMode.HTML, disable_web_page_preview=CFG.disable_preview)
