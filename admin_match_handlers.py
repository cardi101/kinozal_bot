import html
import logging
from datetime import datetime, timezone
from typing import Any, List

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, Message

from admin_match_review_helpers import (
    build_match_review_alert,
    deliver_item_to_matching_subscriptions,
    notify_admins_about_match_review,
)
from admin_helpers import is_admin, extract_kinozal_id_from_text, parse_admin_route_target
from config import CFG
from country_helpers import COUNTRY_NAMES_RU, parse_country_codes
from delivery_audit import build_delivery_audit
from delivery_sender import send_item_to_user
from keyboards import match_candidates_kb, match_review_kb
from match_debug_helpers import build_match_explanation, rematch_item_live, _strip_existing_match_fields
from release_versioning import describe_variant_change, extract_kinozal_id, format_variant_summary
from services.admin_api_service import AdminApiService
from subscription_matching import match_subscription
from text_access import format_dt, human_media_type
from utils import compact_spaces, short


log = logging.getLogger(__name__)


def register_admin_match_handlers(router: Router, db: Any, tmdb: Any) -> None:
    def _skip_current_admin_chat(message: Message) -> set[int]:
        chat_type = str(getattr(message.chat, "type", ""))
        chat_id = int(getattr(message.chat, "id", 0) or 0)
        if chat_type == "private" and chat_id in {int(admin_id) for admin_id in CFG.admin_ids}:
            return {chat_id}
        return set()

    def _count_matching_users(item: dict) -> int:
        matched_users = 0
        item_tmdb_id = item.get("tmdb_id")
        for sub in db.list_enabled_subscriptions():
            sub_full = db.get_subscription(int(sub["id"]))
            if not sub_full:
                continue
            tg_user_id = int(sub_full["tg_user_id"])
            if item_tmdb_id and db.is_title_muted(tg_user_id, int(item_tmdb_id)):
                continue
            if match_subscription(db, sub_full, item):
                matched_users += 1
        return matched_users

    async def _send_match_review_card(message: Message, item: dict) -> None:
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
        text = build_match_review_alert(item, affected_users=_count_matching_users(item))
        reply_markup = match_review_kb(kinozal_id, has_tmdb_match=bool(item.get("tmdb_id"))) if kinozal_id else None
        await message.answer(
            text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=reply_markup,
        )

    async def _send_match_explanation(message: Message, kinozal_id: str) -> None:
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

        await message.answer(
            build_match_explanation(db, item, live_item),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )

    async def _send_match_candidates(message: Message, kinozal_id: str) -> None:
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
        await message.answer(
            "\n".join(lines),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=match_candidates_kb(kinozal_id, candidates),
        )

    async def _send_delivery_audit(message: Message, kinozal_id: str, tg_user_id: int | None = None) -> None:
        audits = db.get_delivery_audits(kinozal_id, tg_user_id=tg_user_id, limit=10)
        if not audits:
            await message.answer(f"История delivery audit для Kinozal ID {kinozal_id} не найдена.")
            return

        lines = [f"📨 Delivery audit for Kinozal ID <code>{html.escape(kinozal_id)}</code>", ""]
        for row in audits:
            audit = row.get("delivery_audit") or {}
            sub_name = compact_spaces(str(row.get("subscription_name") or "—")) or "—"
            matched_subs = audit.get("matched_subscriptions") or []
            matched_summary = "; ".join(
                f"{compact_spaces(str(sub.get('name') or '—'))}: {compact_spaces(str(sub.get('reason') or '—'))}"
                for sub in matched_subs[:3]
            ) or "—"
            context = compact_spaces(str(audit.get("context") or ""))
            bucket = compact_spaces(str(audit.get("bucket") or ""))
            confidence = compact_spaces(str(audit.get("tmdb_match_confidence") or ""))
            match_path = compact_spaces(str(audit.get("tmdb_match_path") or ""))
            if not audit:
                context = "historical_audit_missing"
                bucket = "—"
                confidence = "—"
                match_path = "—"
            lines.extend(
                [
                    (
                        f"• user=<code>{int(row.get('tg_user_id') or 0)}</code> | "
                        f"sub={html.escape(sub_name)} | "
                        f"at=<code>{html.escape(format_dt(row.get('delivered_at')))}</code> | "
                        f"src=<code>{html.escape(compact_spaces(str(row.get('delivery_source') or 'live')))}</code>"
                    ),
                    (
                        f"  context=<code>{html.escape(context or '—')}</code> | "
                        f"bucket=<code>{html.escape(bucket or '—')}</code> | "
                        f"confidence=<code>{html.escape(confidence or '—')}</code> | "
                        f"path=<code>{html.escape(match_path or '—')}</code>"
                    ),
                    f"  matched: {html.escape(matched_summary)}",
                    "",
                ]
            )

        await message.answer(
            "\n".join(lines).rstrip(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _send_delivery_explanation(message: Message, kinozal_id: str, tg_user_id: int) -> None:
        user = db.get_user(int(tg_user_id))
        if not user:
            await message.answer(f"Не нашёл пользователя {tg_user_id}.")
            return

        item = db.find_item_by_kinozal_id(kinozal_id)
        if not item:
            await message.answer(f"Не нашёл релиз в базе по Kinozal ID {kinozal_id}.")
            return

        matched_subscriptions: List[dict] = []
        mismatched_subscriptions: List[dict] = []
        for sub in db.list_user_subscriptions(int(tg_user_id)):
            sub_full = db.get_subscription(int(sub["id"]))
            if not sub_full:
                continue
            if not int(sub_full.get("is_enabled") or 0):
                mismatched_subscriptions.append(
                    {
                        "id": int(sub_full["id"]),
                        "name": str(sub_full.get("name") or ""),
                        "reason": "disabled",
                    }
                )
                continue
            reason = db.explain_subscription_match(sub_full, item) if hasattr(db, "explain_subscription_match") else None
            if reason is None:
                from subscription_matching import explain_subscription_match

                reason = explain_subscription_match(db, sub_full, item)
            payload = {
                "id": int(sub_full["id"]),
                "name": str(sub_full.get("name") or ""),
                "reason": str(reason or ""),
            }
            if payload["reason"] == "passed":
                matched_subscriptions.append(payload)
            else:
                mismatched_subscriptions.append(payload)

        item_tmdb_id = int(item["tmdb_id"]) if item.get("tmdb_id") not in (None, "") else None
        muted_title = bool(item_tmdb_id and db.is_title_muted(int(tg_user_id), item_tmdb_id))
        delivered_exact = db.delivered(int(tg_user_id), int(item["id"]))
        delivered_equivalent = db.delivered_equivalent(int(tg_user_id), item)
        cooldown_active = db.recently_delivered_kinozal_id(int(tg_user_id), kinozal_id, cooldown_seconds=420)
        quiet_start, quiet_end = db.get_user_quiet_hours(int(tg_user_id))
        current_hour = datetime.now(timezone.utc).hour
        quiet_active = False
        if quiet_start is not None and quiet_end is not None:
            if quiet_start < quiet_end:
                quiet_active = quiet_start <= current_hour < quiet_end
            else:
                quiet_active = current_hour >= quiet_start or current_hour < quiet_end

        with db.lock:
            pending_delivery = db.conn.execute(
                """
                SELECT item_id, matched_sub_ids, old_release_text, is_release_text_change, queued_at
                FROM pending_deliveries
                WHERE tg_user_id = ? AND item_id = ?
                LIMIT 1
                """,
                (int(tg_user_id), int(item["id"])),
            ).fetchone()
            debounce_entry = db.conn.execute(
                """
                SELECT tg_user_id, kinozal_id, item_id, matched_sub_ids, deliver_after_ts, reset_count
                FROM debounce_queue
                WHERE tg_user_id = ? AND kinozal_id = ?
                LIMIT 1
                """,
                (int(tg_user_id), kinozal_id),
            ).fetchone()

        pending_match_review = db.get_pending_match_review_by_item_id(int(item["id"]))
        anomalies = [
            anomaly
            for anomaly in db.list_release_anomalies(kinozal_id, limit=10)
            if int(anomaly.get("item_id") or 0) == int(item["id"]) and str(anomaly.get("status") or "") == "open"
        ]

        status = "ready"
        blockers: List[str] = []
        if muted_title:
            status = "skipped"
            blockers.append("muted_title")
        if not matched_subscriptions:
            status = "skipped"
            blockers.append("no_matching_enabled_subscriptions")
        if delivered_exact:
            status = "delivered"
            blockers.append("delivered_exact")
        elif delivered_equivalent:
            status = "skipped"
            blockers.append("delivered_equivalent")
        elif cooldown_active:
            status = "waiting"
            blockers.append("cooldown")
        if pending_delivery:
            status = "waiting"
            blockers.append("pending_delivery")
        if debounce_entry:
            status = "waiting"
            blockers.append("debounce")
        if pending_match_review:
            status = "waiting"
            blockers.append("match_review")
        if anomalies:
            status = "held"
            blockers.append("anomaly_hold")

        lines = [
            f"🧭 Delivery explain for Kinozal ID <code>{html.escape(kinozal_id)}</code>",
            f"user=<code>{int(tg_user_id)}</code> | item=<code>{int(item['id'])}</code> | status=<code>{html.escape(status)}</code>",
            f"blockers: <code>{html.escape(', '.join(blockers) if blockers else 'none')}</code>",
            f"title: {html.escape(compact_spaces(str(item.get('source_title') or '—')))}",
            f"variant: <code>{html.escape(format_variant_summary(item))}</code>",
            "",
            f"muted_title=<code>{str(muted_title).lower()}</code> | delivered_exact=<code>{str(delivered_exact).lower()}</code> | delivered_equivalent=<code>{str(delivered_equivalent).lower()}</code>",
            f"cooldown_active=<code>{str(bool(cooldown_active)).lower()}</code> | quiet_active=<code>{str(bool(quiet_active)).lower()}</code>",
            "",
            "✅ Matching subscriptions:",
        ]
        if matched_subscriptions:
            for sub in matched_subscriptions:
                lines.append(
                    f"• <code>{sub['id']}</code> {html.escape(sub['name'])} | <code>{html.escape(sub['reason'])}</code>"
                )
        else:
            lines.append("• none")

        lines.append("")
        lines.append("⛔ Mismatched subscriptions:")
        if mismatched_subscriptions:
            for sub in mismatched_subscriptions[:10]:
                lines.append(
                    f"• <code>{sub['id']}</code> {html.escape(sub['name'])} | <code>{html.escape(sub['reason'])}</code>"
                )
        else:
            lines.append("• none")

        if pending_delivery:
            lines.extend(
                [
                    "",
                    (
                        "pending_delivery: "
                        f"queued_at=<code>{html.escape(format_dt(pending_delivery.get('queued_at')))}</code> "
                        f"matched_sub_ids=<code>{html.escape(str(pending_delivery.get('matched_sub_ids') or ''))}</code>"
                    ),
                ]
            )
        if debounce_entry:
            lines.extend(
                [
                    "",
                    (
                        "debounce: "
                        f"deliver_after=<code>{html.escape(format_dt(debounce_entry.get('deliver_after_ts')))}</code> "
                        f"reset_count=<code>{html.escape(str(debounce_entry.get('reset_count') or 0))}</code>"
                    ),
                ]
            )
        if pending_match_review:
            lines.extend(
                [
                    "",
                    (
                        "match_review: "
                        f"reason=<code>{html.escape(compact_spaces(str(pending_match_review.get('reason') or 'pending')))}</code>"
                    ),
                ]
            )
        if anomalies:
            lines.append("")
            lines.append("🚨 Open anomalies:")
            for anomaly in anomalies:
                lines.append(
                    "• "
                    f"{html.escape(str(anomaly.get('anomaly_type') or 'unknown'))} | "
                    f"<code>{html.escape(str(anomaly.get('old_value') or ''))}</code> -> "
                    f"<code>{html.escape(str(anomaly.get('new_value') or ''))}</code>"
                )

        await message.answer(
            "\n".join(lines).rstrip(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )

    async def _send_version_timeline(message: Message, kinozal_id: str) -> None:
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

        await message.answer(
            "\n".join(lines).strip(),
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=CFG.disable_preview,
        )

    async def _replay_anomaly(message: Message, kinozal_id: str) -> None:
        open_anomaly = next(
            (
                anomaly
                for anomaly in db.list_release_anomalies(kinozal_id, limit=10)
                if str(anomaly.get("status") or "") == "open" and int(anomaly.get("item_id") or 0) > 0
            ),
            None,
        )
        anomaly_item_id = int(open_anomaly["item_id"]) if open_anomaly else None
        service = AdminApiService(db=db, tmdb_service=tmdb, kinozal_service=None, bot=message.bot)
        result = await service.replay_delivery_to_matching_users(
            kinozal_id,
            item_id=anomaly_item_id,
            force=True,
            resolve_anomalies=True,
        )
        lines = [
            f"📨 Anomaly replay for Kinozal ID {kinozal_id}",
            f"Item ID: {int(result['item_id'])}",
            f"Подходящих подписок: {int(result['matched_subscriptions'])}",
            f"Подходящих пользователей: {int(result['matched_users'])}",
            f"Новых уведомлений отправлено: {int(result['delivered_count'])}",
            f"Закрыто anomaly: {int(result['resolved_anomalies'])}",
        ]
        if result.get("reason_counts"):
            reasons = ", ".join(
                f"{reason}={count}"
                for reason, count in sorted(result["reason_counts"].items())
            )
            lines.append(f"Итоги: {reasons}")
        await message.answer("\n".join(lines), disable_web_page_preview=True)

    async def _override_match(
        message: Message,
        admin_user_id: int,
        kinozal_id: str,
        tmdb_id_raw: str,
        media_type: str,
    ) -> None:
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
        updated_item["tmdb_match_evidence"] = f"admin override by {admin_user_id}"
        item_id, _, _ = db.save_item(updated_item)
        refreshed = db.get_item(item_id) or updated_item
        db.set_match_override(kinozal_id, int(tmdb_id_raw), media_type, source="admin_override")
        review = db.get_pending_match_review(kinozal_id)
        if review:
            db.resolve_match_review(int(review["item_id"]), "overridden", admin_user_id, note=f"override to {tmdb_id_raw}/{media_type}")
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

    async def _approve_match(message: Message, admin_user_id: int, kinozal_id: str) -> None:
        review = db.get_pending_match_review(kinozal_id)
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not review or not item:
            await message.answer(f"Pending review для Kinozal ID {kinozal_id} не найден.")
            return
        if not item.get("tmdb_id"):
            await message.answer(f"У релиза {kinozal_id} сейчас нет TMDB-матча. Используй /overridematch.")
            return

        db.set_match_override(kinozal_id, int(item["tmdb_id"]), str(item.get("media_type") or "movie"), source="admin_approve")
        db.resolve_match_review(int(review["item_id"]), "approved", admin_user_id, note="approved current match")
        matched_users, delivered_count = await deliver_item_to_matching_subscriptions(db, message.bot, item, force=True)
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

    async def _reject_match(message: Message, admin_user_id: int, kinozal_id: str) -> None:
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
        db.resolve_match_review(int(review["item_id"]), "rejected", admin_user_id, note="rejected current match")
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

    async def _no_match(message: Message, admin_user_id: int, kinozal_id: str) -> None:
        review = db.get_pending_match_review(kinozal_id)
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not review or not item:
            await message.answer(f"Pending review для Kinozal ID {kinozal_id} не найден.")
            return

        rejected_tmdb_id = item.get("tmdb_id")
        if rejected_tmdb_id:
            db.add_match_rejection(kinozal_id, int(rejected_tmdb_id), note="admin marked no_match")
        db.clear_item_match(int(item["id"]))
        db.resolve_match_review(int(review["item_id"]), "no_match", admin_user_id, note="admin marked no good match")
        await message.answer(
            "\n".join(
                [
                    f"🚫 No Match for Kinozal ID {kinozal_id}",
                    "Текущий TMDB очищен, доставка пользователям остановлена.",
                    "Кейс сохранён как no_match и не будет повторно всплывать для этого item.",
                ]
            ),
            disable_web_page_preview=True,
        )

    async def _force_deliver(message: Message, admin_user_id: int, kinozal_id: str) -> None:
        review = db.get_pending_match_review(kinozal_id)
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not review or not item:
            await message.answer(f"Pending review для Kinozal ID {kinozal_id} не найден.")
            return

        matched_users, delivered_count = await deliver_item_to_matching_subscriptions(db, message.bot, item)
        db.resolve_match_review(int(review["item_id"]), "forced", admin_user_id, note="forced delivery without verified match")
        await message.answer(
            "\n".join(
                [
                    f"📨 Force delivery for Kinozal ID {kinozal_id}",
                    "Релиз отправлен по текущим полям item без подтверждённого TMDB-матча.",
                    f"Подходящих подписок: {matched_users}",
                    f"Новых уведомлений отправлено: {delivered_count}",
                ]
            ),
            disable_web_page_preview=True,
        )

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
        for review in reviews:
            item = db.get_item(int(review["item_id"]))
            if item:
                await _send_match_review_card(message, item)

    @router.message(Command("reviewmatch"))
    async def cmd_reviewmatch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id and message.reply_to_message:
            replied_text = message.reply_to_message.html_text or message.reply_to_message.text or message.reply_to_message.caption or ""
            kinozal_id = extract_kinozal_id_from_text(replied_text)
        if not kinozal_id:
            await message.answer("Используй: /reviewmatch <kinozal_id>")
            return

        review = db.get_pending_match_review(kinozal_id)
        item = db.find_item_by_kinozal_id(kinozal_id)
        if not review or not item:
            await message.answer(f"Pending review для Kinozal ID {kinozal_id} не найден.")
            return

        await _send_match_review_card(message, item)
        sent_count = await notify_admins_about_match_review(
            message.bot,
            item,
            affected_users=_count_matching_users(item),
            skip_admin_ids=_skip_current_admin_chat(message),
        )
        if sent_count > 0:
            db.mark_match_review_notified(int(review["item_id"]))
        await message.answer(
            f"🧪 Review resend for {kinozal_id}: в этот чат отправлено 1 карточку, админам отправлено {sent_count}.",
            disable_web_page_preview=True,
        )

    @router.message(Command("reviewpending"))
    async def cmd_reviewpending(message: Message, command: CommandObject) -> None:
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

        resent_admin = 0
        shown_here = 0
        for review in reviews:
            item = db.get_item(int(review["item_id"]))
            if not item:
                continue
            shown_here += 1
            await _send_match_review_card(message, item)
            item_sent_count = await notify_admins_about_match_review(
                message.bot,
                item,
                affected_users=_count_matching_users(item),
                skip_admin_ids=_skip_current_admin_chat(message),
            )
            resent_admin += item_sent_count
            if item_sent_count > 0:
                db.mark_match_review_notified(int(review["item_id"]))

        await message.answer(
            f"🧪 Review resend done: карточек в этот чат {shown_here}, отправок админам {resent_admin}.",
            disable_web_page_preview=True,
        )

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

        await _send_match_candidates(message, kinozal_id)

    @router.message(Command("deliveryaudit"))
    async def cmd_deliveryaudit(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        parts = compact_spaces(str(command.args or "")).split()
        if not parts:
            await message.answer("Используй: /deliveryaudit <kinozal_id> [tg_user_id]")
            return

        kinozal_id = extract_kinozal_id(parts[0] or "")
        tg_user_id = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else None
        if not kinozal_id:
            await message.answer("Используй: /deliveryaudit <kinozal_id> [tg_user_id]")
            return

        await _send_delivery_audit(message, kinozal_id, tg_user_id=tg_user_id)

    @router.message(Command("explaindelivery"))
    async def cmd_explaindelivery(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        parts = compact_spaces(str(command.args or "")).split()
        if len(parts) < 2:
            await message.answer("Используй: /explaindelivery <kinozal_id> <tg_user_id>")
            return

        kinozal_id = extract_kinozal_id(parts[0] or "")
        tg_user_id = int(parts[1]) if parts[1].isdigit() else None
        if not kinozal_id or tg_user_id is None:
            await message.answer("Используй: /explaindelivery <kinozal_id> <tg_user_id>")
            return

        await _send_delivery_explanation(message, kinozal_id, tg_user_id)

    @router.callback_query(F.data.startswith("matchreview:"))
    async def cb_matchreview(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Только для администратора.", show_alert=True)
            return
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3:
            await callback.answer("Некорректное действие", show_alert=True)
            return
        _, action, kinozal_id = parts
        kinozal_id = extract_kinozal_id(kinozal_id or "")
        if not kinozal_id:
            await callback.answer("Некорректный Kinozal ID", show_alert=True)
            return

        if action == "approve":
            await _approve_match(callback.message, callback.from_user.id, kinozal_id)
            await callback.answer("Match approved")
            return
        if action == "reject":
            await _reject_match(callback.message, callback.from_user.id, kinozal_id)
            await callback.answer("Match rejected")
            return
        if action == "no_match":
            await _no_match(callback.message, callback.from_user.id, kinozal_id)
            await callback.answer("Marked no match")
            return
        if action in {"force", "deliver"}:
            await _force_deliver(callback.message, callback.from_user.id, kinozal_id)
            await callback.answer("Уведомление отправлено")
            return
        if action == "candidates":
            await _send_match_candidates(callback.message, kinozal_id)
            await callback.answer()
            return
        if action == "explain":
            await _send_match_explanation(callback.message, kinozal_id)
            await callback.answer()
            return
        await callback.answer("Неизвестное действие", show_alert=True)

    @router.callback_query(F.data.startswith("anomaly:"))
    async def cb_anomaly(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Только для администратора.", show_alert=True)
            return
        parts = (callback.data or "").split(":", 2)
        if len(parts) != 3:
            await callback.answer("Некорректное действие", show_alert=True)
            return
        _, action, kinozal_id = parts
        kinozal_id = extract_kinozal_id(kinozal_id or "")
        if not kinozal_id:
            await callback.answer("Некорректный Kinozal ID", show_alert=True)
            return

        if action == "timeline":
            await _send_version_timeline(callback.message, kinozal_id)
            await callback.answer()
            return
        if action == "replay":
            await _replay_anomaly(callback.message, kinozal_id)
            await callback.answer("Replay выполнен")
            return
        await callback.answer("Неизвестное действие", show_alert=True)

    @router.callback_query(F.data.startswith("matchpick:"))
    async def cb_matchpick(callback: CallbackQuery) -> None:
        if not is_admin(callback.from_user.id):
            await callback.answer("Только для администратора.", show_alert=True)
            return
        parts = (callback.data or "").split(":", 3)
        if len(parts) != 4:
            await callback.answer("Некорректное действие", show_alert=True)
            return
        _, kinozal_id, tmdb_id_raw, media_type = parts
        kinozal_id = extract_kinozal_id(kinozal_id or "")
        media_type = compact_spaces(media_type).lower()
        if not kinozal_id or not tmdb_id_raw.isdigit() or media_type not in {"movie", "tv"}:
            await callback.answer("Некорректный кандидат", show_alert=True)
            return

        await _override_match(callback.message, callback.from_user.id, kinozal_id, tmdb_id_raw, media_type)
        await callback.answer("Override applied")

    @router.message(Command("approvematch"))
    async def cmd_approvematch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id:
            await message.answer("Используй: /approvematch <kinozal_id>")
            return

        await _approve_match(message, message.from_user.id, kinozal_id)

    @router.message(Command("rejectmatch"))
    async def cmd_rejectmatch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id:
            await message.answer("Используй: /rejectmatch <kinozal_id>")
            return

        await _reject_match(message, message.from_user.id, kinozal_id)

    @router.message(Command("nomatch"))
    async def cmd_nomatch(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id:
            await message.answer("Используй: /nomatch <kinozal_id>")
            return

        await _no_match(message, message.from_user.id, kinozal_id)

    @router.message(Command("forcedeliver"))
    async def cmd_forcedeliver(message: Message, command: CommandObject) -> None:
        if not is_admin(message.from_user.id):
            await message.answer("Только для администратора.")
            return

        kinozal_id = extract_kinozal_id(command.args or "")
        if not kinozal_id:
            await message.answer("Используй: /forcedeliver <kinozal_id>")
            return

        await _force_deliver(message, message.from_user.id, kinozal_id)

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

        await _override_match(message, message.from_user.id, kinozal_id, tmdb_id_raw, media_type)

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
        matched_subscription_count = 0
        matched_user_ids: set[int] = set()
        skipped_existing_user_ids: set[int] = set()
        matched_by_user: dict[int, list[dict[str, Any]]] = {}
        for sub in db.list_enabled_subscriptions():
            sub_full = db.get_subscription(int(sub["id"]))
            if not sub_full:
                continue
            if not match_subscription(db, sub_full, item):
                continue
            matched_subscription_count += 1
            tg_user_id = int(sub_full["tg_user_id"])
            matched_user_ids.add(tg_user_id)
            matched_by_user.setdefault(tg_user_id, []).append(sub_full)

        for tg_user_id, matched_subs in matched_by_user.items():
            if db.delivered(tg_user_id, int(item["id"])) or db.delivered_equivalent(tg_user_id, item):
                skipped_existing_user_ids.add(tg_user_id)
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
                delivery_audit = build_delivery_audit(db, item, matched_subs, context="admin_route")
                if not db.begin_delivery_claim(
                    tg_user_id,
                    int(item["id"]),
                    int(matched_subs[0]["id"]),
                    [int(sub["id"]) for sub in matched_subs],
                    delivery_audit=delivery_audit,
                    context="admin_route",
                ):
                    skipped_existing_user_ids.add(tg_user_id)
                    continue
                await send_item_to_user(db, message.bot, tg_user_id, item, matched_subs)
                db.record_delivery(
                    tg_user_id,
                    int(item["id"]),
                    int(matched_subs[0]["id"]),
                    [int(sub["id"]) for sub in matched_subs],
                    delivery_audit=delivery_audit,
                )
                delivered_count += 1
            except Exception:
                db.mark_delivery_claim_failed(tg_user_id, int(item["id"]), error="admin_route_send_failed")
                log.exception("Admin route delivery failed item=%s user=%s", item.get("id"), tg_user_id)

        summary_lines = [
            f"✅ Релиз перенаправлен как: {label}.",
            f"Kinozal ID: {kinozal_id}",
            f"Подходящих подписок: {matched_subscription_count}",
            f"Подходящих пользователей: {len(matched_user_ids)}",
            f"Уже получали релиз: {len(skipped_existing_user_ids)}",
            f"Новых уведомлений отправлено: {delivered_count}",
        ]
        if delivered_count == 0 and skipped_existing_user_ids:
            summary_lines.append("Причина: все подходящие пользователи уже получали этот релиз или его эквивалент.")
        elif delivered_count == 0 and not matched_user_ids:
            summary_lines.append("Причина: после ручного маршрута не нашлось ни одной подходящей подписки.")

        await message.answer(
            "\n".join(summary_lines)
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

        await _send_match_explanation(message, kinozal_id)

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

        await _send_version_timeline(message, kinozal_id)
