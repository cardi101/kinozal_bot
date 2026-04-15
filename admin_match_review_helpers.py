import html
import logging
from typing import Any, Dict, Iterable, Tuple

from aiogram.enums import ParseMode

from delivery_audit import build_delivery_audit
from keyboards import match_review_kb
from service_helpers import ops_alert_chat_ids
from subscription_matching import match_subscription
from utils import compact_spaces


log = logging.getLogger(__name__)

REVIEW_MATCH_CONFIDENCES = {"low"}


def _cfg() -> Any:
    from config import CFG

    return CFG


def item_requires_match_review(item: Dict[str, Any]) -> bool:
    if not _cfg().match_review_enabled:
        return False
    confidence = compact_spaces(str(item.get("tmdb_match_confidence") or "")).lower()
    return confidence == "low" and bool(item.get("tmdb_id"))


def build_match_review_alert(item: Dict[str, Any], affected_users: int) -> str:
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or "—"
    source_title = compact_spaces(str(item.get("source_title") or "")) or "—"
    tmdb_title = compact_spaces(str(item.get("tmdb_title") or item.get("tmdb_original_title") or "")) or "—"
    tmdb_id = item.get("tmdb_id") or "—"
    confidence = compact_spaces(str(item.get("tmdb_match_confidence") or "")) or "—"
    evidence = compact_spaces(str(item.get("tmdb_match_evidence") or "")) or "—"
    match_path = compact_spaces(str(item.get("tmdb_match_path") or "")) or "—"
    lines = [
        "🧪 <b>Match review required</b>",
        f"Kinozal ID: <code>{html.escape(str(kinozal_id))}</code>",
        f"Заголовок: {html.escape(source_title)}",
        f"TMDB: {html.escape(tmdb_title)} (id={html.escape(str(tmdb_id))})",
        f"Confidence: <code>{html.escape(confidence)}</code>",
        f"Path: <code>{html.escape(match_path)}</code>",
        f"Evidence: {html.escape(evidence)}",
        f"Затронет пользователей: {affected_users}",
        "Статус: <code>delivery held</code>",
        "Автодоставка удержана до ручного решения.",
        "",
    ]
    if item.get("tmdb_id"):
        lines.append(f"/approvematch {html.escape(str(kinozal_id))}")
        lines.append(f"/rejectmatch {html.escape(str(kinozal_id))}")
    lines.append(f"/nomatch {html.escape(str(kinozal_id))}")
    lines.append(f"/forcedeliver {html.escape(str(kinozal_id))}")
    lines.append(
        f"/overridematch {html.escape(str(kinozal_id))} "
        f"&lt;tmdb_id&gt; &lt;movie|tv&gt;"
    )
    lines.append(f"/matchcandidates {html.escape(str(kinozal_id))}")
    lines.append(f"/explainmatch {html.escape(str(kinozal_id))}")
    return "\n".join(lines)


async def notify_admins_about_match_review(
    bot: Any,
    item: Dict[str, Any],
    affected_users: int,
    skip_admin_ids: Iterable[int] = (),
) -> int:
    text = build_match_review_alert(item, affected_users)
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
    reply_markup = match_review_kb(kinozal_id, has_tmdb_match=bool(item.get("tmdb_id"))) if kinozal_id else None
    targets = ops_alert_chat_ids()
    if not targets:
        log.warning("Match review notification skipped: no ops/admin targets configured kinozal_id=%s", kinozal_id or "—")
        return 0
    skipped_admins = {int(admin_id) for admin_id in skip_admin_ids}
    sent_count = 0
    for target_chat_id in targets:
        target_chat_id = int(target_chat_id)
        if target_chat_id in skipped_admins:
            continue
        try:
            await bot.send_message(
                target_chat_id,
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=reply_markup,
            )
            sent_count += 1
        except Exception:
            log.exception("Match review send failed chat_id=%s kinozal_id=%s", target_chat_id, kinozal_id or "—")
    return sent_count


async def deliver_item_to_matching_subscriptions(db: Any, bot: Any, item: Dict[str, Any]) -> Tuple[int, int]:
    from delivery_sender import send_item_to_user

    item_id = int(item["id"])
    item_tmdb_id = int(item["tmdb_id"]) if item.get("tmdb_id") is not None else None
    matched_by_user: Dict[int, list[Dict[str, Any]]] = {}
    delivered_count = 0

    for sub in db.list_enabled_subscriptions():
        sub_full = db.get_subscription(int(sub["id"]))
        if not sub_full:
            continue
        tg_user_id = int(sub_full["tg_user_id"])
        if item_tmdb_id and db.is_title_muted(tg_user_id, item_tmdb_id):
            continue
        if not match_subscription(db, sub_full, item):
            continue
        matched_by_user.setdefault(tg_user_id, []).append(sub_full)

    for tg_user_id, matched_subs in matched_by_user.items():
        if db.delivered(tg_user_id, item_id) or db.delivered_equivalent(tg_user_id, item):
            continue
        await send_item_to_user(db, bot, tg_user_id, item, matched_subs)
        db.record_delivery(
            tg_user_id,
            item_id,
            int(matched_subs[0]["id"]),
            [int(sub["id"]) for sub in matched_subs],
            delivery_audit=build_delivery_audit(db, item, matched_subs, context="match_review"),
        )
        delivered_count += 1

    return len(matched_by_user), delivered_count
