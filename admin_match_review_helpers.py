import html
from typing import Any, Dict, Tuple

from aiogram.enums import ParseMode

from config import CFG
from delivery_sender import send_item_to_user
from keyboards import match_review_kb
from subscription_matching import match_subscription
from utils import compact_spaces


REVIEW_MATCH_CONFIDENCES = {"medium", "low", "unmatched"}


def item_requires_match_review(item: Dict[str, Any]) -> bool:
    confidence = compact_spaces(str(item.get("tmdb_match_confidence") or "")).lower()
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
    if confidence in {"medium", "low"} and bool(item.get("tmdb_id")):
        return True
    return confidence == "unmatched" and bool(kinozal_id)


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
        "",
    ]
    if item.get("tmdb_id"):
        lines.append(f"/approvematch {html.escape(str(kinozal_id))}")
        lines.append(f"/rejectmatch {html.escape(str(kinozal_id))}")
    lines.append(f"/nomatch {html.escape(str(kinozal_id))}")
    lines.append(f"/forcedeliver {html.escape(str(kinozal_id))}")
    lines.append(f"/overridematch {html.escape(str(kinozal_id))} <tmdb_id> <movie|tv>")
    lines.append(f"/matchcandidates {html.escape(str(kinozal_id))}")
    lines.append(f"/explainmatch {html.escape(str(kinozal_id))}")
    return "\n".join(lines)


async def notify_admins_about_match_review(bot: Any, item: Dict[str, Any], affected_users: int) -> None:
    text = build_match_review_alert(item, affected_users)
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
    reply_markup = match_review_kb(kinozal_id, has_tmdb_match=bool(item.get("tmdb_id"))) if kinozal_id else None
    if not CFG.admin_ids:
        return
    for admin_id in CFG.admin_ids:
        try:
            await bot.send_message(
                int(admin_id),
                text,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
                reply_markup=reply_markup,
            )
        except Exception:
            continue


async def deliver_item_to_matching_subscriptions(db: Any, bot: Any, item: Dict[str, Any]) -> Tuple[int, int]:
    item_id = int(item["id"])
    item_tmdb_id = int(item["tmdb_id"]) if item.get("tmdb_id") is not None else None
    matched_users = 0
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
        matched_users += 1
        if db.delivered(tg_user_id, item_id) or db.delivered_equivalent(tg_user_id, item):
            continue
        await send_item_to_user(db, bot, tg_user_id, item, [sub_full])
        db.record_delivery(tg_user_id, item_id, int(sub_full["id"]), [int(sub_full["id"])])
        delivered_count += 1

    return matched_users, delivered_count
