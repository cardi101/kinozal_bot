import html
from typing import Any

from aiogram import Bot

from config import CFG
from service_helpers import _exc_brief, send_admins_text
from text_access import format_dt
from utils import utc_ts, compact_spaces


def _meta_int(db: Any, key: str, default: int = 0) -> int:
    try:
        value = db.get_meta(key)
        return int(value) if value is not None else default
    except Exception:
        return default


async def note_source_cycle_success(db: Any, bot: Bot) -> None:
    now_ts = utc_ts()
    prev_status = compact_spaces(db.get_meta("source_health_status") or "ok").lower() or "ok"
    fail_streak = _meta_int(db, "source_fail_streak", 0)
    last_failed_at = _meta_int(db, "source_last_failed_at", 0)
    last_error = compact_spaces(db.get_meta("source_last_error") or "")

    db.set_meta("source_last_success_at", str(now_ts))
    db.set_meta("source_fail_streak", "0")
    db.set_meta("source_health_status", "ok")

    if prev_status == "down" and fail_streak >= max(1, int(CFG.source_error_alert_threshold)):
        duration_min = max(0, (now_ts - last_failed_at) // 60) if last_failed_at else 0
        lines = [
            "✅ <b>Источник снова отвечает</b>",
            f"Восстановление: {html.escape(format_dt(now_ts))}",
        ]
        if duration_min:
            lines.append(f"Простой: ~{duration_min} мин.")
        if last_error:
            lines.append(f"Последняя ошибка: <code>{html.escape(last_error)}</code>")
        await send_admins_text(bot, "\n".join(lines))


async def note_source_cycle_failure(db: Any, bot: Bot, exc: Exception) -> None:
    now_ts = utc_ts()
    fail_streak = _meta_int(db, "source_fail_streak", 0) + 1
    last_alert_at = _meta_int(db, "source_last_alert_at", 0)
    last_success_at = _meta_int(db, "source_last_success_at", 0)
    repeat_seconds = max(60, int(CFG.source_error_alert_repeat_minutes) * 60)
    error_text = _exc_brief(exc)

    db.set_meta("source_fail_streak", str(fail_streak))
    db.set_meta("source_last_failed_at", str(now_ts))
    db.set_meta("source_last_error", error_text)
    db.set_meta("source_health_status", "down")

    threshold = max(1, int(CFG.source_error_alert_threshold))
    should_alert = fail_streak >= threshold and (last_alert_at <= 0 or now_ts - last_alert_at >= repeat_seconds)
    if not should_alert:
        return

    db.set_meta("source_last_alert_at", str(now_ts))
    lines = [
        "⚠️ <b>Сбой в цикле опроса источника</b>",
        f"Повторов подряд: {fail_streak}",
        f"Время: {html.escape(format_dt(now_ts))}",
        f"Ошибка: <code>{html.escape(error_text)}</code>",
    ]
    if last_success_at:
        lines.append(f"Последний успешный цикл: {html.escape(format_dt(last_success_at))}")
    await send_admins_text(bot, "\n".join(lines))
