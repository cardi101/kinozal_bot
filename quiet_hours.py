from datetime import datetime, timedelta, timezone
from typing import Any, Dict
from zoneinfo import ZoneInfo

from utils import compact_spaces

DEFAULT_QUIET_TIMEZONE = "UTC"


def normalize_quiet_timezone(value: Any) -> str:
    text = compact_spaces(str(value or ""))
    if not text:
        return DEFAULT_QUIET_TIMEZONE
    try:
        ZoneInfo(text)
        return text
    except Exception:
        return DEFAULT_QUIET_TIMEZONE


def quiet_window_active(start_h: int, end_h: int, current_h: int) -> bool:
    if start_h < end_h:
        return start_h <= current_h < end_h
    return current_h >= start_h or current_h < end_h


def quiet_window_status(
    start_h: int | None,
    end_h: int | None,
    tz_name: str,
    *,
    now: datetime | None = None,
) -> Dict[str, Any]:
    tz = ZoneInfo(normalize_quiet_timezone(tz_name))
    current_utc = now or datetime.now(timezone.utc)
    local_now = current_utc.astimezone(tz)
    local_hour = local_now.hour
    active = bool(
        start_h is not None
        and end_h is not None
        and quiet_window_active(int(start_h), int(end_h), local_hour)
    )
    return {
        "timezone": tz.key,
        "local_hour": local_hour,
        "active": active,
        "local_iso": local_now.isoformat(),
    }


def next_quiet_window_end_ts(
    start_h: int | None,
    end_h: int | None,
    tz_name: str,
    *,
    now: datetime | None = None,
) -> int:
    current_utc = now or datetime.now(timezone.utc)
    tz = ZoneInfo(normalize_quiet_timezone(tz_name))
    if start_h is None or end_h is None:
        return int(current_utc.timestamp())

    local_now = current_utc.astimezone(tz)
    start_h = int(start_h)
    end_h = int(end_h)
    if not quiet_window_active(start_h, end_h, local_now.hour):
        return int(current_utc.timestamp())

    if start_h < end_h:
        local_end = local_now.replace(hour=end_h, minute=0, second=0, microsecond=0)
    elif local_now.hour >= start_h:
        local_end = (local_now + timedelta(days=1)).replace(hour=end_h, minute=0, second=0, microsecond=0)
    else:
        local_end = local_now.replace(hour=end_h, minute=0, second=0, microsecond=0)

    return int(local_end.astimezone(timezone.utc).timestamp())
