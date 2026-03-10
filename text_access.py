import html
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from config import CFG
from utils import utc_ts


def format_dt(ts: Optional[int]) -> str:
    if not ts:
        return "—"
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def user_access_state(user: Optional[Dict[str, Any]]) -> str:
    if not user:
        return "не найден"
    if int(user.get("is_active") or 0) != 1:
        return "неактивен"
    if int(user.get("access_granted") or 0) != 1:
        return "без доступа"
    expires_at = user.get("access_expires_at")
    if expires_at is not None and int(expires_at) <= utc_ts():
        return "истёк"
    if expires_at is None:
        return "активен бессрочно"
    return "активен"


def format_access_expiry(ts: Optional[int]) -> str:
    if ts is None:
        return "без срока"
    return format_dt(ts)


def human_media_type(value: str) -> str:
    return {"movie": "Фильмы", "tv": "Сериалы", "any": "Всё", "other": "Прочее"}.get(value or "any", value or "any")


def html_to_plain_text(text: str) -> str:
    text = text or ""
    text = re.sub(r"(?i)<br\s*/?>", "\n", text)
    text = re.sub(r"(?i)</p>", "\n\n", text)
    text = re.sub(r"(?i)</?(?:b|strong|i|em|u|code)>", "", text)
    text = re.sub(r'(?i)<a\s+[^>]*href="([^"]+)"[^>]*>(.*?)</a>', r'\2 (\1)', text)
    text = re.sub(r"(?s)<[^>]+>", "", text)
    text = html.unescape(text).replace("\xa0", " ")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ 	]+\n", "\n", text)
    return text.strip()


def require_access_message() -> str:
    if CFG.allow_mode == "open":
        return "Доступ должен выдаваться автоматически. Если не пустило — проверь конфиг."
    if CFG.allow_mode == "invite":
        return "У тебя пока нет доступа. Пришли /start КОД_ИНВАЙТА"
    return "У тебя пока нет доступа. Пусть админ добавит тебя через /grant USER_ID"
