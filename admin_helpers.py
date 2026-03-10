import html
from typing import Any, Dict, List, Optional, Tuple

from config import CFG
from release_versioning import extract_kinozal_id
from subscription_text import sub_summary
from text_access import format_dt, user_access_state, format_access_expiry
from utils import compact_spaces


def is_admin(user_id: int) -> bool:
    return user_id in CFG.admin_ids


def extract_kinozal_id_from_text(text: str) -> Optional[str]:
    return extract_kinozal_id(text)


def parse_admin_route_target(raw: str) -> Tuple[Optional[str], List[str], str]:
    token = compact_spaces((raw or "").strip()).lower()
    mapping = {
        "anime": ("anime", [], "аниме"),
        "аниме": ("anime", [], "аниме"),
        "dorama": ("dorama", [], "дорамы"),
        "дорама": ("dorama", [], "дорамы"),
        "дорамы": ("dorama", [], "дорамы"),
        "world": ("regular", [], "мир"),
        "мир": ("regular", [], "мир"),
        "regular": ("regular", [], "обычное"),
        "обычное": ("regular", [], "обычное"),
        "turkey": ("regular", ["TR"], "Турция"),
        "turkish": ("regular", ["TR"], "Турция"),
        "турция": ("regular", ["TR"], "Турция"),
        "tr": ("regular", ["TR"], "Турция"),
    }
    return mapping.get(token, (None, [], ""))


def format_admin_user_line(user: Dict[str, Any]) -> str:
    state = user_access_state(user)
    username = compact_spaces(str(user.get("username") or ""))
    first_name = compact_spaces(str(user.get("first_name") or ""))
    label_parts = []
    if first_name:
        label_parts.append(first_name)
    if username:
        label_parts.append(f"@{username}")
    label = " / ".join(label_parts) if label_parts else "без имени"
    total_subs = int(user.get("subscriptions_total") or 0)
    enabled_subs = int(user.get("subscriptions_enabled") or 0)
    return (
        f"• <code>{user['tg_user_id']}</code> — {html.escape(label)}\n"
        f"  Статус: {html.escape(state)} | до: {html.escape(format_access_expiry(user.get('access_expires_at')))}\n"
        f"  Подписок: {total_subs} (вкл: {enabled_subs})"
    )


def format_admin_user_details(db: Any, user: Dict[str, Any]) -> str:
    state = user_access_state(user)
    username = compact_spaces(str(user.get("username") or ""))
    first_name = compact_spaces(str(user.get("first_name") or ""))
    lines = [
        f"👤 Пользователь <code>{user['tg_user_id']}</code>",
        f"Имя: {html.escape(first_name or '—')}",
        f"Username: {html.escape('@' + username if username else '—')}",
        f"Статус доступа: {html.escape(state)}",
        f"Доступ до: {html.escape(format_access_expiry(user.get('access_expires_at')))}",
        f"Создан: {html.escape(format_dt(user.get('created_at')))}",
    ]
    subs = user.get("subscriptions") or []
    if subs:
        lines.append("")
        lines.append("Подписки:")
        for sub in subs:
            sub_full = db.get_subscription(int(sub["id"])) or sub
            lines.append(sub_summary(db, sub_full))
            lines.append("")
        if lines[-1] == "":
            lines.pop()
    else:
        lines.append("")
        lines.append("Подписок нет.")
    return "\n".join(lines)


def parse_command_payload(text: Optional[str]) -> str:
    raw = text or ""
    parts = raw.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""
