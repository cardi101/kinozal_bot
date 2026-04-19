from datetime import datetime
from typing import Any, Dict, List

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def main_menu_kb(is_admin: bool, quiet_active: bool = False) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📂 Мои подписки", callback_data="menu:subs")
    kb.button(text="✨ Новая подписка", callback_data="menu:new")
    kb.button(text="📰 Последние релизы", callback_data="menu:latest")
    kb.button(text="ℹ️ Мой ID", callback_data="menu:whoami")
    quiet_label = "🌙 Тихий режим ✅" if quiet_active else "🌙 Тихий режим"
    kb.button(text=quiet_label, callback_data="menu:quiet")
    kb.button(text="🔕 Заглушённые", callback_data="menu:muted")
    if is_admin:
        kb.button(text="🛠 Инвайты", callback_data="menu:admin_invites")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def subscriptions_list_kb(subs: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for sub in subs:
        title = f"{'🟢' if sub.get('is_enabled') else '⏸'} #{sub['id']} {sub['name']}"
        kb.button(text=title[:55], callback_data=f"sub:view:{sub['id']}")
    kb.button(text="✨ Создать ещё", callback_data="menu:new")
    kb.button(text="◀️ В меню", callback_data="menu:root")
    kb.adjust(1)
    return kb.as_markup()


def sub_view_kb(sub_id: int, sub: Dict[str, Any]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅/⏸ Вкл/выкл", callback_data=f"sub:toggle:{sub_id}")
    kb.button(text="🧪 Тест", callback_data=f"sub:test:{sub_id}")
    kb.button(text="⚡ Пресеты", callback_data=f"sub:edit_presets:{sub_id}")
    kb.button(text="🎬 Тип", callback_data=f"sub:edit_type:{sub_id}")
    kb.button(text="🎭 Подтип", callback_data=f"sub:edit_content_filter:{sub_id}")
    kb.button(text="📆 Годы", callback_data=f"sub:edit_years:{sub_id}")
    kb.button(text="📺 Форматы", callback_data=f"sub:edit_formats:{sub_id}")
    kb.button(text="⭐ Рейтинг", callback_data=f"sub:edit_rating:{sub_id}")
    kb.button(text="🏷 Жанры", callback_data=f"sub:edit_genres:{sub_id}:0")
    kb.button(text="🌍 Страны", callback_data=f"sub:edit_countries:{sub_id}:0")
    kb.button(text="🚫 Страны", callback_data=f"sub:edit_exclude_countries:{sub_id}:0")
    kb.button(text="🔎 Ключи", callback_data=f"sub:edit_keywords:{sub_id}")
    kb.button(text="✏️ Имя", callback_data=f"sub:rename:{sub_id}")
    kb.button(text="🗑 Удалить", callback_data=f"sub:delete:{sub_id}")
    kb.button(text="◀️ К списку", callback_data="menu:subs")
    kb.adjust(2, 1, 2, 2, 2, 2, 2, 2)
    return kb.as_markup()


def sub_type_kb(sub_id: int, prefix: str = "subtype") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🎞 Фильмы", callback_data=f"{prefix}:{sub_id}:movie")
    kb.button(text="📺 Сериалы", callback_data=f"{prefix}:{sub_id}:tv")
    kb.button(text="🧩 Всё", callback_data=f"{prefix}:{sub_id}:any")
    kb.button(text="◀️ Назад", callback_data=f"sub:view:{sub_id}")
    kb.adjust(2, 1, 1)
    return kb.as_markup()


def year_preset_kb(sub_id: int, prefix: str = "subyear") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    current_year = datetime.now().year
    presets = [
        ("Любые", "any"),
        (f"{current_year}+", str(current_year)),
        ("2024+", "2024"),
        ("2020+", "2020"),
        ("2015+", "2015"),
    ]
    for text, code in presets:
        kb.button(text=text, callback_data=f"{prefix}:{sub_id}:{code}")
    kb.button(text="✍️ Ввести вручную", callback_data=f"sub:ask_years:{sub_id}")
    kb.button(text="◀️ Назад", callback_data=f"sub:view:{sub_id}")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()


def rating_kb(sub_id: int, prefix: str = "subrating") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    options = [("Без фильтра", "none"), ("6.5+", "6.5"), ("7.0+", "7.0"), ("7.5+", "7.5"), ("8.0+", "8.0")]
    for text, code in options:
        kb.button(text=text, callback_data=f"{prefix}:{sub_id}:{code}")
    kb.button(text="◀️ Назад", callback_data=f"sub:view:{sub_id}")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()


def format_kb(sub_id: int, sub: Dict[str, Any], mode: str = "edit") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    done_cb = f"wizfmtdone:{sub_id}" if mode == "wiz" else f"sub:view:{sub_id}"
    back_cb = f"sub:view:{sub_id}" if mode == "edit" else f"sub:delete:{sub_id}"
    for value in ("720", "1080", "2160"):
        enabled = bool(sub.get(f"allow_{value}"))
        kb.button(
            text=f"{'✅' if enabled else '⬜️'} {value}p",
            callback_data=f"subfmt:{sub_id}:{value}:{mode}",
        )
    kb.button(text="Готово", callback_data=done_cb)
    kb.button(text="◀️ Назад", callback_data=back_cb)
    kb.adjust(2, 1, 1)
    return kb.as_markup()


def preset_kb(sub_id: int, flow: str = "edit") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    options = [
        ("🌍 Новинки — мир", "world"),
        ("🇹🇷 Новинки — Турция", "turkey"),
        ("🌸 Новинки — дорамы", "dorama"),
        ("🍥 Новинки — аниме", "anime"),
    ]
    for text, code in options:
        kb.button(text=text, callback_data=f"subpreset:{sub_id}:{flow}:{code}")
    custom_text = "🛠 Своя настройка" if flow == "new" else "🛠 Оставить как есть"
    kb.button(text=custom_text, callback_data=f"subpreset:{sub_id}:{flow}:custom")
    kb.button(text="❌ Отмена" if flow == "new" else "◀️ Назад", callback_data=f"sub:delete:{sub_id}" if flow == "new" else f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()


def wizard_type_kb(sub_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🎞 Фильмы", callback_data=f"wiztype:{sub_id}:movie")
    kb.button(text="📺 Сериалы", callback_data=f"wiztype:{sub_id}:tv")
    kb.button(text="🧩 Всё", callback_data=f"wiztype:{sub_id}:any")
    kb.button(text="❌ Отмена", callback_data=f"sub:delete:{sub_id}")
    kb.adjust(2, 1, 1)
    return kb.as_markup()


def wizard_years_kb(sub_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    current_year = datetime.now().year
    for text, code in [("Любые", "any"), (f"{current_year}+", str(current_year)), ("2024+", "2024"), ("2020+", "2020")]:
        kb.button(text=text, callback_data=f"wizyear:{sub_id}:{code}")
    kb.button(text="✍️ Ввести вручную позже", callback_data=f"wizyear:{sub_id}:manualskip")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def wizard_rating_kb(sub_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for text, code in [("Без фильтра", "none"), ("6.5+", "6.5"), ("7.0+", "7.0"), ("7.5+", "7.5")]:
        kb.button(text=text, callback_data=f"wizrating:{sub_id}:{code}")
    kb.adjust(2, 2)
    return kb.as_markup()


def admin_invites_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🧾 Последние инвайты", callback_data="admin:invites")
    kb.button(text="👥 Пользователи", callback_data="admin:users:0")
    kb.button(text="◀️ В меню", callback_data="menu:root")
    kb.adjust(1)
    return kb.as_markup()


def admin_users_kb(page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_prev:
        kb.button(text="⬅️ Назад", callback_data=f"admin:users:{page - 1}")
    if has_next:
        kb.button(text="➡️ Дальше", callback_data=f"admin:users:{page + 1}")
    kb.button(text="🧾 Инвайты", callback_data="admin:invites")
    kb.button(text="◀️ В меню", callback_data="menu:root")
    kb.adjust(2, 2)
    return kb.as_markup()


def mute_title_kb(tmdb_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔕 Не уведомлять о названии", callback_data=f"mute_title:{tmdb_id}")
    return kb.as_markup()


def unmute_title_kb(tmdb_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="↩️ Отменить", callback_data=f"unmute_title:{tmdb_id}")
    return kb.as_markup()


def quiet_hours_kb(current_start, current_end) -> InlineKeyboardMarkup:
    presets = [
        ("🌙 23:00–07:00 UTC", "23", "7"),
        ("🌙 22:00–06:00 UTC", "22", "6"),
        ("🌙 21:00–05:00 UTC", "21", "5"),
        ("🌙 20:00–04:00 UTC", "20", "4"),
    ]
    kb = InlineKeyboardBuilder()
    for label, s, e in presets:
        active = current_start == int(s) and current_end == int(e)
        kb.button(text=("✅ " if active else "") + label, callback_data=f"quiet:{s}:{e}")
    kb.button(text="🔔 Отключить", callback_data="quiet:off")
    kb.button(text="◀️ В меню", callback_data="menu:root")
    kb.adjust(2, 2, 1, 1)
    return kb.as_markup()


def match_review_kb(kinozal_id: str, has_tmdb_match: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if has_tmdb_match:
        kb.button(text="✅ Approve", callback_data=f"matchreview:approve:{kinozal_id}")
        kb.button(text="⛔ Reject", callback_data=f"matchreview:reject:{kinozal_id}")
    kb.button(text="🚫 No Match", callback_data=f"matchreview:no_match:{kinozal_id}")
    kb.button(text="🔔 Отправить уведомление", callback_data=f"matchreview:deliver:{kinozal_id}")
    kb.button(text="📨 Force Deliver", callback_data=f"matchreview:force:{kinozal_id}")
    kb.button(text="🔎 Candidates", callback_data=f"matchreview:candidates:{kinozal_id}")
    kb.button(text="🧭 Explain", callback_data=f"matchreview:explain:{kinozal_id}")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def anomaly_alert_kb(kinozal_id: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔎 Timeline", callback_data=f"anomaly:timeline:{kinozal_id}")
    kb.button(text="📨 Replay", callback_data=f"anomaly:replay:{kinozal_id}")
    kb.adjust(2)
    return kb.as_markup()


def match_candidates_kb(kinozal_id: str, candidates: List[Dict[str, Any]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for row in candidates[:8]:
        tmdb_id = row.get("tmdb_id")
        media_type = str(row.get("media_type") or "movie")
        if not tmdb_id:
            continue
        title = str(row.get("title") or row.get("original_title") or "TMDB")
        kb.button(
            text=f"Use {int(tmdb_id)} {media_type} · {title[:20]}",
            callback_data=f"matchpick:{kinozal_id}:{int(tmdb_id)}:{media_type}",
        )
    kb.adjust(1)
    return kb.as_markup()
