from typing import Any

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from country_helpers import country_name_ru


def genres_kb(db: Any, sub_id: int, page: int = 0) -> InlineKeyboardMarkup:
    all_genres = list(db.get_all_genres_merged().items())
    selected = set(db.get_subscription_genres(sub_id))
    per_page = 8
    pages = max(1, (len(all_genres) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    chunk = all_genres[page * per_page : (page + 1) * per_page]

    kb = InlineKeyboardBuilder()
    for genre_id, name in chunk:
        mark = "✅" if genre_id in selected else "⬜️"
        kb.button(text=f"{mark} {name}", callback_data=f"subgenre:{sub_id}:{page}:{genre_id}")
    if pages > 1:
        kb.button(text="⬅️", callback_data=f"subgenrespage:{sub_id}:{page-1}")
        kb.button(text=f"{page+1}/{pages}", callback_data="noop")
        kb.button(text="➡️", callback_data=f"subgenrespage:{sub_id}:{page+1}")
    kb.button(text="Очистить жанры", callback_data=f"subgenresclear:{sub_id}:{page}")
    kb.button(text="Готово", callback_data=f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()


def countries_kb(db: Any, sub_id: int, page: int = 0, mode: str = "include") -> InlineKeyboardMarkup:
    all_codes = db.get_known_country_codes()
    selected = set(
        db.get_subscription_country_codes(sub_id)
        if mode == "include"
        else db.get_subscription_exclude_country_codes(sub_id)
    )
    for code in selected:
        if code not in all_codes:
            all_codes.append(code)
    all_codes = sorted(all_codes, key=lambda code: country_name_ru(code).lower())

    per_page = 8
    pages = max(1, (len(all_codes) + per_page - 1) // per_page)
    page = max(0, min(page, pages - 1))
    chunk = all_codes[page * per_page : (page + 1) * per_page]

    kb = InlineKeyboardBuilder()
    for code in chunk:
        mark = "✅" if code in selected else "⬜️"
        kb.button(text=f"{mark} {country_name_ru(code)}", callback_data=f"subcountry:{mode}:{sub_id}:{page}:{code}")
    if pages > 1:
        kb.button(text="⬅️", callback_data=f"subcountriespage:{mode}:{sub_id}:{page-1}")
        kb.button(text=f"{page+1}/{pages}", callback_data="noop")
        kb.button(text="➡️", callback_data=f"subcountriespage:{mode}:{sub_id}:{page+1}")
    clear_text = "Очистить страны" if mode == "include" else "Очистить исключения"
    kb.button(text=clear_text, callback_data=f"subcountriesclear:{mode}:{sub_id}:{page}")
    kb.button(text="Готово", callback_data=f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()


def content_filter_kb(db: Any, sub_id: int) -> InlineKeyboardMarkup:
    selected = str((db.get_subscription(sub_id) or {}).get("content_filter") or "any")
    options = [
        ("Любое", "any"),
        ("Только аниме", "only_anime"),
        ("Только дорамы", "only_dorama"),
        ("Без аниме", "exclude_anime"),
        ("Без дорам", "exclude_dorama"),
        ("Без аниме и дорам", "exclude_anime_dorama"),
    ]
    kb = InlineKeyboardBuilder()
    for text, code in options:
        mark = "✅" if code == selected else "⬜️"
        kb.button(text=f"{mark} {text}", callback_data=f"subcontent:{sub_id}:{code}")
    kb.button(text="◀️ Назад", callback_data=f"sub:view:{sub_id}")
    kb.adjust(1)
    return kb.as_markup()
