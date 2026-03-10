from typing import Any

from aiogram import F, Router
from aiogram.types import CallbackQuery

from access_helpers import ensure_access_for_callback
from dynamic_keyboards import content_filter_kb, genres_kb, countries_kb
from keyboards import format_kb, rating_kb, sub_type_kb, sub_view_kb, year_preset_kb
from service_helpers import safe_edit
from subscription_text import sub_summary


def register_subscription_filter_handlers(router: Router, db: Any) -> None:
    @router.callback_query(F.data.startswith("sub:edit_content_filter:"))
    async def cb_sub_edit_content_filter(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери подтип контента:", content_filter_kb(db, sub_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("subcontent:"))
    async def cb_sub_content_filter(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, code = callback.data.split(":")
        sub_id = int(sub_id_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        db.update_subscription(sub_id, content_filter=code)
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
        await callback.answer("Подтип обновлён")

    @router.callback_query(F.data.startswith("sub:edit_type:"))
    async def cb_sub_edit_type(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери тип контента:", sub_type_kb(sub_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("subtype:"))
    async def cb_subtype(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, media_type = callback.data.split(":")
        sub_id = int(sub_id_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        db.update_subscription(sub_id, media_type=media_type)
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
        await callback.answer("Тип обновлён")

    @router.callback_query(F.data.startswith("sub:edit_years:"))
    async def cb_sub_edit_years(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери диапазон лет:", year_preset_kb(sub_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("subyear:"))
    async def cb_subyear(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, code = callback.data.split(":")
        sub_id = int(sub_id_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        if code == "any":
            db.update_subscription(sub_id, year_from=None, year_to=None)
        else:
            db.update_subscription(sub_id, year_from=int(code), year_to=2100)
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
        await callback.answer("Годы обновлены")

    @router.callback_query(F.data.startswith("sub:edit_formats:"))
    async def cb_sub_edit_formats(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, "Переключай нужные форматы:", format_kb(sub_id, sub, "edit"))
        await callback.answer()

    @router.callback_query(F.data.startswith("subfmt:"))
    async def cb_sub_format_toggle(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, fmt, mode = callback.data.split(":")
        sub_id = int(sub_id_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        sub = db.get_subscription(sub_id)
        field = f"allow_{fmt}"
        db.update_subscription(sub_id, **{field: 0 if sub.get(field) else 1})
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, "Переключай нужные форматы:", format_kb(sub_id, sub, mode))
        await callback.answer()

    @router.callback_query(F.data.startswith("sub:edit_rating:"))
    async def cb_sub_edit_rating(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Минимальный рейтинг TMDB:", rating_kb(sub_id))
        await callback.answer()

    @router.callback_query(F.data.startswith("subrating:"))
    async def cb_sub_rating(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, code = callback.data.split(":")
        sub_id = int(sub_id_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        db.update_subscription(sub_id, min_tmdb_rating=None if code == "none" else float(code))
        sub = db.get_subscription(sub_id)
        await safe_edit(callback, sub_summary(db, sub), sub_view_kb(sub_id, sub))
        await callback.answer("Рейтинг обновлён")

    @router.callback_query(F.data.startswith("sub:edit_genres:"))
    async def cb_sub_edit_genres(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        parts = callback.data.split(":")
        sub_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери жанры:", genres_kb(db, sub_id, page))
        await callback.answer()

    @router.callback_query(F.data.startswith("subgenre:"))
    async def cb_sub_genre_toggle(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, page_str, genre_id_str = callback.data.split(":")
        sub_id = int(sub_id_str)
        page = int(page_str)
        genre_id = int(genre_id_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        db.toggle_subscription_genre(sub_id, genre_id)
        await safe_edit(callback, "Выбери жанры:", genres_kb(db, sub_id, page))
        await callback.answer()

    @router.callback_query(F.data.startswith("subgenrespage:"))
    async def cb_sub_genres_page(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, page_str = callback.data.split(":")
        sub_id = int(sub_id_str)
        page = int(page_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери жанры:", genres_kb(db, sub_id, page))
        await callback.answer()

    @router.callback_query(F.data.startswith("subgenresclear:"))
    async def cb_sub_genres_clear(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, sub_id_str, page_str = callback.data.split(":")
        sub_id = int(sub_id_str)
        page = int(page_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        db.set_subscription_genres(sub_id, [])
        await safe_edit(callback, "Выбери жанры:", genres_kb(db, sub_id, page))
        await callback.answer("Жанры очищены")

    @router.callback_query(F.data.startswith("sub:edit_countries:"))
    async def cb_sub_edit_countries(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        parts = callback.data.split(":")
        sub_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери страны, которые нужно включать:", countries_kb(db, sub_id, page, "include"))
        await callback.answer()

    @router.callback_query(F.data.startswith("sub:edit_exclude_countries:"))
    async def cb_sub_edit_exclude_countries(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        parts = callback.data.split(":")
        sub_id = int(parts[2])
        page = int(parts[3]) if len(parts) > 3 else 0
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        await safe_edit(callback, "Выбери страны, которые нужно исключать:", countries_kb(db, sub_id, page, "exclude"))
        await callback.answer()

    @router.callback_query(F.data.startswith("subcountry:"))
    async def cb_sub_country_toggle(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, mode, sub_id_str, page_str, country_code = callback.data.split(":")
        sub_id = int(sub_id_str)
        page = int(page_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        if mode == "exclude":
            db.toggle_subscription_exclude_country_code(sub_id, country_code)
            title = "Выбери страны, которые нужно исключать:"
        else:
            db.toggle_subscription_country_code(sub_id, country_code)
            title = "Выбери страны, которые нужно включать:"
            mode = "include"
        await safe_edit(callback, title, countries_kb(db, sub_id, page, mode))
        await callback.answer()

    @router.callback_query(F.data.startswith("subcountriespage:"))
    async def cb_sub_countries_page(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, mode, sub_id_str, page_str = callback.data.split(":")
        sub_id = int(sub_id_str)
        page = int(page_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        title = "Выбери страны, которые нужно исключать:" if mode == "exclude" else "Выбери страны, которые нужно включать:"
        await safe_edit(callback, title, countries_kb(db, sub_id, page, mode))
        await callback.answer()

    @router.callback_query(F.data.startswith("subcountriesclear:"))
    async def cb_sub_countries_clear(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        _, mode, sub_id_str, page_str = callback.data.split(":")
        sub_id = int(sub_id_str)
        page = int(page_str)
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return
        if mode == "exclude":
            db.set_subscription_exclude_country_codes(sub_id, [])
            title = "Выбери страны, которые нужно исключать:"
            done = "Исключаемые страны очищены"
        else:
            db.set_subscription_country_codes(sub_id, [])
            title = "Выбери страны, которые нужно включать:"
            done = "Страны очищены"
            mode = "include"
        await safe_edit(callback, title, countries_kb(db, sub_id, page, mode))
        await callback.answer(done)
