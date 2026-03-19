import html
from typing import Any

from aiogram import F, Router
from aiogram.enums import ParseMode
from aiogram.types import CallbackQuery

from access_helpers import ensure_access_for_callback
from config import CFG
from delivery_sender import send_item_to_user
from kinozal_details import enrich_kinozal_item_with_details
from subscription_test_helpers import get_live_test_items_for_subscription


def register_subscription_test_handlers(router: Router, db: Any, source: Any, tmdb: Any) -> None:
    @router.callback_query(F.data.startswith("sub:test:"))
    async def cb_sub_test(callback: CallbackQuery) -> None:
        if not await ensure_access_for_callback(db, callback):
            return
        sub_id = int(callback.data.split(":")[2])
        if not db.subscription_belongs_to(sub_id, callback.from_user.id):
            await callback.answer("Это не твоя подписка", show_alert=True)
            return

        sub = db.get_subscription(sub_id)
        items = await get_live_test_items_for_subscription(db, source, tmdb, sub_id, limit=5)

        if items:
            await callback.message.answer(
                f"Тест для <b>{html.escape(sub['name'])}</b>:\n<i>Показываю самые свежие совпадения с верха ленты.</i>",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=CFG.disable_preview,
            )
            for item in items:
                item = await enrich_kinozal_item_with_details(dict(item))

                await send_item_to_user(db, callback.bot, callback.message.chat.id, item, [sub])
            await callback.answer("Показал свежие")
            return

        fallback_items = db.get_last_items_for_subscription(sub_id, 5)
        if fallback_items:
            await callback.message.answer(
                f"Тест для <b>{html.escape(sub['name'])}</b>:\n<i>Свежих совпадений сверху ленты сейчас не нашлось, показываю последние совпадения из базы.</i>",
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=CFG.disable_preview,
            )
            for item in fallback_items:
                item = await enrich_kinozal_item_with_details(dict(item))

                await send_item_to_user(db, callback.bot, callback.message.chat.id, item, [sub])
            await callback.answer("Показал из базы")
            return

        await callback.answer("Совпадений среди свежих релизов пока нет", show_alert=True)
