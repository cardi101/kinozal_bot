import html
import logging
from typing import Any, Dict, List, Optional, Tuple

from content_buckets import item_content_bucket
from country_helpers import effective_item_countries, human_country_names, parse_jsonish_list
from parsing_basic import parse_year
from release_versioning import resolve_item_kinozal_id
from subscription_matching import match_subscription
from text_access import human_media_type
from tmdb_aliases import title_search_candidates
from utils import compact_spaces


log = logging.getLogger(__name__)


def build_match_explanation(db: Any, item: Dict[str, Any], live_item: Optional[Dict[str, Any]] = None) -> str:
    display_item = live_item or item
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item) or "—"
    media = str(display_item.get("media_type") or "movie")
    bucket = item_content_bucket(display_item)
    category_name = compact_spaces(str(display_item.get("source_category_name") or "")) or "—"
    category_id = compact_spaces(str(display_item.get("source_category_id") or "")) or "—"
    countries = effective_item_countries(display_item)
    country_names = human_country_names(countries, limit=8)
    candidates = title_search_candidates(display_item.get("source_title") or "", display_item.get("cleaned_title") or "")
    matched_subs: List[Dict[str, Any]] = []
    for sub in db.list_enabled_subscriptions():
        sub_full = db.get_subscription(int(sub["id"]))
        if not sub_full:
            continue
        if match_subscription(db, sub_full, display_item):
            matched_subs.append(sub_full)

    lines = [
        f"🧭 <b>Explain match</b> — Kinozal ID <code>{html.escape(kinozal_id)}</code>",
        f"Заголовок: {html.escape(compact_spaces(str(display_item.get('source_title') or '—')))}",
        f"TMDB в БД: {html.escape('есть' if item.get('tmdb_id') else 'нет')}",
    ]
    if item.get("tmdb_id"):
        lines.append(
            f"TMDB в БД title: {html.escape(compact_spaces(str(item.get('tmdb_title') or item.get('tmdb_original_title') or '—')))} "
            f"(id={int(item.get('tmdb_id') or 0)}, year={parse_year(str(item.get('tmdb_release_date') or '')) or '—'})"
        )
    if live_item is not None:
        lines.append(f"TMDB live: {html.escape('есть' if live_item.get('tmdb_id') else 'нет')}")
        if live_item.get("tmdb_id"):
            lines.append(
                f"TMDB live title: {html.escape(compact_spaces(str(live_item.get('tmdb_title') or live_item.get('tmdb_original_title') or '—')))} "
                f"(id={int(live_item.get('tmdb_id') or 0)}, year={parse_year(str(live_item.get('tmdb_release_date') or '')) or '—'})"
            )
    lines.extend([
        f"Media: {html.escape(human_media_type(media))}",
        f"Bucket: {html.escape(bucket)}",
        f"Категория API: {html.escape(category_name)} ({html.escape(category_id)})",
        f"Страны: {html.escape(', '.join(country_names or countries or ['—']))}",
        f"Manual route: bucket={html.escape(compact_spaces(str(display_item.get('manual_bucket') or '')) or '—')} | countries={html.escape(','.join(parse_jsonish_list(display_item.get('manual_country_codes')) or [] ) or '—')}",
        f"Кандидаты TMDB: {html.escape(', '.join(candidates[:8]) if candidates else 'не извлеклись')}",
        f"Подходящих подписок сейчас: {len(matched_subs)}",
    ])
    for sub in matched_subs[:12]:
        lines.append(
            f"• <code>{int(sub['tg_user_id'])}</code> — {html.escape(sub.get('name') or 'без названия')} "
            f"[{html.escape(sub.get('preset_key') or 'custom')}]"
        )
    if len(matched_subs) > 12:
        lines.append(f"… ещё {len(matched_subs) - 12}")
    if not display_item.get("tmdb_id") and category_name != "—":
        lines.append("")
        lines.append("Фолбэк сейчас работает через source category.")
    return "\n".join(lines)


async def rematch_item_live(db: Any, tmdb: Any, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
    before = dict(item)
    try:
        enriched = await tmdb.enrich_item(dict(item))
        db.save_item(enriched)
        refreshed = db.get_item(int(item["id"])) or db.find_item_by_kinozal_id(str(item.get("kinozal_id") or ""))
        return before, refreshed, True
    except Exception:
        log.exception("Rematch failed for item_id=%s kinozal_id=%s", item.get("id"), item.get("kinozal_id"))
        return before, None, False
