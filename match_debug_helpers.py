import html
import logging
import json
from typing import Any, Dict, List, Optional, Tuple

from content_buckets import resolve_item_content_bucket
from country_helpers import effective_item_countries, human_country_names, parse_jsonish_list
from parsing_basic import parse_year
from release_versioning import resolve_item_kinozal_id
from subscription_matching import explain_subscription_match
from text_access import human_media_type
from tmdb_aliases import title_search_candidates
from utils import compact_spaces


log = logging.getLogger(__name__)


def _strip_existing_match_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(item)
    for key in [
        "tmdb_id",
        "tmdb_title",
        "tmdb_original_title",
        "tmdb_original_language",
        "tmdb_overview",
        "tmdb_poster_url",
        "tmdb_backdrop_url",
        "tmdb_release_date",
        "tmdb_vote_average",
        "tmdb_vote_count",
        "tmdb_genre_ids",
        "tmdb_genres",
        "tmdb_media_type",
        "tmdb_status",
        "tmdb_countries",
        "tmdb_episode_run_time",
        "tmdb_number_of_seasons",
        "tmdb_number_of_episodes",
        "tmdb_origin_country",
        "tmdb_spoken_languages",
        "tmdb_external_ids_json",
        "tmdb_search_query",
        "tmdb_search_lang",
        "tmdb_search_path",
        "tmdb_match_score",
        "tmdb_match_reason",
        "tmdb_match_debug",
        "tmdb_match_path",
        "tmdb_match_confidence",
        "tmdb_match_evidence",
        "imdb_id",
        "media_type",
    ]:
        cleaned.pop(key, None)
    cleaned["_clear_tmdb_match"] = True
    return cleaned



def _humanize_subscription_reason(reason: str) -> str:
    if reason == "passed":
        return "подходит"
    if reason == "globally_ignored":
        return "глобально проигнорировано"
    if reason == "missing_sub_or_item":
        return "нет подписки или релиза"
    if reason == "disabled":
        return "подписка выключена"
    if reason == "media_other_mismatch":
        return "не подходит по типу media=other"
    if reason == "year_missing":
        return "не удалось определить год"
    if reason == "rating_missing":
        return "нет рейтинга TMDB"
    if reason == "include_keyword_mismatch":
        return "не совпали обязательные ключи"
    if reason.startswith("media_mismatch:"):
        return f"не подходит тип: {reason.split(':', 1)[1]}"
    if reason.startswith("year_mismatch:"):
        return f"не подходит по году: {reason.split(':', 1)[1]}"
    if reason.startswith("format_mismatch:"):
        return f"не подходит формат: {reason.split(':', 1)[1] or '—'}"
    if reason.startswith("rating_mismatch:"):
        return f"рейтинг ниже фильтра: {reason.split(':', 1)[1]}"
    if reason.startswith("genre_mismatch:"):
        return f"не подходят жанры: {reason.split(':', 1)[1]}"
    if reason.startswith("bucket_mismatch:"):
        return f"не подходит подтип: {reason.split(':', 1)[1]}"
    if reason.startswith("bucket_excluded:"):
        return f"подтип исключён: {reason.split(':', 1)[1]}"
    if reason == "country_missing":
        return "не удалось определить страну"
    if reason.startswith("country_mismatch:"):
        return f"не подходят страны: {reason.split(':', 1)[1]}"
    if reason.startswith("excluded_country:"):
        return f"страна в исключениях: {reason.split(':', 1)[1]}"
    if reason.startswith("exclude_keyword:"):
        return f"сработал минус-ключ: {reason.split(':', 1)[1]}"
    return reason


def build_match_explanation(db: Any, item: Dict[str, Any], live_item: Optional[Dict[str, Any]] = None) -> str:
    display_item = live_item or item
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item) or "—"
    media = str(display_item.get("media_type") or "movie")
    bucket_decision = resolve_item_content_bucket(display_item)
    bucket = bucket_decision["bucket"]
    bucket_reason = bucket_decision["reason"]
    category_name = compact_spaces(str(display_item.get("source_category_name") or "")) or "—"
    category_id = compact_spaces(str(display_item.get("source_category_id") or "")) or "—"
    countries = effective_item_countries(display_item)
    country_names = human_country_names(countries, limit=8)
    candidates = title_search_candidates(display_item.get("source_title") or "", display_item.get("cleaned_title") or "")
    match_path = compact_spaces(str(display_item.get("tmdb_match_path") or item.get("tmdb_match_path") or "")) or "—"
    match_confidence = compact_spaces(str(display_item.get("tmdb_match_confidence") or item.get("tmdb_match_confidence") or "")) or "—"
    match_evidence = compact_spaces(str(display_item.get("tmdb_match_evidence") or item.get("tmdb_match_evidence") or "")) or "—"
    match_debug_raw = compact_spaces(str(display_item.get("tmdb_match_debug") or item.get("tmdb_match_debug") or ""))
    match_debug_summary = "—"
    if match_debug_raw:
        try:
            debug_events = json.loads(match_debug_raw)
            if isinstance(debug_events, list):
                summary_parts: List[str] = []
                for event in debug_events[-6:]:
                    if not isinstance(event, dict):
                        continue
                    stage = compact_spaces(str(event.get("stage") or "")) or "event"
                    reason = compact_spaces(str(event.get("reason") or ""))
                    reason_code = compact_spaces(str(event.get("reason_code") or ""))
                    tmdb_id = compact_spaces(str(event.get("tmdb_id") or ""))
                    tmdb_title = compact_spaces(str(event.get("tmdb_title") or ""))
                    query = compact_spaces(str(event.get("query") or ""))
                    candidate_score = compact_spaces(str(event.get("candidate_score") or ""))
                    features = event.get("features") if isinstance(event.get("features"), dict) else {}
                    piece = stage
                    if query:
                        piece += f" q={query}"
                    if tmdb_id:
                        piece += f" id={tmdb_id}"
                    if tmdb_title:
                        piece += f" title={tmdb_title}"
                    if candidate_score:
                        piece += f" score={candidate_score}"
                    if features:
                        similarity = compact_spaces(str(features.get("title_similarity") or ""))
                        overlap = compact_spaces(str(features.get("title_overlap") or ""))
                        year_delta = compact_spaces(str(features.get("year_delta") or ""))
                        if similarity:
                            piece += f" sim={similarity}"
                        if overlap:
                            piece += f" overlap={overlap}"
                        if year_delta:
                            piece += f" year_delta={year_delta}"
                    if reason_code:
                        piece += f" code={reason_code}"
                    if reason and reason != reason_code:
                        piece += f" reason={reason}"
                    summary_parts.append(piece)
                if summary_parts:
                    match_debug_summary = " | ".join(summary_parts)
        except Exception:
            match_debug_summary = match_debug_raw

    matched_subs: List[Dict[str, Any]] = []
    rejected_subs: List[Tuple[Dict[str, Any], str]] = []

    for sub in db.list_enabled_subscriptions():
        sub_full = db.get_subscription(int(sub["id"]))
        if not sub_full:
            continue
        reason = explain_subscription_match(db, sub_full, display_item)
        if reason == "passed":
            matched_subs.append(sub_full)
        else:
            rejected_subs.append((sub_full, reason))

    lines = [
        f"🧭 <b>Explain match</b> — Kinozal ID <code>{html.escape(kinozal_id)}</code>",
        f"Заголовок: {html.escape(compact_spaces(str(display_item.get('source_title') or '—')))}",
        f"TMDB в БД: {html.escape('есть' if item.get('tmdb_id') else 'нет')}",
    ]

    source_imdb_id = compact_spaces(str(display_item.get("source_imdb_id") or item.get("source_imdb_id") or ""))
    stored_imdb_id = compact_spaces(str(item.get("imdb_id") or ""))
    if source_imdb_id:
        lines.append(f"Source IMDb: {html.escape(source_imdb_id)}")
    else:
        lines.append("Source IMDb: —")
    if stored_imdb_id:
        lines.append(f"Stored IMDb: {html.escape(stored_imdb_id)}")

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
        f"Bucket reason: {html.escape(bucket_reason)}",
        f"Категория API: {html.escape(category_name)} ({html.escape(category_id)})",
        f"Страны: {html.escape(', '.join(country_names or countries or ['—']))}",
        f"Manual route: bucket={html.escape(compact_spaces(str(display_item.get('manual_bucket') or '')) or '—')} | countries={html.escape(','.join(parse_jsonish_list(display_item.get('manual_country_codes')) or []) or '—')}",
        f"Кандидаты TMDB: {html.escape(', '.join(candidates[:8]) if candidates else 'не извлеклись')}",
        f"TMDB match path: {html.escape(match_path)}",
        f"TMDB confidence: {html.escape(match_confidence)}",
        f"TMDB evidence: {html.escape(match_evidence)}",
        f"TMDB debug: {html.escape(match_debug_summary)}",
        f"Подходящих подписок сейчас: {len(matched_subs)}",
    ])

    for sub in matched_subs[:12]:
        lines.append(
            f"• ✅ #{int(sub['id'])} — {html.escape(sub.get('name') or 'без названия')} "
            f"[{html.escape(sub.get('preset_key') or 'custom')}]"
        )
    if len(matched_subs) > 12:
        lines.append(f"… ещё подошло {len(matched_subs) - 12}")

    if rejected_subs:
        lines.append("")
        lines.append("Почему не подошли остальные подписки:")
        for sub, reason in rejected_subs[:20]:
            lines.append(
                f"• ❌ #{int(sub['id'])} — {html.escape(sub.get('name') or 'без названия')} "
                f"[{html.escape(sub.get('preset_key') or 'custom')}] → "
                f"{html.escape(_humanize_subscription_reason(reason))} "
                f"<code>{html.escape(reason)}</code>"
            )
        if len(rejected_subs) > 20:
            lines.append(f"… ещё не подошло {len(rejected_subs) - 20}")

    if not display_item.get("tmdb_id") and category_name != "—":
        lines.append("")
        lines.append("Фолбэк сейчас работает через source category.")

    return "\n".join(lines)


async def rematch_item_live(db: Any, tmdb: Any, item: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], bool]:
    before = dict(item)
    try:
        rematch_input = _strip_existing_match_fields(item)
        rematch_input["_skip_kinozal_override"] = True
        enriched = await tmdb.enrich_item(rematch_input)
        kinozal_id = compact_spaces(str(item.get("kinozal_id") or "")) or resolve_item_kinozal_id(item)
        if kinozal_id and hasattr(db, "delete_match_override"):
            db.delete_match_override(kinozal_id)
        saved_id = int(item.get("id") or 0)
        save_result = db.save_item(enriched)
        if isinstance(save_result, tuple) and save_result:
            try:
                saved_id = int(save_result[0] or saved_id)
            except Exception:
                pass
        refreshed = None
        if kinozal_id:
            refreshed = db.find_item_by_kinozal_id(str(item.get("kinozal_id") or ""))
        if refreshed is None and saved_id:
            refreshed = db.get_item(saved_id)
        if refreshed is not None and enriched.get("tmdb_match_path"):
            refreshed["tmdb_match_path"] = enriched.get("tmdb_match_path")
        return before, refreshed, True
    except Exception:
        log.exception("Rematch failed for item_id=%s kinozal_id=%s", item.get("id"), item.get("kinozal_id"))
        return before, None, False
