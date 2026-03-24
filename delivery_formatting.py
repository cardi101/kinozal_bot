import html
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from country_helpers import human_country_names, parse_country_codes
from genres_helpers import item_genre_names
from item_years import item_display_year
from parsing_audio import (
    count_audio_variants,
    format_audio_variants,
    format_release_full_title,
    infer_release_type,
    parse_audio_tracks,
    parse_audio_variants,
    parse_release_text_episode_ranges,
)
from text_access import human_media_type
from utils import compact_spaces


def _annotate_audio_with_episode_ranges(
    variants: List[Dict[str, Any]], ranges: Dict[str, str]
) -> List[Dict[str, Any]]:
    """Annotates audio variant labels with episode ranges from release text.

    E.g. variant "ПМ (Дубляжная, HDrezka Studio, LostFilm)" with ranges
    {"дубляжная": "1-3", "lostfilm": "1-3"} becomes
    "ПМ (Дубляжная [1-3], HDrezka Studio, LostFilm [1-3])".
    """
    if not ranges:
        return variants
    result = []
    for variant in variants:
        label = str(variant.get("label") or "")
        m = re.search(r'\(([^)]+)\)', label)
        if m:
            parts = [compact_spaces(p) for p in m.group(1).split(",")]
            new_parts = []
            for p in parts:
                ep = ranges.get(p.lower())
                new_parts.append(f"{p} [{ep}]" if ep else p)
            new_inner = ", ".join(new_parts)
            new_label = label[:m.start(1)] + new_inner + label[m.end(1):]
            result.append({**variant, "label": new_label})
        else:
            result.append(variant)
    return result


def _preserve_multiline_overview(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    lines = []
    seen = set()

    for raw_line in raw.splitlines():
        line = " ".join(raw_line.split()).strip()
        if not line:
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        lines.append(line)

    return "\n".join(lines)


def item_message(db: Any, item: Dict[str, Any], matched_subs: Optional[Sequence[Dict[str, Any]]] = None) -> str:
    def human_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            return dt.strftime("%d.%m.%Y")
        except Exception:
            return str(value)

    def fmt_episode(prefix: str) -> Optional[str]:
        name = item.get(f"{prefix}_name")
        air_date = human_date(item.get(f"{prefix}_air_date"))
        season = item.get(f"{prefix}_season_number")
        episode = item.get(f"{prefix}_episode_number")
        parts = []
        if season and episode:
            parts.append(f"S{season}E{episode}")
        if air_date:
            parts.append(str(air_date))
        if name:
            parts.append(name)
        return " • ".join(str(x) for x in parts if x) or None

    source_title = item.get("source_title") or ""
    title = item.get("tmdb_title") or source_title or "Без названия"
    original = item.get("tmdb_original_title")
    media_type = item.get("media_type") or "movie"
    media = human_media_type(media_type)
    rating = item.get("tmdb_rating")
    votes = item.get("tmdb_vote_count")
    year = item_display_year(item)
    fmt = item.get("source_format")
    genres = item_genre_names(db, item)
    release_type = infer_release_type(source_title)
    audio_variants = parse_audio_variants(source_title)
    if not audio_variants:
        raw_audio_tracks = item.get("source_audio_tracks")
        if isinstance(raw_audio_tracks, str):
            try:
                raw_audio_tracks = json.loads(raw_audio_tracks)
            except Exception:
                raw_audio_tracks = parse_audio_tracks(source_title)
        raw_audio_tracks = raw_audio_tracks or parse_audio_tracks(source_title)
        audio_variants = [{"label": str(label), "count": 1} for label in raw_audio_tracks if str(label).strip()]
    release_full = format_release_full_title(source_title, item.get("tmdb_title"), item.get("tmdb_original_title"))
    countries = parse_country_codes(item.get("tmdb_countries"))
    country_names = human_country_names(countries, limit=4)

    lines = [f"🆕 <b>{html.escape(title)}</b>"]

    if original and original.lower() != title.lower():
        lines.append(f"<i>Ориг.: {html.escape(original)}</i>")

    meta = [media]
    if year:
        meta.append(str(year))
    if release_type:
        meta.append(release_type)
    if fmt:
        fmt_str = str(fmt)
        meta.append(f"{fmt_str}p" if fmt_str.isdigit() else fmt_str)
    if rating is not None and float(rating) > 0:
        if votes:
            meta.append(f"TMDB {float(rating):.1f} ({int(votes)})")
        else:
            meta.append(f"TMDB {float(rating):.1f}")
    if country_names:
        meta.append(", ".join(country_names))
    if meta:
        lines.append("🎬 " + " • ".join(meta))

    if release_full and compact_spaces(release_full).lower() != compact_spaces(title).lower():
        lines.append(f"📦 <b>Релиз:</b> {html.escape(release_full)}")

    episode_progress = item.get("source_episode_progress") or item.get("episode_progress") or item.get("source_series_update")
    if episode_progress:
        lines.append(f"📺 <b>В торренте:</b> {html.escape(str(episode_progress))}")

    if audio_variants:
        release_text = str(item.get("source_release_text") or "")
        ep_ranges = parse_release_text_episode_ranges(release_text)
        display_variants = _annotate_audio_with_episode_ranges(audio_variants, ep_ranges)
        lines.append(f"🎧 <b>Озвучки:</b> {count_audio_variants(audio_variants)} • {html.escape(format_audio_variants(display_variants))}")

    source_category_name = compact_spaces(str(item.get("source_category_name") or ""))
    if source_category_name:
        lines.append(f"🗂 <b>Категория API:</b> {html.escape(source_category_name)}")

    if genres:
        lines.append(f"🎭 <b>Жанры:</b> {html.escape(', '.join(genres[:6]))}")

    if item.get("tmdb_status"):
        lines.append(f"ℹ️ <b>Статус:</b> {html.escape(str(item['tmdb_status']))}")

    if media_type == "tv":
        seasons = item.get("tmdb_number_of_seasons")
        episodes = item.get("tmdb_number_of_episodes")
        if seasons or episodes:
            parts = []
            if seasons:
                parts.append(f"сезонов: {seasons}")
            if episodes:
                parts.append(f"эпизодов: {episodes}")
            lines.append("🧾 <b>TMDB:</b> " + ", ".join(parts))

        next_ep = fmt_episode("tmdb_next_episode")
        if next_ep:
            lines.append(f"🗓 <b>След. серия:</b> {html.escape(next_ep)}")

        last_ep = fmt_episode("tmdb_last_episode")
        if last_ep:
            lines.append(f"🕘 <b>Последняя серия:</b> {html.escape(last_ep)}")

    if item.get("tmdb_age_rating"):
        lines.append(f"🔞 <b>Возраст:</b> {html.escape(str(item['tmdb_age_rating']))}")

    if matched_subs:
        matched_names = [html.escape(str(sub.get("name") or "").strip()) for sub in matched_subs if str(sub.get("name") or "").strip()]
        matched_names = list(dict.fromkeys(matched_names))
        if matched_names:
            label = ", ".join(matched_names[:8])
            if len(matched_names) > 8:
                label += f" и ещё {len(matched_names) - 8}"
            lines.append(f"🔔 <b>Подошло под:</b> {label}")

    links = []
    if item.get("source_link"):
        links.append(f'<a href="{html.escape(item["source_link"], quote=True)}">Kinozal</a>')
    if item.get("tmdb_id"):
        tmdb_kind = "tv" if media_type == "tv" else "movie"
        links.append(f'<a href="https://www.themoviedb.org/{tmdb_kind}/{int(item["tmdb_id"])}">TMDB</a>')
    if item.get("imdb_id"):
        links.append(f'<a href="https://www.imdb.com/title/{html.escape(str(item["imdb_id"]), quote=True)}/">IMDb</a>')
    if links:
        lines.append("🔗 " + " • ".join(links))

    overview = item.get("tmdb_overview") or item.get("source_description")
    if overview:
        lines.append("")
        overview_text = _preserve_multiline_overview(overview)
        for part in overview_text.splitlines():
            lines.append(html.escape(part))

    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def grouped_items_message(db: Any, items: List[Dict[str, Any]], matched_subs: Optional[Sequence[Dict[str, Any]]] = None) -> str:
    if not items:
        return ""
    first = items[0]
    title = first.get("tmdb_title") or first.get("source_title") or "Без названия"
    original = first.get("tmdb_original_title")
    media_type = first.get("media_type") or "movie"
    media = human_media_type(media_type)
    rating = first.get("tmdb_rating")
    votes = first.get("tmdb_vote_count")
    year = item_display_year(first)

    lines = [f"🆕 <b>{html.escape(title)}</b>"]
    if original and original.lower() != title.lower():
        lines.append(f"<i>Ориг.: {html.escape(original)}</i>")

    meta = [media]
    if year:
        meta.append(str(year))
    if rating is not None and float(rating) > 0:
        if votes:
            meta.append(f"TMDB {float(rating):.1f} ({int(votes)})")
        else:
            meta.append(f"TMDB {float(rating):.1f}")
    lines.append("🎬 " + " • ".join(meta))

    lines.append(f"\n📦 <b>Вышло {len(items)} варианта:</b>")
    for item in items:
        source_title = item.get("source_title") or ""
        audio_variants = parse_audio_variants(source_title)
        fmt = item.get("source_format")
        release_type = infer_release_type(source_title)
        parts = []
        if audio_variants:
            parts.append(format_audio_variants(audio_variants))
        if fmt:
            fmt_str = str(fmt)
            parts.append(f"{fmt_str}p" if fmt_str.isdigit() else fmt_str)
        if release_type:
            parts.append(release_type)
        desc = " / ".join(parts) if parts else compact_spaces(source_title)
        link = item.get("source_link")
        if link:
            lines.append(f'  • <a href="{html.escape(link, quote=True)}">{html.escape(desc)}</a>')
        else:
            lines.append(f"  • {html.escape(desc)}")

    if matched_subs:
        matched_names = [html.escape(str(sub.get("name") or "").strip()) for sub in matched_subs if str(sub.get("name") or "").strip()]
        matched_names = list(dict.fromkeys(matched_names))
        if matched_names:
            label = ", ".join(matched_names[:8])
            if len(matched_names) > 8:
                label += f" и ещё {len(matched_names) - 8}"
            lines.append(f"\n🔔 <b>Подошло под:</b> {label}")

    links = []
    tmdb_id = first.get("tmdb_id")
    if tmdb_id:
        tmdb_kind = "tv" if media_type == "tv" else "movie"
        links.append(f'<a href="https://www.themoviedb.org/{tmdb_kind}/{int(tmdb_id)}">TMDB</a>')
    if first.get("imdb_id"):
        links.append(f'<a href="https://www.imdb.com/title/{html.escape(str(first["imdb_id"]), quote=True)}/">IMDb</a>')
    if links:
        lines.append("🔗 " + " • ".join(links))

    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
