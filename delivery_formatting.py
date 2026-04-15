import html
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from content_buckets import item_content_bucket
from country_helpers import human_country_names, parse_country_codes
from magnet_links import build_public_magnet_redirect_url
from genres_helpers import item_genre_names
from item_years import item_display_year
from parsing_audio import (
    format_audio_variants,
    parse_audio_tracks,
    parse_audio_variants,
)
from utils import compact_spaces


_KINOZAL_ID_RE = re.compile(r"[?&]id=(\d+)")


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


def _message_event_label(item: Dict[str, Any], old_release_text: str = "") -> str:
    if compact_spaces(old_release_text):
        return "🟢 UPDATE"
    previous_progress = compact_spaces(str(item.get("previous_progress") or ""))
    current_progress = compact_spaces(
        str(item.get("source_episode_progress") or item.get("episode_progress") or item.get("source_series_update") or "")
    )
    if previous_progress and current_progress and previous_progress != current_progress:
        return "🟢 UPDATE"
    if item.get("previous_related_item_id"):
        return "🟢 UPDATE"
    return "🆕 NEW"


def _message_media_badge(item: Dict[str, Any]) -> str:
    media_type = compact_spaces(str(item.get("media_type") or "")).lower()
    return {
        "tv": "TV",
        "movie": "MOVIE",
        "other": "OTHER",
    }.get(media_type, (media_type or "ITEM").upper())


def _message_route_label(item: Dict[str, Any], matched_subs: Optional[Sequence[Dict[str, Any]]] = None) -> str:
    if matched_subs:
        names = [compact_spaces(str(sub.get("name") or "")) for sub in matched_subs if compact_spaces(str(sub.get("name") or ""))]
        names = list(dict.fromkeys(names))
        if names:
            label = " | ".join(names[:2])
            if len(names) > 2:
                label += f" +{len(names) - 2}"
            return label

    bucket = item_content_bucket(item)
    if bucket == "anime":
        return "🎌 Аниме"
    if bucket == "dorama":
        return "🎎 Дорамы"
    countries = set(parse_country_codes(item.get("tmdb_countries")))
    if "TR" in countries:
        return "🇹🇷 Турция"
    return "🌍 Мир"


def _compact_audio_summary(audio_variants: List[Dict[str, Any]]) -> str:
    labels = [compact_spaces(str(variant.get("label") or "")) for variant in audio_variants]
    labels = [label for label in labels if label]
    compact_variants: List[Dict[str, Any]] = []
    for variant in audio_variants[:2]:
        label = compact_spaces(str(variant.get("label") or ""))
        if not label:
            continue
        compact_variants.append({"label": label, "count": int(variant.get("count") or 1)})
    if not compact_variants:
        return ""
    summary = " + ".join(
        format_audio_variants([variant]).strip()
        for variant in compact_variants
    )
    if len(labels) > 2:
        summary += f" +{len(labels) - 2}"
    return summary


def _quality_badge(item: Dict[str, Any]) -> str:
    fmt = compact_spaces(str(item.get("source_format") or ""))
    if not fmt:
        title = compact_spaces(str(item.get("source_title") or ""))
        match = re.search(r"(?<!\d)(720|1080|2160)p\b", title, flags=re.I)
        if match:
            fmt = match.group(1)
    if not fmt:
        return ""
    return f"{fmt}p" if fmt.isdigit() else fmt


def _audio_variant_counts(variants: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for variant in variants:
        label = compact_spaces(str(variant.get("label") or ""))
        if not label:
            continue
        counts[label.lower()] = counts.get(label.lower(), 0) + max(1, int(variant.get("count") or 1))
    return counts


def _audio_change_label(label: str) -> str:
    upper = compact_spaces(label).upper()
    if upper.startswith(("ПМ", "MVO", "МВО")):
        return "многоголосая дорожка"
    if upper.startswith(("ЛД", "ДВО", "DVO")):
        return "двухголосая дорожка"
    if upper.startswith(("ДБ", "AVO", "РУ")):
        return "новая аудиодорожка"
    if upper.startswith(("СТ", "SUB")):
        return "дорожка субтитров"
    return "новая аудиодорожка"


def _describe_audio_change(previous_title: str, current_title: str) -> str:
    previous_variants = parse_audio_variants(previous_title)
    current_variants = parse_audio_variants(current_title)
    if not current_variants:
        return ""

    previous_counts = _audio_variant_counts(previous_variants)
    current_counts = _audio_variant_counts(current_variants)
    if previous_counts == current_counts:
        return ""

    added_total = 0
    removed_total = 0
    added_labels: List[str] = []
    for label, count in current_counts.items():
        delta = count - previous_counts.get(label, 0)
        if delta > 0:
            added_total += delta
            added_labels.extend([label] * delta)
    for label, count in previous_counts.items():
        delta = count - current_counts.get(label, 0)
        if delta > 0:
            removed_total += delta

    if added_total > 0 and removed_total == 0:
        if added_total == 1 and added_labels:
            return f"добавлена {_audio_change_label(added_labels[0])}"
        if added_total > 1:
            return f"добавлено {added_total} новых аудиодорожки"
        return "добавлена новая аудиодорожка"
    return "обновился набор озвучек"


def _describe_update_change(item: Dict[str, Any], old_release_text: str = "") -> str:
    changes: List[str] = []
    previous_progress = compact_spaces(str(item.get("previous_progress") or ""))
    current_progress = compact_spaces(
        str(item.get("source_episode_progress") or item.get("episode_progress") or item.get("source_series_update") or "")
    )
    if previous_progress and current_progress and previous_progress != current_progress:
        changes.append(f"{previous_progress} → {current_progress}")

    previous_source_title = compact_spaces(str(item.get("previous_source_title") or ""))
    current_source_title = compact_spaces(str(item.get("source_title") or ""))
    if previous_source_title and current_source_title:
        audio_change = _describe_audio_change(previous_source_title, current_source_title)
        if audio_change:
            changes.append(audio_change)

    previous_format = compact_spaces(str(item.get("previous_source_format") or ""))
    current_format = compact_spaces(str(item.get("source_format") or ""))
    if previous_format and current_format and previous_format != current_format:
        old_fmt = f"{previous_format}p" if previous_format.isdigit() else previous_format
        new_fmt = f"{current_format}p" if current_format.isdigit() else current_format
        changes.append(f"качество {old_fmt} → {new_fmt}")

    if not changes and compact_spaces(old_release_text):
        return "обновились детали релиза"
    if not changes and item.get("previous_related_item_id"):
        return "обновились данные релиза"
    return "; ".join(changes)


def _should_show_tmdb_line(rating: Any, votes: Any) -> bool:
    if rating is None:
        return False
    try:
        if float(rating) <= 0:
            return False
    except Exception:
        return False
    try:
        return int(votes or 0) >= 20
    except Exception:
        return False


def _display_kinozal_id(item: Dict[str, Any]) -> str:
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
    if kinozal_id:
        return kinozal_id
    source_link = compact_spaces(str(item.get("source_link") or ""))
    if not source_link:
        return ""
    match = _KINOZAL_ID_RE.search(source_link)
    return match.group(1) if match else ""


def item_message(
    db: Any,
    item: Dict[str, Any],
    matched_subs: Optional[Sequence[Dict[str, Any]]] = None,
    old_release_text: str = "",
) -> str:
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
    rating = item.get("tmdb_rating")
    votes = item.get("tmdb_vote_count")
    year = item_display_year(item)
    fmt = item.get("source_format")
    genres = item_genre_names(db, item)
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
    countries = parse_country_codes(item.get("tmdb_countries"))
    country_names = human_country_names(countries, limit=4)
    route_label = _message_route_label(item, matched_subs)
    event_label = _message_event_label(item, old_release_text=old_release_text)
    episode_progress = compact_spaces(
        str(item.get("source_episode_progress") or item.get("episode_progress") or item.get("source_series_update") or "")
    )
    audio_summary = _compact_audio_summary(audio_variants)
    kinozal_id = _display_kinozal_id(item)
    quality_badge = _quality_badge(item)
    is_update = event_label == "🟢 UPDATE"
    change_text = _describe_update_change(item, old_release_text=old_release_text) if is_update else ""

    header_parts = [event_label, _message_media_badge(item)]
    if quality_badge:
        header_parts.append(quality_badge)

    lines = [" • ".join(html.escape(part) for part in header_parts if part)]

    title_line = html.escape(title)
    if original and original.lower() != title.lower():
        title_line += f" / {html.escape(original)}"
    if year:
        title_line += f" ({html.escape(str(year))})"
    lines.append(f"<b>{title_line}</b>")

    route_parts = [route_label]
    if kinozal_id:
        route_parts.append(f"Kinozal {kinozal_id}")
    lines.append(html.escape(" • ".join(str(part) for part in route_parts if part)))

    if change_text:
        lines.append(f"Изменение: {html.escape(change_text)}")

    if media_type == "tv" and episode_progress and not change_text.startswith(episode_progress):
        lines.append(f"Серии: {html.escape(episode_progress)}")

    if audio_summary:
        lines.append(f"Озвучка: {html.escape(audio_summary)}")

    if genres:
        lines.append(f"Жанры: {html.escape(', '.join(genres[:4]))}")
    if country_names:
        lines.append(f"Страны: {html.escape(', '.join(country_names[:3]))}")

    if _should_show_tmdb_line(rating, votes):
        lines.append(f"TMDB: <code>{float(rating):.1f}</code> ({int(votes)})")

    if media_type == "tv":
        next_ep = fmt_episode("tmdb_next_episode")
        if next_ep:
            lines.append(f"Следующая серия: {html.escape(next_ep)}")

    links = []
    if item.get("source_link"):
        links.append(f'<a href="{html.escape(item["source_link"], quote=True)}">Kinozal</a>')
    if item.get("tmdb_id"):
        tmdb_kind = "tv" if media_type == "tv" else "movie"
        links.append(f'<a href="https://www.themoviedb.org/{tmdb_kind}/{int(item["tmdb_id"])}">TMDB</a>')
    if item.get("mal_id"):
        links.append(f'<a href="https://myanimelist.net/anime/{html.escape(str(item["mal_id"]), quote=True)}">MAL</a>')
    if item.get("imdb_id"):
        links.append(f'<a href="https://www.imdb.com/title/{html.escape(str(item["imdb_id"]), quote=True)}/">IMDb</a>')
    if links:
        lines.append("Ссылки: " + " • ".join(links))

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
    rating = first.get("tmdb_rating")
    votes = first.get("tmdb_vote_count")
    year = item_display_year(first)
    route_label = _message_route_label(first, matched_subs)
    kinozal_id = _display_kinozal_id(first)

    header_parts = ["📦 MULTI", _message_media_badge(first), f"{len(items)} variants"]
    lines = [" • ".join(header_parts)]
    title_line = html.escape(title)
    if original and original.lower() != title.lower():
        title_line += f" / {html.escape(original)}"
    if year:
        title_line += f" ({html.escape(str(year))})"
    lines.append(f"<b>{title_line}</b>")

    route_parts = [route_label]
    if kinozal_id:
        route_parts.append(f"Kinozal {kinozal_id}")
    lines.append(html.escape(" • ".join(str(part) for part in route_parts if part)))

    for item in items:
        source_title = item.get("source_title") or ""
        audio_variants = parse_audio_variants(source_title)
        fmt = item.get("source_format")
        parts = []
        progress = compact_spaces(str(item.get("source_episode_progress") or item.get("episode_progress") or item.get("source_series_update") or ""))
        if progress:
            parts.append(progress)
        if audio_variants:
            parts.append(_compact_audio_summary(audio_variants))
        if fmt:
            fmt_str = str(fmt)
            parts.append(f"{fmt_str}p" if fmt_str.isdigit() else fmt_str)
        desc = " • ".join(parts) if parts else compact_spaces(source_title)
        link = item.get("source_link")
        magnet_url = build_public_magnet_redirect_url(item)
        bullet_parts = []
        if link:
            bullet_parts.append(f'<a href="{html.escape(link, quote=True)}">{html.escape(desc)}</a>')
        else:
            bullet_parts.append(html.escape(desc))
        if magnet_url:
            bullet_parts.append(f'<a href="{html.escape(magnet_url, quote=True)}">🧲</a>')
        lines.append("  • " + " • ".join(bullet_parts))

    links = []
    tmdb_id = first.get("tmdb_id")
    if tmdb_id:
        tmdb_kind = "tv" if media_type == "tv" else "movie"
        links.append(f'<a href="https://www.themoviedb.org/{tmdb_kind}/{int(tmdb_id)}">TMDB</a>')
    if first.get("mal_id"):
        links.append(f'<a href="https://myanimelist.net/anime/{html.escape(str(first["mal_id"]), quote=True)}">MAL</a>')
    if first.get("imdb_id"):
        links.append(f'<a href="https://www.imdb.com/title/{html.escape(str(first["imdb_id"]), quote=True)}/">IMDb</a>')
    if links:
        lines.append("Ссылки: " + " • ".join(links))

    if _should_show_tmdb_line(rating, votes):
        lines.append(f"TMDB: <code>{float(rating):.1f}</code> ({int(votes)})")

    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
