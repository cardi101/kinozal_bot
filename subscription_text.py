import html
from typing import Any, Dict

from country_helpers import human_content_filter, human_country_names
from genres_helpers import sub_genre_names
from text_access import human_media_type


def sub_summary(db: Any, sub: Dict[str, Any]) -> str:
    genres = sub_genre_names(db, sub)
    countries = human_country_names(sub.get("country_codes") or sub.get("country_codes_list"), limit=12)
    exclude_countries = human_country_names(sub.get("exclude_country_codes") or sub.get("exclude_country_codes_list"), limit=12)
    formats = []
    if sub.get("allow_720"):
        formats.append("720")
    if sub.get("allow_1080"):
        formats.append("1080")
    if sub.get("allow_2160"):
        formats.append("2160")
    years = "любой"
    if sub.get("year_from") or sub.get("year_to"):
        years = f"{sub.get('year_from') or '…'}–{sub.get('year_to') or '…'}"
    rating = f"{float(sub['min_tmdb_rating']):.1f}" if sub.get("min_tmdb_rating") is not None else "без фильтра"
    keywords = []
    if sub.get("include_keywords"):
        keywords.append("+" + sub["include_keywords"].replace(",", ", +"))
    if sub.get("exclude_keywords"):
        keywords.append("-" + sub["exclude_keywords"].replace(",", ", -"))
    return (
        f"#{sub['id']} {'🟢' if sub.get('is_enabled') else '⏸'} <b>{html.escape(sub['name'])}</b>\n"
        f"Тип: {human_media_type(sub.get('media_type'))}\n"
        f"Подтип: {human_content_filter(sub.get('content_filter') or 'any')}\n"
        f"Годы: {years}\n"
        f"Форматы: {', '.join(formats) if formats else 'любые'}\n"
        f"Рейтинг TMDB: {rating}\n"
        f"Жанры: {', '.join(genres) if genres else 'любые'}\n"
        f"Страны: {', '.join(countries) if countries else 'любые'}\n"
        f"Искл. страны: {', '.join(exclude_countries) if exclude_countries else 'нет'}\n"
        f"Ключи: {' '.join(keywords) if keywords else 'без фильтра'}"
    )
