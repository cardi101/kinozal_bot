import ast
import html
import re
from typing import Any, Dict, List

from country_helpers import human_content_filter, human_country_names
from genres_helpers import sub_genre_names
from text_access import human_media_type


def _raw_country_codes(value: Any) -> List[str]:
    if not value:
        return []

    if isinstance(value, (list, tuple, set)):
        result = []
        for item in value:
            code = str(item).strip().upper()
            if re.fullmatch(r"[A-Z]{2}", code) and code not in result:
                result.append(code)
        return result

    text = str(value).strip()
    if not text:
        return []

    parsed = None
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = ast.literal_eval(text)
        except Exception:
            parsed = None

    if isinstance(parsed, (list, tuple, set)):
        result = []
        for item in parsed:
            code = str(item).strip().upper()
            if re.fullmatch(r"[A-Z]{2}", code) and code not in result:
                result.append(code)
        return result

    result = []
    for code in re.findall(r"[A-Za-z]{2}", text):
        code = code.upper()
        if code not in result:
            result.append(code)
    return result


def _format_country_field(raw_value: Any, empty_label: str) -> str:
    raw_codes = _raw_country_codes(raw_value)
    names = human_country_names(raw_value, limit=12)

    if not raw_codes and not names:
        return empty_label

    if names and raw_codes:
        return f"{', '.join(names)} [raw: {', '.join(raw_codes)}]"
    if names:
        return ", ".join(names)
    return f"raw: {', '.join(raw_codes)}"


def sub_summary(db: Any, sub: Dict[str, Any]) -> str:
    genres = sub_genre_names(db, sub)

    raw_countries_value = sub.get("country_codes") or sub.get("country_codes_list")
    raw_exclude_countries_value = sub.get("exclude_country_codes") or sub.get("exclude_country_codes_list")

    countries = _format_country_field(raw_countries_value, "любые")
    exclude_countries = _format_country_field(raw_exclude_countries_value, "нет")

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
        f"Страны: {countries}\n"
        f"Искл. страны: {exclude_countries}\n"
        f"Ключи: {' '.join(keywords) if keywords else 'без фильтра'}"
    )
