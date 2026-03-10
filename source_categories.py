from typing import Any, Dict, List

from utils import compact_spaces


SOURCE_CATEGORY_MAP: Dict[str, str] = {
    "1": "Другое - Видеоклипы",
    "2": "Другое - АудиоКниги",
    "3": "Музыка - Буржуйская",
    "4": "Музыка - Русская",
    "5": "Музыка - Сборники",
    "6": "Кино - Боевик / Военный",
    "7": "Кино - Классика",
    "8": "Кино - Комедия",
    "9": "Кино - Исторический",
    "10": "Кино - Наше Кино",
    "11": "Кино - Приключения",
    "12": "Кино - Детский / Семейный",
    "13": "Кино - Фантастика",
    "14": "Кино - Фэнтези",
    "15": "Кино - Триллер / Детектив",
    "16": "Кино - Эротика",
    "17": "Кино - Драма",
    "18": "Кино - Документальный",
    "20": "Мульт - Аниме",
    "21": "Мульт - Буржуйский",
    "22": "Мульт - Русский",
    "23": "Другое - Игры",
    "24": "Кино - Ужас / Мистика",
    "32": "Другое - Программы",
    "35": "Кино - Мелодрама",
    "37": "Кино - Спорт",
    "38": "Кино - Театр, Опера, Балет",
    "39": "Кино - Индийское",
    "40": "Другое - Дизайн / Графика",
    "41": "Другое - Библиотека",
    "42": "Музыка - Классическая",
    "45": "Сериал - Русский",
    "46": "Сериал - Буржуйский",
    "47": "Кино - Азиатский",
    "48": "Кино - Концерт",
    "49": "Кино - Передачи / ТВ-шоу",
    "50": "Кино - ТВ-шоу Мир",
    "1001": "Все сериалы",
    "1002": "Все фильмы",
    "1003": "Все мульты",
    "1004": "Вся музыка",
    "1006": "Шоу, концерты, спорт",
}


SOURCE_CATEGORY_NAME_TO_ID = {compact_spaces(v).casefold(): k for k, v in SOURCE_CATEGORY_MAP.items()}


SOURCE_CATEGORY_NON_VIDEO_IDS = {"1", "2", "3", "4", "5", "23", "32", "40", "41", "42", "1004"}


SOURCE_CATEGORY_ANIME_IDS = {"20"}


SOURCE_CATEGORY_DORAMA_IDS = {"47"}


SOURCE_CATEGORY_TV_IDS = {"20", "45", "46", "49", "50", "1001"}


SOURCE_CATEGORY_MOVIE_IDS = {"6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "24", "35", "37", "38", "39", "47", "48", "1002"}


SOURCE_CATEGORY_RU_IDS = {"10", "45"}


def normalize_source_category_id(value: Any) -> str:
    text = compact_spaces(str(value or ""))
    if not text:
        return ""
    if text.isdigit() and text in SOURCE_CATEGORY_MAP:
        return text
    return SOURCE_CATEGORY_NAME_TO_ID.get(text.casefold(), "")


def resolve_source_category_name(category_id: Any, category_name: Any) -> str:
    normalized_id = normalize_source_category_id(category_id)
    if normalized_id:
        return SOURCE_CATEGORY_MAP.get(normalized_id, compact_spaces(str(category_name or "")))
    text = compact_spaces(str(category_name or ""))
    if not text:
        return ""
    normalized_from_name = normalize_source_category_id(text)
    if normalized_from_name:
        return SOURCE_CATEGORY_MAP.get(normalized_from_name, text)
    return text


def source_category_is_non_video(category_id: Any, category_name: Any = None) -> bool:
    normalized_id = normalize_source_category_id(category_id or category_name)
    if normalized_id:
        return normalized_id in SOURCE_CATEGORY_NON_VIDEO_IDS
    name = compact_spaces(str(category_name or "")).casefold()
    if not name:
        return False
    return any(token in name for token in ("музыка", "аудиокниг", "игр", "программ", "библиотек", "дизайн", "график"))


def source_category_forced_media_type(category_id: Any, category_name: Any = None) -> str:
    normalized_id = normalize_source_category_id(category_id or category_name)
    if normalized_id in SOURCE_CATEGORY_TV_IDS:
        return "tv"
    if normalized_id in SOURCE_CATEGORY_MOVIE_IDS:
        return "movie"
    if normalized_id in SOURCE_CATEGORY_NON_VIDEO_IDS:
        return "other"
    name = compact_spaces(str(category_name or "")).casefold()
    if not name:
        return ""
    if "сериал" in name:
        return "tv"
    if any(token in name for token in ("кино", "мульт", "фильм")):
        return "movie"
    return ""


def source_category_bucket_hint(category_id: Any, category_name: Any = None) -> str:
    normalized_id = normalize_source_category_id(category_id or category_name)
    if normalized_id in SOURCE_CATEGORY_ANIME_IDS:
        return "anime"
    if normalized_id in SOURCE_CATEGORY_DORAMA_IDS:
        return "dorama"
    name = compact_spaces(str(category_name or "")).casefold()
    if "аниме" in name:
        return "anime"
    if "азиат" in name:
        return "dorama"
    return ""


def source_category_fallback_country_codes(item: Dict[str, Any]) -> List[str]:
    normalized_id = normalize_source_category_id(item.get("source_category_id") or item.get("source_category_name"))
    if normalized_id in SOURCE_CATEGORY_RU_IDS:
        return ["RU"]
    return []
