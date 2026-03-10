import json
import re
from typing import Any, Dict, List, Optional

try:
    import pycountry
except ImportError:
    pycountry = None

from source_categories import source_category_fallback_country_codes
from utils import compact_spaces


COUNTRY_NAMES_RU: Dict[str, str] = {
    "AE": "ОАЭ",
    "AR": "Аргентина",
    "AT": "Австрия",
    "AU": "Австралия",
    "BE": "Бельгия",
    "BG": "Болгария",
    "BR": "Бразилия",
    "CA": "Канада",
    "CH": "Швейцария",
    "CL": "Чили",
    "CN": "Китай",
    "CO": "Колумбия",
    "CZ": "Чехия",
    "DE": "Германия",
    "DK": "Дания",
    "EG": "Египет",
    "ES": "Испания",
    "FI": "Финляндия",
    "FR": "Франция",
    "GB": "Великобритания",
    "GR": "Греция",
    "HK": "Гонконг",
    "HR": "Хорватия",
    "HU": "Венгрия",
    "ID": "Индонезия",
    "IE": "Ирландия",
    "IL": "Израиль",
    "IN": "Индия",
    "IR": "Иран",
    "IS": "Исландия",
    "IT": "Италия",
    "JP": "Япония",
    "KR": "Южная Корея",
    "KZ": "Казахстан",
    "LT": "Литва",
    "LV": "Латвия",
    "MX": "Мексика",
    "MY": "Малайзия",
    "NL": "Нидерланды",
    "NO": "Норвегия",
    "NZ": "Новая Зеландия",
    "PH": "Филиппины",
    "PL": "Польша",
    "PT": "Португалия",
    "RO": "Румыния",
    "RS": "Сербия",
    "RU": "Россия",
    "SE": "Швеция",
    "SG": "Сингапур",
    "TH": "Таиланд",
    "TR": "Турция",
    "TW": "Тайвань",
    "UA": "Украина",
    "US": "США",
    "VN": "Вьетнам",
    "ZA": "ЮАР",
}


ANIME_COUNTRY_CODES = {"JP", "CN", "TW", "HK", "KR"}


DORAMA_COUNTRY_CODES = {
    "BN", "CN", "HK", "ID", "JP", "KH", "KR", "LA", "MM", "MO", "MY", "PH", "SG", "TH", "TW", "VN",
}


DORAMA_LANGUAGE_CODES = {"cn", "id", "ja", "ko", "ms", "th", "tl", "vi", "zh"}


ASIAN_SCRIPT_RE = re.compile(r"[぀-ヿ㐀-䶿一-鿿豈-﫿가-힯฀-๿]")


def parse_jsonish_list(value: Any) -> List[str]:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            loaded = json.loads(raw)
            items = loaded if isinstance(loaded, list) else [loaded]
        except Exception:
            items = [x.strip() for x in raw.split(",") if x.strip()]
    else:
        return []

    result: List[str] = []
    seen = set()
    for item in items:
        value_str = compact_spaces(str(item or "")).strip()
        if not value_str:
            continue
        value_key = value_str.lower()
        if value_key in seen:
            continue
        seen.add(value_key)
        result.append(value_str)
    return result


def parse_country_codes(value: Any) -> List[str]:
    result: List[str] = []
    seen = set()
    for code in parse_jsonish_list(value):
        normalized = compact_spaces(str(code)).upper()
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def country_name_ru(code: str) -> str:
    normalized = compact_spaces(str(code or "")).upper()
    if not normalized:
        return ""
    if normalized in COUNTRY_NAMES_RU:
        return COUNTRY_NAMES_RU[normalized]
    if pycountry is not None:
        try:
            country = pycountry.countries.get(alpha_2=normalized)
            if country and getattr(country, "name", None):
                return str(country.name)
        except Exception:
            pass
    return normalized


def human_country_names(value: Any, limit: Optional[int] = None) -> List[str]:
    names = [country_name_ru(code) for code in parse_country_codes(value)]
    return names[:limit] if limit is not None else names


def effective_item_countries(item: Dict[str, Any]) -> List[str]:
    manual = parse_country_codes(item.get("manual_country_codes"))
    if manual:
        return manual
    tmdb_countries = parse_country_codes(item.get("tmdb_countries"))
    if tmdb_countries:
        return tmdb_countries
    return parse_country_codes(source_category_fallback_country_codes(item))


def normalize_tmdb_language(value: Any) -> str:
    lang = compact_spaces(str(value or "")).lower()
    lang = lang.replace("_", "-")
    if not lang:
        return ""
    if lang in {"jp", "kr"}:
        return {"jp": "ja", "kr": "ko"}[lang]
    primary = lang.split("-", 1)[0]
    if primary in {"jp", "kr"}:
        return {"jp": "ja", "kr": "ko"}[primary]
    return primary


def has_asian_script(text: Any) -> bool:
    return bool(ASIAN_SCRIPT_RE.search(compact_spaces(str(text or ""))))


def asian_dorama_signal_score(item: Dict[str, Any]) -> int:
    score = 0
    countries = set(effective_item_countries(item))
    if countries & DORAMA_COUNTRY_CODES:
        score += 2

    lang = normalize_tmdb_language(item.get("tmdb_original_language"))
    if lang in DORAMA_LANGUAGE_CODES:
        score += 2

    original_title = item.get("tmdb_original_title") or ""
    tmdb_title = item.get("tmdb_title") or ""
    source_title = item.get("source_title") or ""
    if has_asian_script(original_title):
        score += 2
    elif has_asian_script(tmdb_title):
        score += 1
    elif has_asian_script(source_title):
        score += 1

    return score


def human_content_filter(value: str) -> str:
    mapping = {
        "any": "любое",
        "only_anime": "только аниме",
        "only_dorama": "только дорамы",
        "exclude_anime": "без аниме",
        "exclude_dorama": "без дорам",
        "exclude_anime_dorama": "без аниме и дорам",
    }
    return mapping.get(str(value or "any"), str(value or "any"))
