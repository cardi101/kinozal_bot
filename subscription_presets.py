from datetime import datetime
from typing import Any, Dict, Optional

from utils import compact_spaces


def subscription_presets() -> Dict[str, Dict[str, Any]]:
    current_year = datetime.now().year
    year_from = current_year - 1
    base_exclude_keywords = "hdr,lossless,mp3,flac,fb2,epub,pdf,mobi"
    return {
        "world": {
            "name": "🌍 Новинки — мир",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 0,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords + ",ру,укр,украин",
                "content_filter": "exclude_anime_dorama",
                "country_codes": "",
                "exclude_country_codes": "TR,RU,UA,JP,KR,CN,TW,TH,HK,ID,MY,SG,PH,VN,LA,KH,MM,BD,PK,LK,NP,MN,KZ,UZ,KG,TJ,TM,AF,IR,IQ,SA,AE,QA,KW,OM,BH,YE,JO,LB,SY,IL,PS,BT,BN,MV",
            },
            "genre_ids": [],
        },
        "turkey": {
            "name": "🇹🇷 Новинки — Турция",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 1,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords,
                "content_filter": "any",
                "country_codes": "TR",
                "exclude_country_codes": "",
            },
            "genre_ids": [],
        },
        "dorama": {
            "name": "🌸 Новинки — дорамы",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 1,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords,
                "content_filter": "only_dorama",
                "country_codes": "",
                "exclude_country_codes": "",
            },
            "genre_ids": [],
        },
        "anime": {
            "name": "🍥 Новинки — аниме",
            "fields": {
                "media_type": "any",
                "year_from": year_from,
                "year_to": current_year,
                "allow_720": 1,
                "allow_1080": 1,
                "allow_2160": 1,
                "min_tmdb_rating": None,
                "include_keywords": "",
                "exclude_keywords": base_exclude_keywords,
                "content_filter": "only_anime",
                "country_codes": "",
                "exclude_country_codes": "",
            },
            "genre_ids": [],
        },
    }


def apply_subscription_preset(db: Any, sub_id: int, preset_key: str) -> Optional[Dict[str, Any]]:
    spec = subscription_presets().get(preset_key)
    if not spec:
        return None
    fields = dict(spec["fields"])
    fields["name"] = spec["name"]
    fields["preset_key"] = preset_key
    db.update_subscription(sub_id, **fields)
    db.set_subscription_genres(sub_id, spec.get("genre_ids", []))
    return db.get_subscription(sub_id)


PRESET_ROLLOUT_VERSION = "categories_v1"


def detect_subscription_preset_key(sub: Dict[str, Any]) -> str:
    preset_key = compact_spaces(str(sub.get("preset_key") or "")).lower()
    if preset_key in subscription_presets():
        return preset_key
    name_norm = compact_spaces(str(sub.get("name") or "")).casefold()
    if not name_norm:
        return ""
    for key, spec in subscription_presets().items():
        if compact_spaces(spec.get("name") or "").casefold() == name_norm:
            return key
    return ""
