import re
from typing import Any, Dict, List, Optional

from utils import compact_spaces


AUDIO_TAGS = [
    "ДБ", "ПМ", "ЛМ", "ЛД", "СТ", "РУ", "БП", "АП",
    "МВО", "ДВО", "AVO", "DVO", "MVO", "VO", "SUB", "Оригинал",
]


def parse_audio_variants(source_title: str) -> List[Dict[str, Any]]:
    title = compact_spaces(source_title or "")
    variants: List[Dict[str, Any]] = []
    positions: Dict[str, int] = {}
    pattern = (
        r'(?:(\d{1,2})\s*[xх×]\s*)?'
        r'(?<!\w)(' + "|".join(re.escape(tag) for tag in AUDIO_TAGS) + r')(?!\w)'
        r'(?:\s*\(([^)]{1,120})\))?'
    )

    for match in re.finditer(pattern, title, flags=re.I):
        count = int(match.group(1) or 1)
        kind_raw = compact_spaces(match.group(2) or "")
        kind = kind_raw if kind_raw.lower() == "оригинал" else kind_raw.upper()
        extra = compact_spaces(match.group(3) or "")
        label = f"{kind} ({extra})" if extra else kind
        key = label.lower()
        if key in positions:
            variants[positions[key]]["count"] += count
        else:
            positions[key] = len(variants)
            variants.append({"label": label, "count": count})

    return variants


def format_audio_variants(variants: List[Dict[str, Any]]) -> str:
    parts: List[str] = []
    for item in variants:
        label = compact_spaces(str(item.get("label") or ""))
        count = int(item.get("count") or 1)
        if not label:
            continue
        parts.append(f"{count}×{label}" if count > 1 else label)
    return ", ".join(parts)


def count_audio_variants(variants: List[Dict[str, Any]]) -> int:
    total = 0
    for item in variants:
        try:
            total += max(1, int(item.get("count") or 1))
        except Exception:
            total += 1
    return total


def parse_audio_tracks(source_title: str) -> List[str]:
    return [str(item.get("label")) for item in parse_audio_variants(source_title) if item.get("label")]


def infer_release_type(source_title: str) -> Optional[str]:
    source_title = (source_title or "").lower()
    for token, label in [
        ("blu-ray remux", "Blu-Ray Remux"),
        ("bluray remux", "Blu-Ray Remux"),
        ("blu-ray", "BluRay"),
        ("bluray", "BluRay"),
        ("web-dlrip", "WEB-DLRip"),
        ("web-dl", "WEB-DL"),
        ("webrip", "WEBRip"),
        ("bdrip", "BDRip"),
        ("dvdrip", "DVDRip"),
        ("hdtv", "HDTV"),
        ("remux", "Remux"),
    ]:
        if token in source_title:
            return label
    return None


def format_release_full_title(source_title: str, tmdb_title: Optional[str] = None, tmdb_original_title: Optional[str] = None) -> str:
    source_title = compact_spaces(source_title or "")
    if source_title:
        return source_title
    title = tmdb_title or "Без названия"
    original = tmdb_original_title or ""
    if original and original.lower() != title.lower():
        return f"{title} / {original}"
    return title
