import json
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from utils import compact_spaces, sha1_text


VERSION_RELEASE_TYPE_TOKENS: List[Tuple[str, str]] = [
    ("blu-ray remux", "blu-ray remux"),
    ("bluray remux", "blu-ray remux"),
    ("blu-ray", "bluray"),
    ("bluray", "bluray"),
    ("web-dlrip", "web-dlrip"),
    ("web-dl", "web-dl"),
    ("webrip", "webrip"),
    ("bdrip", "bdrip"),
    ("dvdrip", "dvdrip"),
    ("hdtv", "hdtv"),
    ("remux", "remux"),
]


def parse_episode_progress(text: str) -> Optional[str]:
    text = compact_spaces(text or "")
    patterns = [
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*сер(?:ия|ии|ий)\s*из\s*\d+)",
        r"(\d+\s*-\s*\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*выпуск(?:а|ов)?\s*из\s*\d+)",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*сезон:\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*сезон:\s*\d+\s*-\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*сезон:\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*-\s*\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*сер(?:ия|ии|ий))",
        r"(\d+\s*-\s*\d+\s*выпуск(?:а|ов)?)",
        r"(\d+\s*выпуск(?:а|ов)?)",
        r"(s\d{1,2}\s*e\d{1,3}\s*-\s*e\d{1,3})",
        r"(s\d{1,2}\s*e\d{1,3})",
        r"(\d+\s*-\s*\d+\s*из\s*\d+)",
        r"(\d+\s*из\s*\d+)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, flags=re.I)
        if m:
            return compact_spaces(m.group(1))
    return None


def normalize_episode_progress_signature(value: Any) -> str:
    text = compact_spaces(str(value or "")).lower()
    text = text.replace("ё", "е")
    return text


def extract_kinozal_id(value: Any) -> Optional[str]:
    text = compact_spaces(str(value or ""))
    if not text:
        return None
    if re.fullmatch(r"\d{5,12}", text):
        return text
    match = re.search(r"(?:details\.php\?id=|kinozal:)(\d+)", text, flags=re.I)
    if match:
        return match.group(1)
    return None


def resolve_item_kinozal_id(item: Optional[Dict[str, Any]]) -> Optional[str]:
    if not item:
        return None
    kinozal_id = compact_spaces(str(item.get("kinozal_id") or ""))
    if kinozal_id:
        return kinozal_id
    return extract_kinozal_id(item.get("source_uid")) or extract_kinozal_id(item.get("source_link"))


def build_source_uid(guid: Any, link: Any, source_title: str, cleaned_title: str) -> str:
    kinozal_id = extract_kinozal_id(link) or extract_kinozal_id(guid)
    if kinozal_id:
        return f"kinozal:{kinozal_id}"
    guid_text = compact_spaces(str(guid or ""))
    if guid_text:
        return guid_text
    link_text = compact_spaces(str(link or ""))
    if link_text:
        return link_text
    return sha1_text(f"{source_title}|{cleaned_title}")


def normalize_audio_tracks_signature(value: Any) -> str:
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            items = []
        else:
            try:
                loaded = json.loads(raw)
                items = loaded if isinstance(loaded, list) else [loaded]
            except Exception:
                items = [x.strip() for x in raw.split(",") if x.strip()]
    else:
        items = []

    normalized: List[str] = []
    seen = set()
    for item in items:
        label = compact_spaces(str(item or "")).lower()
        if not label or label in seen:
            continue
        seen.add(label)
        normalized.append(label)
    normalized.sort()
    return ",".join(normalized)


def version_release_type_signature(source_title: Any) -> str:
    title = compact_spaces(str(source_title or "")).lower()
    if not title:
        return ""
    for token, label in VERSION_RELEASE_TYPE_TOKENS:
        if token in title:
            return label
    return ""


def build_variant_signature(
    media_type: Any,
    source_title: Any,
    source_episode_progress: Any,
    source_format: Any,
    source_audio_tracks: Any,
) -> str:
    media = compact_spaces(str(media_type or "movie")).lower() or "movie"
    progress_sig = normalize_episode_progress_signature(source_episode_progress) or "noprogress"
    format_sig = compact_spaces(str(source_format or "")).lower() or "nofmt"
    release_sig = version_release_type_signature(source_title) or "norelease"
    audio_sig = normalize_audio_tracks_signature(source_audio_tracks) or "noaudio"
    if media == "tv":
        return sha1_text(f"tv|{progress_sig}|{format_sig}|{release_sig}|{audio_sig}")
    if media == "movie":
        return sha1_text(f"movie|{format_sig}|{release_sig}|{audio_sig}")
    return sha1_text(f"{media}|{progress_sig}|{format_sig}|{release_sig}|{audio_sig}")


def build_item_variant_signature(item: Dict[str, Any]) -> str:
    return build_variant_signature(
        media_type=item.get("media_type"),
        source_title=item.get("source_title"),
        source_episode_progress=item.get("source_episode_progress"),
        source_format=item.get("source_format"),
        source_audio_tracks=item.get("source_audio_tracks"),
    )


def get_variant_components(
    media_type: Any,
    source_title: Any,
    source_episode_progress: Any,
    source_format: Any,
    source_audio_tracks: Any,
) -> Dict[str, str]:
    media = compact_spaces(str(media_type or "movie")).lower() or "movie"
    return {
        "media": media,
        "progress": normalize_episode_progress_signature(source_episode_progress) or "noprogress",
        "format": compact_spaces(str(source_format or "")).lower() or "nofmt",
        "release": version_release_type_signature(source_title) or "norelease",
        "audio": normalize_audio_tracks_signature(source_audio_tracks) or "noaudio",
    }


def get_item_variant_components(item: Dict[str, Any]) -> Dict[str, str]:
    return get_variant_components(
        media_type=item.get("media_type"),
        source_title=item.get("source_title"),
        source_episode_progress=item.get("source_episode_progress"),
        source_format=item.get("source_format"),
        source_audio_tracks=item.get("source_audio_tracks"),
    )


def describe_variant_change(old_item: Optional[Dict[str, Any]], new_item: Dict[str, Any]) -> str:
    if not old_item:
        return "first version"
    old_parts = get_item_variant_components(old_item)
    new_parts = get_item_variant_components(new_item)
    changes: List[str] = []
    if old_parts.get("progress") != new_parts.get("progress"):
        changes.append(f"progress {old_parts.get('progress')} -> {new_parts.get('progress')}")
    if old_parts.get("format") != new_parts.get("format"):
        changes.append(f"format {old_parts.get('format')} -> {new_parts.get('format')}")
    if old_parts.get("release") != new_parts.get("release"):
        changes.append(f"release {old_parts.get('release')} -> {new_parts.get('release')}")
    if old_parts.get("audio") != new_parts.get("audio"):
        changes.append(f"audio {old_parts.get('audio')} -> {new_parts.get('audio')}")
    if not changes:
        return "metadata refresh"
    return "; ".join(changes)


def format_variant_summary(item: Dict[str, Any]) -> str:
    parts = get_item_variant_components(item)
    return (
        f"{parts.get('media')} | "
        f"progress={parts.get('progress')} | "
        f"format={parts.get('format')} | "
        f"release={parts.get('release')} | "
        f"audio={parts.get('audio')}"
    )


def build_version_signature(
    source_uid: str,
    media_type: str,
    source_title: str,
    source_episode_progress: Optional[str],
    source_format: Optional[str],
    source_audio_tracks: Optional[Sequence[str]],
) -> str:
    media = compact_spaces(str(media_type or "movie")).lower() or "movie"
    source_key = compact_spaces(str(source_uid or "")) or "unknown"
    variant_sig = build_variant_signature(
        media_type=media,
        source_title=source_title,
        source_episode_progress=source_episode_progress,
        source_format=source_format,
        source_audio_tracks=source_audio_tracks,
    )
    return sha1_text(f"{source_key}|{media}|{variant_sig}")
