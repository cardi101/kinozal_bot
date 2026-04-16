from typing import Any, Iterable, Sequence

from release_versioning import extract_kinozal_id
from utils import compact_spaces, sha1_text


def normalize_release_text_for_event(value: Any) -> str:
    lines = [
        " ".join(str(line or "").split()).strip()
        for line in str(value or "").splitlines()
        if " ".join(str(line or "").split()).strip()
    ]
    return "\n".join(lines).strip()


def delivery_event_identity(item: Any) -> str:
    if hasattr(item, "to_dict"):
        item = item.to_dict()
    payload = dict(item or {})
    kinozal_id = compact_spaces(str(payload.get("kinozal_id") or ""))
    if kinozal_id:
        return kinozal_id
    source_uid = compact_spaces(str(payload.get("source_uid") or ""))
    if source_uid:
        return source_uid
    source_link = compact_spaces(str(payload.get("source_link") or ""))
    extracted = extract_kinozal_id(source_link)
    if extracted:
        return extracted
    item_id = int(payload.get("id") or 0)
    return str(item_id) if item_id else "unknown"


def resolve_delivery_event_type(context: str = "", is_release_text_change: bool = False) -> str:
    normalized = compact_spaces(str(context or "")).lower()
    if is_release_text_change or normalized == "release_text_update":
        return "release_text"
    if normalized.startswith("grouped"):
        return "grouped"
    return "release"


def build_delivery_event_key(
    tg_user_id: int,
    item: Any,
    *,
    context: str = "",
    is_release_text_change: bool = False,
    release_text: str = "",
) -> str:
    if hasattr(item, "to_dict"):
        item = item.to_dict()
    payload = dict(item or {})
    event_type = resolve_delivery_event_type(context, is_release_text_change=is_release_text_change)
    identity = delivery_event_identity(payload)
    version_signature = compact_spaces(str(payload.get("version_signature") or ""))
    item_id = int(payload.get("id") or 0)
    if event_type == "release_text":
        normalized_release_text = normalize_release_text_for_event(release_text or payload.get("source_release_text") or "")
        release_hash = sha1_text(normalized_release_text or f"item:{item_id}")
        return f"release_text:{int(tg_user_id)}:{identity}:{release_hash}"
    if event_type == "grouped":
        return f"grouped:{int(tg_user_id)}:{identity}:{version_signature or item_id or 'noversion'}"
    return f"release:{int(tg_user_id)}:{identity}:{version_signature or item_id or 'noversion'}"


def build_grouped_event_key(
    tg_user_id: int,
    items: Sequence[Any],
    *,
    group_key: str = "",
) -> str:
    normalized_group_key = compact_spaces(str(group_key or ""))
    item_keys = []
    for item in items:
        if hasattr(item, "to_dict"):
            item = item.to_dict()
        payload = dict(item or {})
        item_keys.append(
            f"{delivery_event_identity(payload)}:{compact_spaces(str(payload.get('version_signature') or payload.get('id') or 'noversion'))}"
        )
    fingerprint = sha1_text("|".join(sorted(item_keys)) or "empty")
    return f"grouped:{int(tg_user_id)}:{normalized_group_key or fingerprint}:{fingerprint}"


def subscription_ids_fingerprint(subscription_ids: Iterable[Any]) -> str:
    normalized = sorted({int(value) for value in subscription_ids if str(value or "").strip()})
    return ",".join(str(value) for value in normalized)
