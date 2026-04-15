from typing import Any, Dict, Iterable

from content_buckets import item_content_bucket
from country_helpers import parse_country_codes
from release_versioning import format_variant_summary
from subscription_matching import explain_subscription_match
from utils import compact_spaces


def _sub_payload(sub: Any) -> Dict[str, Any]:
    if hasattr(sub, "to_dict"):
        return sub.to_dict()
    return dict(sub)


def _item_snapshot(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": int(item.get("id") or 0),
        "kinozal_id": compact_spaces(str(item.get("kinozal_id") or "")),
        "source_uid": compact_spaces(str(item.get("source_uid") or "")),
        "source_link": compact_spaces(str(item.get("source_link") or "")),
        "source_title": compact_spaces(str(item.get("source_title") or "")),
        "media_type": compact_spaces(str(item.get("media_type") or "")),
        "version_signature": compact_spaces(str(item.get("version_signature") or "")),
        "variant_signature": compact_spaces(str(item.get("variant_signature") or "")),
        "source_episode_progress": compact_spaces(str(item.get("source_episode_progress") or "")),
        "source_format": compact_spaces(str(item.get("source_format") or "")),
        "source_audio_tracks": list(item.get("source_audio_tracks") or []),
        "tmdb_id": item.get("tmdb_id"),
        "tmdb_title": compact_spaces(str(item.get("tmdb_title") or item.get("tmdb_original_title") or "")),
        "imdb_id": compact_spaces(str(item.get("imdb_id") or "")),
    }


def build_delivery_audit(
    db: Any,
    item: Dict[str, Any],
    subs: Iterable[Any],
    context: str = "worker",
) -> Dict[str, Any]:
    sub_payloads = [_sub_payload(sub) for sub in subs]
    return {
        "context": compact_spaces(context) or "worker",
        "decision": "delivered",
        "bucket": item_content_bucket(item),
        "kinozal_id": compact_spaces(str(item.get("kinozal_id") or "")),
        "source_title": compact_spaces(str(item.get("source_title") or "")),
        "variant_summary": format_variant_summary(item),
        "source_category_name": compact_spaces(str(item.get("source_category_name") or "")),
        "media_type": compact_spaces(str(item.get("media_type") or "")),
        "source_episode_progress": compact_spaces(str(item.get("source_episode_progress") or "")),
        "tmdb_id": item.get("tmdb_id"),
        "tmdb_title": compact_spaces(str(item.get("tmdb_title") or item.get("tmdb_original_title") or "")),
        "tmdb_match_path": compact_spaces(str(item.get("tmdb_match_path") or "")),
        "tmdb_match_confidence": compact_spaces(str(item.get("tmdb_match_confidence") or "")),
        "tmdb_countries": parse_country_codes(item.get("tmdb_countries")),
        "tmdb_original_language": compact_spaces(str(item.get("tmdb_original_language") or "")),
        "manual_bucket": compact_spaces(str(item.get("manual_bucket") or "")),
        "previous_related_item_id": item.get("previous_related_item_id"),
        "previous_progress": compact_spaces(str(item.get("previous_progress") or "")),
        "anomaly_flags": list(item.get("anomaly_flags") or []),
        "item_snapshot": _item_snapshot(item),
        "matched_subscriptions": [
            {
                "id": int(sub.get("id") or 0),
                "name": compact_spaces(str(sub.get("name") or "")),
                "preset_key": compact_spaces(str(sub.get("preset_key") or "")),
                "content_filter": compact_spaces(str(sub.get("content_filter") or "")),
                "reason": explain_subscription_match(db, sub, item),
            }
            for sub in sub_payloads
        ],
    }
