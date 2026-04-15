from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional

from release_versioning import compare_episode_progress


def _normalize_progress(value: Any) -> str:
    return str(value or "").strip()


def _pick_highest_progress(progresses: Iterable[str]) -> str:
    best = ""
    for progress in progresses:
        if not progress:
            continue
        if not best:
            best = progress
            continue
        comparison = compare_episode_progress(progress, best)
        if comparison == 1:
            best = progress
    return best


def classify_missing_progress_gap(observed_progress: str, current_highest_known: str) -> str:
    if not observed_progress or not current_highest_known:
        return "unknown_gap"
    if compare_episode_progress(current_highest_known, observed_progress) == 1:
        return "historical_gap"
    if compare_episode_progress(observed_progress, current_highest_known) == 1:
        return "latest_gap"
    return "unknown_gap"


def build_missing_progress_candidates(
    item_rows: Iterable[Dict[str, Any]],
    observation_rows: Iterable[Dict[str, Any]],
    delivery_rows: Iterable[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    known_by_kinozal: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    observed_by_kinozal: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)
    deliveries_by_kinozal: Dict[str, Dict[str, Any]] = {}

    for row in item_rows:
        kinozal_id = str(row.get("kinozal_id") or "").strip()
        progress = _normalize_progress(row.get("source_episode_progress"))
        if not kinozal_id or not progress:
            continue
        created_at = int(row.get("created_at") or 0)
        current = known_by_kinozal[kinozal_id].get(progress)
        if current is None or created_at > int(current.get("created_at") or 0):
            known_by_kinozal[kinozal_id][progress] = {
                "title": str(row.get("source_title") or ""),
                "created_at": created_at,
            }

    for row in observation_rows:
        kinozal_id = str(row.get("kinozal_id") or "").strip()
        progress = _normalize_progress(row.get("episode_progress"))
        if not kinozal_id or not progress:
            continue
        poll_ts = int(row.get("poll_ts") or 0)
        current = observed_by_kinozal[kinozal_id].get(progress)
        if current is None or poll_ts > int(current.get("poll_ts") or 0):
            observed_by_kinozal[kinozal_id][progress] = {
                "title": str(row.get("source_title") or ""),
                "poll_ts": poll_ts,
            }

    for row in delivery_rows:
        kinozal_id = str(row.get("kinozal_id") or "").strip()
        if not kinozal_id:
            continue
        deliveries_by_kinozal[kinozal_id] = {
            "delivery_count": int(row.get("delivery_count") or 0),
            "delivery_users": int(row.get("delivery_users") or 0),
            "last_delivered_at": int(row.get("last_delivered_at") or 0),
        }

    candidates: List[Dict[str, Any]] = []
    for kinozal_id, observed_progresses in observed_by_kinozal.items():
        known_progresses = known_by_kinozal.get(kinozal_id, {})
        if not known_progresses:
            continue
        current_highest_known = _pick_highest_progress(known_progresses.keys())
        for observed_progress, observed_meta in observed_progresses.items():
            if observed_progress in known_progresses:
                continue

            lower_known: List[str] = []
            for known_progress in known_progresses:
                if compare_episode_progress(observed_progress, known_progress) == 1:
                    lower_known.append(known_progress)
            if not lower_known:
                continue

            delivery_meta = deliveries_by_kinozal.get(
                kinozal_id,
                {"delivery_count": 0, "delivery_users": 0, "last_delivered_at": 0},
            )
            highest_known_below = _pick_highest_progress(lower_known)
            candidates.append(
                {
                    "kinozal_id": kinozal_id,
                    "gap_kind": classify_missing_progress_gap(observed_progress, current_highest_known),
                    "observed_progress": observed_progress,
                    "highest_known_below_observed": highest_known_below,
                    "current_highest_known": current_highest_known,
                    "poll_ts": int(observed_meta.get("poll_ts") or 0),
                    "title": str(observed_meta.get("title") or ""),
                    "delivery_count": int(delivery_meta["delivery_count"]),
                    "delivery_users": int(delivery_meta["delivery_users"]),
                    "last_delivered_at": int(delivery_meta["last_delivered_at"]),
                }
            )

    candidates.sort(
        key=lambda row: (
            row["gap_kind"] != "latest_gap",
            -row["delivery_users"],
            -row["delivery_count"],
            -row["poll_ts"],
            row["kinozal_id"],
        )
    )
    return candidates
