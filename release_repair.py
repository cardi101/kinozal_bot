from collections import defaultdict
from typing import Any, Dict, Iterable, List


def select_repair_candidates(
    candidates: Iterable[Dict[str, Any]],
    latest_gap_only: bool = True,
    with_users_only: bool = True,
) -> List[Dict[str, Any]]:
    rows = [dict(row) for row in candidates]
    if latest_gap_only:
        rows = [row for row in rows if str(row.get("gap_kind") or "") == "latest_gap"]
    if with_users_only:
        rows = [row for row in rows if int(row.get("delivery_users") or 0) > 0]
    deduped: List[Dict[str, Any]] = []
    seen_kinozal_ids: set[str] = set()
    for row in rows:
        kinozal_id = str(row.get("kinozal_id") or "").strip()
        if not kinozal_id or kinozal_id in seen_kinozal_ids:
            continue
        seen_kinozal_ids.add(kinozal_id)
        deduped.append(row)
    return deduped


def group_users_by_kinozal(rows: Iterable[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        kinozal_id = str(row.get("kinozal_id") or "").strip()
        if not kinozal_id:
            continue
        grouped[kinozal_id].append(dict(row))
    for users in grouped.values():
        users.sort(
            key=lambda row: (
                -int(row.get("last_delivered_at") or 0),
                int(row.get("tg_user_id") or 0),
            )
        )
    return dict(grouped)


def summarize_repair_statuses(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    summary: Dict[str, int] = defaultdict(int)
    for row in rows:
        status = str(row.get("status") or "unknown")
        summary[status] += 1
    return dict(sorted(summary.items()))
