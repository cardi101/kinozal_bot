import argparse
import asyncio
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _fmt_ts(value: int) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")


def _fetch_affected_users(dsn: str, kinozal_ids: List[str]) -> List[Dict[str, Any]]:
    ids = [str(value or "").strip() for value in kinozal_ids if str(value or "").strip()]
    if not ids:
        return []
    with connect(dsn, row_factory=dict_row, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT kinozal_id,
                       tg_user_id,
                       MAX(delivered_at) AS last_delivered_at
                FROM (
                    SELECT i.kinozal_id, d.tg_user_id, d.delivered_at
                    FROM deliveries d
                    JOIN items i ON i.id = d.item_id
                    WHERE i.kinozal_id = ANY(%s)

                    UNION ALL

                    SELECT da.kinozal_id, da.tg_user_id, da.delivered_at
                    FROM deliveries_archive da
                    WHERE da.kinozal_id = ANY(%s)
                ) delivery_rows
                GROUP BY kinozal_id, tg_user_id
                ORDER BY kinozal_id ASC, MAX(delivered_at) DESC, tg_user_id ASC
                """,
                (ids, ids),
            )
            return list(cur.fetchall())


async def _run() -> None:
    from aiogram import Bot

    from config import CFG
    from db import DB
    from release_audit import build_missing_progress_candidates
    from release_repair import group_users_by_kinozal, select_repair_candidates, summarize_repair_statuses
    from scripts.audit_missing_progress_versions import _fetch_rows
    from services.admin_api_service import AdminApiService

    load_dotenv()
    parser = argparse.ArgumentParser(description="Preview or replay latest missing-progress deliveries")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL", ""), help="Postgres DSN, defaults to DATABASE_URL")
    parser.add_argument("--kinozal-id", default="", help="Limit to one kinozal_id")
    parser.add_argument("--limit", type=int, default=20, help="Maximum number of kinozal rows to process")
    parser.add_argument("--include-historical", action="store_true", help="Include historical_gap rows")
    parser.add_argument("--include-zero-users", action="store_true", help="Include rows without previously delivered users")
    parser.add_argument("--apply", action="store_true", help="Send replay for ready targets")
    args = parser.parse_args()

    if not args.dsn:
        raise RuntimeError("DATABASE_URL is required")

    item_rows, observation_rows, delivery_rows = _fetch_rows(args.dsn, kinozal_id=args.kinozal_id.strip())
    candidates = build_missing_progress_candidates(item_rows, observation_rows, delivery_rows)
    candidates = select_repair_candidates(
        candidates,
        latest_gap_only=not args.include_historical,
        with_users_only=not args.include_zero_users,
    )
    candidates = candidates[: max(int(args.limit or 0), 0)]
    affected_users = _fetch_affected_users(args.dsn, [row["kinozal_id"] for row in candidates])
    users_by_kinozal = group_users_by_kinozal(affected_users)

    db = DB(args.dsn)
    bot = Bot(CFG.bot_token) if args.apply else None
    service = AdminApiService(db=db, tmdb_service=None, kinozal_service=None, bot=bot)
    results: List[Dict[str, Any]] = []

    try:
        for candidate in candidates:
            kinozal_id = str(candidate["kinozal_id"])
            finder = getattr(db, "find_item_any_by_kinozal_id", None) or db.find_item_by_kinozal_id
            current_item = finder(kinozal_id)
            if not current_item:
                results.append(
                    {
                        "kinozal_id": kinozal_id,
                        "tg_user_id": 0,
                        "status": "skipped",
                        "reason": "current_item_missing",
                    }
                )
                continue
            for user_row in users_by_kinozal.get(kinozal_id, []):
                tg_user_id = int(user_row["tg_user_id"])
                explanation = service.explain_delivery(kinozal_id, tg_user_id=tg_user_id)
                row: Dict[str, Any] = {
                    "kinozal_id": kinozal_id,
                    "tg_user_id": tg_user_id,
                    "status": str(explanation.get("status") or "unknown"),
                    "reason": ",".join(explanation.get("blockers") or []),
                    "gap_kind": candidate["gap_kind"],
                    "observed_progress": candidate["observed_progress"],
                    "current_highest_known": candidate["current_highest_known"],
                    "last_delivered_at": int(user_row.get("last_delivered_at") or 0),
                }
                if args.apply and row["status"] == "ready":
                    replay_result = await service.replay_delivery(kinozal_id, tg_user_id=tg_user_id, force=False)
                    row["status"] = str(replay_result.get("status") or row["status"])
                    row["reason"] = str(replay_result.get("reason") or row["reason"])
                results.append(row)
    finally:
        if bot is not None:
            await bot.session.close()
        db.close()

    print(
        " | ".join(
            [
                f"candidates={len(candidates)}",
                f"user_targets={len(results)}",
                f"mode={'apply' if args.apply else 'dry-run'}",
                f"status_counts={summarize_repair_statuses(results)}",
            ]
        )
    )
    for row in results:
        print(
            " | ".join(
                [
                    f"kinozal_id={row['kinozal_id']}",
                    f"tg_user_id={row['tg_user_id']}",
                    f"status={row['status']}",
                    f"reason={row['reason']}",
                    f"gap_kind={row.get('gap_kind', '')}",
                    f"observed={row.get('observed_progress', '')}",
                    f"current_known={row.get('current_highest_known', '')}",
                    f"last_delivered={_fmt_ts(int(row.get('last_delivered_at') or 0))}",
                ]
            )
        )


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
