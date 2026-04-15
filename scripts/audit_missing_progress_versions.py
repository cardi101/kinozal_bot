import argparse
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

from release_audit import build_missing_progress_candidates


def _fmt_ts(value: int) -> str:
    if not value:
        return ""
    return datetime.fromtimestamp(int(value)).strftime("%Y-%m-%d %H:%M:%S")


def _fetch_rows(dsn: str, kinozal_id: str = "") -> tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    kinozal_filter = ""
    params: List[Any] = []
    if kinozal_id:
        kinozal_filter = " AND kinozal_id = %s"
        params.append(kinozal_id)

    with connect(dsn, row_factory=dict_row, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT kinozal_id, source_title, source_episode_progress, created_at
                FROM items
                WHERE COALESCE(kinozal_id, '') <> ''{kinozal_filter}
                """,
                params,
            )
            item_rows = list(cur.fetchall())
            cur.execute(
                f"""
                SELECT kinozal_id, source_title, source_episode_progress, COALESCE(original_created_at, archived_at) AS created_at
                FROM items_archive
                WHERE COALESCE(kinozal_id, '') <> ''{kinozal_filter}
                """,
                params,
            )
            item_rows.extend(cur.fetchall())
            cur.execute(
                f"""
                SELECT kinozal_id, source_title, episode_progress, poll_ts
                FROM source_observations
                WHERE COALESCE(kinozal_id, '') <> ''
                  AND COALESCE(episode_progress, '') <> ''{kinozal_filter}
                """,
                params,
            )
            observation_rows = list(cur.fetchall())
            cur.execute(
                f"""
                SELECT i.kinozal_id,
                       COUNT(*) AS delivery_count,
                       COUNT(DISTINCT d.tg_user_id) AS delivery_users,
                       MAX(d.delivered_at) AS last_delivered_at
                FROM deliveries d
                JOIN items i ON i.id = d.item_id
                WHERE COALESCE(i.kinozal_id, '') <> ''{kinozal_filter.replace('kinozal_id', 'i.kinozal_id')}
                GROUP BY i.kinozal_id
                """,
                params,
            )
            delivery_rows = list(cur.fetchall())
    return item_rows, observation_rows, delivery_rows


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(description="Audit observed episode-progress gaps that never became item versions")
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL", ""), help="Postgres DSN, defaults to DATABASE_URL")
    parser.add_argument("--kinozal-id", default="", help="Limit report to one kinozal_id")
    parser.add_argument("--limit", type=int, default=25, help="Maximum number of rows to print")
    parser.add_argument("--with-users-only", action="store_true", help="Show only candidates with at least one delivered user")
    args = parser.parse_args()

    if not args.dsn:
        raise RuntimeError("DATABASE_URL is required")

    item_rows, observation_rows, delivery_rows = _fetch_rows(args.dsn, kinozal_id=args.kinozal_id.strip())
    candidates = build_missing_progress_candidates(item_rows, observation_rows, delivery_rows)
    if args.with_users_only:
        candidates = [row for row in candidates if int(row["delivery_users"]) > 0]

    print(f"total_candidates={len(candidates)}")
    for row in candidates[: max(args.limit, 0)]:
        print(
            " | ".join(
                [
                    f"kinozal_id={row['kinozal_id']}",
                    f"gap_kind={row['gap_kind']}",
                    f"users={row['delivery_users']}",
                    f"deliveries={row['delivery_count']}",
                    f"observed={row['observed_progress']}",
                    f"known_below={row['highest_known_below_observed']}",
                    f"current_known={row['current_highest_known']}",
                    f"latest_obs={_fmt_ts(row['poll_ts'])}",
                    f"last_delivered={_fmt_ts(row['last_delivered_at'])}",
                    f"title={row['title']}",
                ]
            )
        )


if __name__ == "__main__":
    main()
