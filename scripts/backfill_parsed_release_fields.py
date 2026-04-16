import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import dict_row

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parsed_release import parse_release_title  # noqa: E402
from release_versioning import refresh_item_version_fields  # noqa: E402
from utils import compact_spaces  # noqa: E402


def _fetch_rows(dsn: str, kinozal_id: str = "", limit: int = 0) -> List[Dict[str, Any]]:
    filters = ["COALESCE(source_title, '') <> ''"]
    params: List[Any] = []
    if kinozal_id:
        filters.append("kinozal_id = %s")
        params.append(kinozal_id)
    limit_sql = ""
    if limit > 0:
        limit_sql = " LIMIT %s"
        params.append(int(limit))

    with connect(dsn, row_factory=dict_row, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT
                    id,
                    kinozal_id,
                    source_uid,
                    source_title,
                    media_type,
                    source_episode_progress,
                    source_format,
                    source_audio_tracks,
                    version_signature,
                    parsed_release_json
                FROM items
                WHERE {' AND '.join(filters)}
                ORDER BY id DESC{limit_sql}
                """,
                params,
            )
            return list(cur.fetchall())


def _build_recomputed_payload(row: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
    payload = {
        "id": int(row["id"]),
        "source_uid": row.get("source_uid") or "",
        "media_type": row.get("media_type") or "movie",
        "source_title": row.get("source_title") or "",
        "source_episode_progress": row.get("source_episode_progress") or "",
        "source_format": row.get("source_format") or "",
        "source_audio_tracks": row.get("source_audio_tracks") or [],
        "version_signature": row.get("version_signature") or "",
        "parsed_release_json": row.get("parsed_release_json") or "",
    }
    parsed_release_json = parse_release_title(payload["source_title"], payload["media_type"]).to_json()
    payload["parsed_release_json"] = parsed_release_json
    return parsed_release_json, refresh_item_version_fields(payload)


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Backfill ParsedRelease JSON and refresh derived signatures in place without replaying deliveries."
    )
    parser.add_argument("--dsn", default=os.getenv("DATABASE_URL", ""), help="Postgres DSN, defaults to DATABASE_URL")
    parser.add_argument("--kinozal-id", default="", help="Limit to one kinozal_id")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of rows to inspect")
    parser.add_argument("--apply", action="store_true", help="Persist updates in place")
    args = parser.parse_args()

    if not args.dsn:
        raise RuntimeError("DATABASE_URL is required")

    rows = _fetch_rows(args.dsn, kinozal_id=compact_spaces(args.kinozal_id), limit=max(int(args.limit or 0), 0))
    changed = 0
    skipped = 0

    with connect(args.dsn, row_factory=dict_row, autocommit=True) as conn:
        for row in rows:
            parsed_release_json, refreshed = _build_recomputed_payload(row)
            new_signature = compact_spaces(str(refreshed.get("version_signature") or ""))
            old_signature = compact_spaces(str(row.get("version_signature") or ""))
            old_parsed_json = compact_spaces(str(row.get("parsed_release_json") or ""))

            if old_parsed_json == compact_spaces(parsed_release_json) and old_signature == new_signature:
                skipped += 1
                continue

            changed += 1
            print(
                " | ".join(
                    [
                        f"id={int(row['id'])}",
                        f"kinozal_id={compact_spaces(str(row.get('kinozal_id') or '')) or '—'}",
                        f"status={'updated' if args.apply else 'dry-run'}",
                        f"version_old={old_signature or '—'}",
                        f"version_new={new_signature or '—'}",
                        f"parsed_changed={old_parsed_json != compact_spaces(parsed_release_json)}",
                    ]
                )
            )

            if not args.apply:
                continue

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE items
                    SET parsed_release_json = %s,
                        source_year = %s,
                        source_format = %s,
                        source_episode_progress = %s,
                        source_audio_tracks = %s,
                        version_signature = %s
                    WHERE id = %s
                    """,
                    (
                        parsed_release_json,
                        refreshed.get("source_year"),
                        refreshed.get("source_format") or "",
                        refreshed.get("source_episode_progress") or "",
                        json.dumps(refreshed.get("source_audio_tracks") or [], ensure_ascii=False),
                        new_signature,
                        int(row["id"]),
                    ),
                )

    print(
        " | ".join(
            [
                f"rows={len(rows)}",
                f"changed={changed}",
                f"unchanged={skipped}",
                f"mode={'apply' if args.apply else 'dry-run'}",
            ]
        )
    )


if __name__ == "__main__":
    main()
