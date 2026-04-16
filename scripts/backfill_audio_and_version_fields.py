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

from parsing_audio import parse_audio_tracks
from release_versioning import refresh_item_version_fields
from utils import compact_spaces


# variant_components/variant_signature в проекте вычисляются runtime.
# Скрипт пересчитывает их для dry-run/audit, а in place обновляет только
# хранимые derived поля items: source_audio_tracks и version_signature.
def _parse_audio_tracks_field(value: Any) -> List[str]:
    if isinstance(value, list):
        return [compact_spaces(str(item or "")) for item in value if compact_spaces(str(item or ""))]
    raw = compact_spaces(str(value or ""))
    if not raw:
        return []
    try:
        loaded = json.loads(raw)
    except Exception:
        loaded = [part.strip() for part in raw.split(",") if part.strip()]
    if isinstance(loaded, list):
        return [compact_spaces(str(item or "")) for item in loaded if compact_spaces(str(item or ""))]
    return [compact_spaces(str(loaded or ""))] if compact_spaces(str(loaded or "")) else []


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
                    version_signature
                FROM items
                WHERE {' AND '.join(filters)}
                ORDER BY id DESC{limit_sql}
                """,
                params,
            )
            return list(cur.fetchall())


def _has_signature_collision(conn: Any, item_id: int, source_uid: str, version_signature: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM items
            WHERE source_uid = %s
              AND version_signature = %s
              AND id <> %s
            LIMIT 1
            """,
            (source_uid, version_signature, int(item_id)),
        )
        return cur.fetchone() is not None


def main() -> None:
    load_dotenv()
    parser = argparse.ArgumentParser(
        description="Recompute source_audio_tracks and version signatures in place without replaying deliveries."
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
    skipped_collision = 0
    skipped_unchanged = 0

    with connect(args.dsn, row_factory=dict_row, autocommit=True) as conn:
        for row in rows:
            current_tracks = _parse_audio_tracks_field(row.get("source_audio_tracks"))
            derived_tracks = parse_audio_tracks(row.get("source_title") or "")
            payload = {
                "id": int(row["id"]),
                "source_uid": row.get("source_uid") or "",
                "media_type": row.get("media_type") or "movie",
                "source_title": row.get("source_title") or "",
                "source_episode_progress": row.get("source_episode_progress") or "",
                "source_format": row.get("source_format") or "",
                "source_audio_tracks": derived_tracks,
                "version_signature": row.get("version_signature") or "",
            }
            refreshed = refresh_item_version_fields(payload)
            old_signature = compact_spaces(str(row.get("version_signature") or ""))
            new_signature = compact_spaces(str(refreshed.get("version_signature") or ""))

            if current_tracks == derived_tracks and old_signature == new_signature:
                skipped_unchanged += 1
                continue

            if _has_signature_collision(conn, int(row["id"]), compact_spaces(str(row.get("source_uid") or "")), new_signature):
                skipped_collision += 1
                print(
                    " | ".join(
                        [
                            f"id={int(row['id'])}",
                            f"kinozal_id={compact_spaces(str(row.get('kinozal_id') or '')) or '—'}",
                            "status=collision",
                            f"tracks_old={current_tracks}",
                            f"tracks_new={derived_tracks}",
                            f"version_old={old_signature or '—'}",
                            f"version_new={new_signature or '—'}",
                        ]
                    )
                )
                continue

            changed += 1
            print(
                " | ".join(
                    [
                        f"id={int(row['id'])}",
                        f"kinozal_id={compact_spaces(str(row.get('kinozal_id') or '')) or '—'}",
                        f"status={'updated' if args.apply else 'dry-run'}",
                        f"tracks_old={current_tracks}",
                        f"tracks_new={derived_tracks}",
                        f"variant_audio={refreshed.get('variant_components', {}).get('audio', '—')}",
                        f"variant_sig={compact_spaces(str(refreshed.get('variant_signature') or '')) or '—'}",
                        f"version_old={old_signature or '—'}",
                        f"version_new={new_signature or '—'}",
                    ]
                )
            )

            if not args.apply:
                continue

            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE items
                    SET source_audio_tracks = %s,
                        version_signature = %s
                    WHERE id = %s
                      AND COALESCE(source_audio_tracks, '') = %s
                      AND COALESCE(version_signature, '') = %s
                    """,
                    (
                        json.dumps(derived_tracks, ensure_ascii=False),
                        new_signature,
                        int(row["id"]),
                        compact_spaces(str(row.get("source_audio_tracks") or "")),
                        old_signature,
                    ),
                )

    print(
        " | ".join(
            [
                f"rows={len(rows)}",
                f"changed={changed}",
                f"unchanged={skipped_unchanged}",
                f"collisions={skipped_collision}",
                f"mode={'apply' if args.apply else 'dry-run'}",
            ]
        )
    )


if __name__ == "__main__":
    main()
