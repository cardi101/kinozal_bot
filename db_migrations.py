import re
import time
from pathlib import Path
from typing import Any, Iterable, List, Sequence, Tuple

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
MIGRATION_FILENAME_RE = re.compile(r"^(?P<version>\d+)_(?P<name>[a-z0-9_]+)\.sql$")

SCHEMA_MIGRATIONS_SQL = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at BIGINT NOT NULL
);
"""


def _migration_files(migrations_dir: Path = MIGRATIONS_DIR) -> List[Tuple[str, str, Path]]:
    files: List[Tuple[str, str, Path]] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        match = MIGRATION_FILENAME_RE.match(path.name)
        if not match:
            continue
        files.append((match.group("version"), match.group("name"), path))
    return files


def _extract_versions(rows: Iterable[Any]) -> set[str]:
    versions: set[str] = set()
    for row in rows:
        if isinstance(row, dict):
            value = row.get("version")
        elif isinstance(row, Sequence) and row:
            value = row[0]
        else:
            value = getattr(row, "version", None)
        if value is not None:
            versions.add(str(value))
    return versions


def apply_schema_migrations(conn: Any, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    conn.executescript(SCHEMA_MIGRATIONS_SQL)
    applied_rows = conn.execute(
        "SELECT version FROM schema_migrations ORDER BY version"
    ).fetchall()
    applied_versions = _extract_versions(applied_rows)

    for version, name, path in _migration_files(migrations_dir):
        if version in applied_versions:
            continue
        conn.executescript(path.read_text(encoding="utf-8"))
        conn.execute(
            "INSERT INTO schema_migrations(version, name, applied_at) VALUES(?, ?, ?)",
            (version, name, int(time.time())),
        )
        conn.commit()
