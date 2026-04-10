from pathlib import Path

from db_migrations import apply_schema_migrations


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    def __init__(self, applied_rows=None):
        self.applied_rows = applied_rows or []
        self.scripts = []
        self.executes = []
        self.commits = 0

    def executescript(self, script: str) -> None:
        self.scripts.append(script)

    def execute(self, query: str, params=None):
        self.executes.append((query, params))
        if query.startswith("SELECT version FROM schema_migrations"):
            return FakeCursor(self.applied_rows)
        return FakeCursor([])

    def commit(self) -> None:
        self.commits += 1


def test_apply_schema_migrations_runs_pending_files_in_order(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0002_add_index.sql").write_text("SELECT 2;\n", encoding="utf-8")
    (migrations_dir / "0001_initial_schema.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (migrations_dir / "README.txt").write_text("ignore\n", encoding="utf-8")

    conn = FakeConn()
    apply_schema_migrations(conn, migrations_dir)

    assert conn.scripts[0].strip().startswith("CREATE TABLE IF NOT EXISTS schema_migrations")
    assert conn.scripts[1:] == ["SELECT 1;\n", "SELECT 2;\n"]
    assert conn.executes[1][1][:2] == ("0001", "initial_schema")
    assert conn.executes[2][1][:2] == ("0002", "add_index")
    assert conn.commits == 2


def test_apply_schema_migrations_skips_applied_versions(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    (migrations_dir / "0001_initial_schema.sql").write_text("SELECT 1;\n", encoding="utf-8")
    (migrations_dir / "0002_add_index.sql").write_text("SELECT 2;\n", encoding="utf-8")

    conn = FakeConn(applied_rows=[{"version": "0001"}])
    apply_schema_migrations(conn, migrations_dir)

    assert conn.scripts == [
        conn.scripts[0],
        "SELECT 2;\n",
    ]
    assert len(conn.executes) == 2
    assert conn.executes[1][1][:2] == ("0002", "add_index")
    assert conn.commits == 1
