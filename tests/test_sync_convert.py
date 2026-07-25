"""
Tests for SQLoader.sync() with dialect conversion (the ``convert=`` option).

These exercise the runtime integration added on top of the standalone
``sqloader.dialect`` converter: syncing a directory of ``.sql`` (and ``.json``)
query files from one DB dialect layout to another, optionally rewriting the SQL.
No external database is required.
"""
import os

import pytest

from sqloader.sqloader import SQLoader


def _write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


@pytest.fixture
def sql_tree(tmp_path):
    """A sql_dir with a sqlite3/ source dir holding one .sql and one .json file."""
    root = tmp_path / "sql"
    src = root / "sqlite3"
    _write(str(src / "001_schema.sql"),
           "PRAGMA foreign_keys = ON;\n"
           "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT);\n"
           "INSERT OR IGNORE INTO t (id) VALUES (1);\n")
    _write(str(src / "queries.json"), '{"get": "SELECT * FROM t WHERE id = ?"}')
    return root


class TestSyncCopyStillDefault:
    def test_sync_without_convert_is_byte_copy(self, sql_tree):
        sq = SQLoader(str(sql_tree))
        result = sq.sync("sqlite3", "postgresql")
        assert result["warnings"] == []
        out = sql_tree / "postgresql" / "001_schema.sql"
        assert out.read_text(encoding="utf-8").count("AUTOINCREMENT") == 1  # untouched


class TestSyncConvert:
    def test_convert_rewrites_sql_for_postgresql(self, sql_tree):
        sq = SQLoader(str(sql_tree))
        result = sq.sync("sqlite3", "postgresql", convert=True)

        assert "001_schema.sql" in result["copied"]
        out = (sql_tree / "postgresql" / "001_schema.sql").read_text(encoding="utf-8")
        assert "SERIAL PRIMARY KEY" in out
        assert "AUTOINCREMENT" not in out
        assert "PRAGMA" not in out.upper()          # sqlite-only pragma dropped
        assert "ON CONFLICT DO NOTHING" in out.upper()

    def test_convert_rewrites_sql_for_mysql(self, sql_tree):
        sq = SQLoader(str(sql_tree))
        sq.sync("sqlite3", "mysql", convert=True)
        out = (sql_tree / "mysql" / "001_schema.sql").read_text(encoding="utf-8")
        assert "AUTO_INCREMENT" in out
        assert "INSERT IGNORE" in out.upper()

    def test_json_files_copied_verbatim(self, sql_tree):
        sq = SQLoader(str(sql_tree))
        sq.sync("sqlite3", "postgresql", convert=True)
        out = (sql_tree / "postgresql" / "queries.json").read_text(encoding="utf-8")
        assert out == '{"get": "SELECT * FROM t WHERE id = ?"}'

    def test_warnings_are_prefixed_with_relpath(self, tmp_path):
        root = tmp_path / "sql"
        _write(str(root / "sqlite3" / "q.sql"),
               "SELECT json_extract(data, '$.a.b') FROM t;")
        sq = SQLoader(str(root))
        result = sq.sync("sqlite3", "postgresql", convert=True)
        assert any(w.startswith("q.sql:") and "multi-level" in w
                   for w in result["warnings"])

    def test_convert_false_yields_no_warnings(self, tmp_path):
        root = tmp_path / "sql"
        _write(str(root / "sqlite3" / "q.sql"),
               "SELECT json_extract(data, '$.a.b') FROM t;")
        sq = SQLoader(str(root))
        result = sq.sync("sqlite3", "postgresql", convert=False)
        assert result["warnings"] == []

    def test_missing_source_dir_raises(self, tmp_path):
        sq = SQLoader(str(tmp_path / "sql"))
        with pytest.raises(FileNotFoundError):
            sq.sync("sqlite3", "mysql", convert=True)
