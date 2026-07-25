"""
Unit tests for the SQL dialect converter (sqloader.dialect).

Coverage is anchored to the concrete incompatibilities catalogued in the
multi-SQL investigation (NR0003): AUTOINCREMENT primary keys, INSERT OR IGNORE,
datetime('now'), json_extract, PRAGMA statements, MySQL engine clauses and
parameter placeholders. No external database is required.
"""
import pytest

from sqloader.dialect import (
    DialectConverter,
    convert_sql,
    convert_placeholders,
    normalize_dialect,
)
from sqloader._prototype import SQLITE, MYSQL, POSTGRESQL


# ---------------------------------------------------------------------------
# normalize_dialect
# ---------------------------------------------------------------------------

class TestNormalizeDialect:
    @pytest.mark.parametrize("alias,expected", [
        ("sqlite", "sqlite"),
        ("SQLite3", "sqlite"),
        ("mysql", "mysql"),
        ("MariaDB", "mysql"),
        ("postgres", "postgresql"),
        ("PostgreSQL", "postgresql"),
        ("psql", "postgresql"),
        (SQLITE, "sqlite"),
        (MYSQL, "mysql"),
        (POSTGRESQL, "postgresql"),
    ])
    def test_aliases(self, alias, expected):
        assert normalize_dialect(alias) == expected

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            normalize_dialect("oracle")

    def test_same_source_target_is_identity(self):
        sql = "SELECT * FROM users WHERE id = ?"
        assert convert_sql(sql, "sqlite", "sqlite") == sql


# ---------------------------------------------------------------------------
# SQLite -> MySQL
# ---------------------------------------------------------------------------

class TestSqliteToMysql:
    def test_autoincrement_pk(self):
        out = convert_sql(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
            "sqlite", "mysql",
        )
        assert "AUTO_INCREMENT" in out
        assert "AUTOINCREMENT" not in out.replace("AUTO_INCREMENT", "")

    def test_insert_or_ignore(self):
        out = convert_sql("INSERT OR IGNORE INTO t (a) VALUES (1)", "sqlite", "mysql")
        assert out.strip().upper().startswith("INSERT IGNORE INTO")

    def test_insert_or_replace(self):
        out = convert_sql("INSERT OR REPLACE INTO t (a) VALUES (1)", "sqlite", "mysql")
        assert out.strip().upper().startswith("REPLACE INTO")

    def test_datetime_now(self):
        out = convert_sql("INSERT INTO t (ts) VALUES (datetime('now'))", "sqlite", "mysql")
        assert "UTC_TIMESTAMP()" in out
        assert "datetime('now')" not in out

    def test_json_extract_kept(self):
        # MySQL supports JSON_EXTRACT natively, so it must be preserved.
        out = convert_sql("SELECT json_extract(data, '$.name') FROM t", "sqlite", "mysql")
        assert "json_extract(data, '$.name')" in out

    def test_double_quote_idents_become_backticks(self):
        # SQLite double-quoted identifiers must become backticks; MySQL default
        # mode would otherwise read "id" as a string literal.
        out = convert_sql('SELECT "id" FROM "users"', "sqlite", "mysql")
        assert "`id`" in out and "`users`" in out
        assert '"id"' not in out


# ---------------------------------------------------------------------------
# SQLite -> PostgreSQL
# ---------------------------------------------------------------------------

class TestSqliteToPostgresql:
    def test_autoincrement_pk_becomes_serial(self):
        out = convert_sql(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT)",
            "sqlite", "postgresql",
        )
        assert "SERIAL PRIMARY KEY" in out
        assert "AUTOINCREMENT" not in out

    def test_insert_or_ignore_becomes_on_conflict(self):
        out = convert_sql("INSERT OR IGNORE INTO t (a) VALUES (1)", "sqlite", "postgresql")
        assert out.strip().upper().startswith("INSERT INTO")
        assert out.rstrip().upper().endswith("ON CONFLICT DO NOTHING")

    def test_insert_or_ignore_with_semicolon(self):
        out = convert_sql("INSERT OR IGNORE INTO t (a) VALUES (1);", "sqlite", "postgresql")
        assert out.rstrip().endswith("ON CONFLICT DO NOTHING;")

    def test_datetime_now(self):
        # Parity with FlowGate: datetime('now') maps to a to_char() expression
        # that reproduces SQLite's emitted 'YYYY-MM-DD HH:MM:SS' text.
        out = convert_sql("INSERT INTO t (ts) VALUES (datetime('now'))", "sqlite", "postgresql")
        assert "to_char(now() AT TIME ZONE 'UTC'" in out
        assert "datetime('now')" not in out

    def test_json_extract_becomes_arrow_operator(self):
        out = convert_sql("SELECT json_extract(data, '$.name') FROM t", "sqlite", "postgresql")
        assert "data->>'name'" in out
        assert "json_extract" not in out

    def test_multilevel_json_path_warns(self):
        conv = DialectConverter("sqlite", "postgresql")
        conv.convert("SELECT json_extract(data, '$.a.b') FROM t")
        assert any("multi-level" in w for w in conv.warnings)

    def test_insert_or_replace_warns(self):
        conv = DialectConverter("sqlite", "postgresql")
        out = conv.convert("INSERT OR REPLACE INTO t (a) VALUES (1)")
        assert out.strip().upper().startswith("INSERT INTO")
        assert any("INSERT OR REPLACE" in w for w in conv.warnings)


# ---------------------------------------------------------------------------
# PRAGMA handling
# ---------------------------------------------------------------------------

class TestPragmaDropping:
    def test_pragma_dropped_for_mysql(self):
        sql = "PRAGMA foreign_keys = ON;\nCREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        out = convert_sql(sql, "sqlite", "mysql")
        assert "PRAGMA" not in out.upper()
        assert "CREATE TABLE" in out.upper()

    def test_pragma_dropped_for_postgresql(self):
        sql = "PRAGMA foreign_keys = ON;\nCREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT);"
        out = convert_sql(sql, "sqlite", "postgresql")
        assert "PRAGMA" not in out.upper()

    def test_pragma_kept_for_sqlite_target(self):
        sql = "PRAGMA foreign_keys = ON;\nSELECT 1;"
        # mysql -> sqlite keeps everything that's already sqlite-ish
        out = convert_sql(sql, "mysql", "sqlite")
        assert "PRAGMA" in out.upper()


# ---------------------------------------------------------------------------
# Reverse: MySQL -> SQLite
# ---------------------------------------------------------------------------

class TestMysqlToSqlite:
    def test_auto_increment_pk(self):
        out = convert_sql(
            "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))",
            "mysql", "sqlite",
        )
        assert "INTEGER PRIMARY KEY AUTOINCREMENT" in out

    def test_engine_clause_removed(self):
        out = convert_sql(
            "CREATE TABLE t (id INT) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;",
            "mysql", "sqlite",
        )
        assert "ENGINE" not in out.upper()
        assert out.rstrip().endswith(");")

    def test_insert_ignore(self):
        out = convert_sql("INSERT IGNORE INTO t (a) VALUES (1)", "mysql", "sqlite")
        assert out.strip().upper().startswith("INSERT OR IGNORE INTO")

    def test_backticks_become_double_quotes(self):
        out = convert_sql("SELECT `id` FROM `users`", "mysql", "sqlite")
        assert "`" not in out
        assert '"id"' in out and '"users"' in out


# ---------------------------------------------------------------------------
# Reverse: PostgreSQL -> SQLite
# ---------------------------------------------------------------------------

class TestPostgresqlToSqlite:
    def test_serial_pk(self):
        out = convert_sql("CREATE TABLE t (id SERIAL PRIMARY KEY)", "postgresql", "sqlite")
        assert "INTEGER PRIMARY KEY AUTOINCREMENT" in out
        assert "SERIAL" not in out.upper()

    def test_arrow_operator_becomes_json_extract(self):
        out = convert_sql("SELECT data->>'name' FROM t", "postgresql", "sqlite")
        assert "json_extract(data, '$.name')" in out

    def test_on_conflict_do_nothing_becomes_insert_or_ignore(self):
        out = convert_sql(
            "INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING", "postgresql", "sqlite",
        )
        assert out.strip().upper().startswith("INSERT OR IGNORE INTO")
        assert "ON CONFLICT" not in out.upper()

    def test_on_conflict_do_update_warns(self):
        conv = DialectConverter("postgresql", "sqlite")
        conv.convert("INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2")
        assert any("ON CONFLICT" in w for w in conv.warnings)


# ---------------------------------------------------------------------------
# MySQL <-> PostgreSQL
# ---------------------------------------------------------------------------

class TestMysqlPostgresql:
    def test_mysql_to_postgres_serial(self):
        out = convert_sql(
            "CREATE TABLE t (id INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB;",
            "mysql", "postgresql",
        )
        assert "SERIAL PRIMARY KEY" in out
        assert "ENGINE" not in out.upper()

    def test_postgres_to_mysql_serial(self):
        out = convert_sql("CREATE TABLE t (id SERIAL PRIMARY KEY)", "postgresql", "mysql")
        assert "AUTO_INCREMENT" in out

    def test_postgres_to_mysql_double_quote_idents(self):
        out = convert_sql('SELECT "id" FROM "users"', "postgresql", "mysql")
        assert "`id`" in out and "`users`" in out


# ---------------------------------------------------------------------------
# Placeholders
# ---------------------------------------------------------------------------

class TestPlaceholders:
    def test_sqlite_to_mysql_placeholder(self):
        assert convert_placeholders("WHERE id = ?", "sqlite", "mysql") == "WHERE id = %s"

    def test_mysql_to_sqlite_placeholder(self):
        assert convert_placeholders("WHERE id = %s", "mysql", "sqlite") == "WHERE id = ?"

    def test_mysql_to_postgres_placeholder_noop(self):
        # both use %s
        assert convert_placeholders("WHERE id = %s", "mysql", "postgresql") == "WHERE id = %s"

    def test_convert_with_placeholders_flag(self):
        out = convert_sql("DELETE FROM t WHERE id = ?", "sqlite", "mysql", placeholders=True)
        assert out == "DELETE FROM t WHERE id = %s"

    def test_placeholders_off_by_default(self):
        out = convert_sql("DELETE FROM t WHERE id = ?", "sqlite", "mysql")
        assert "?" in out


# ---------------------------------------------------------------------------
# Multi-statement files
# ---------------------------------------------------------------------------

class TestMultiStatement:
    def test_multiple_statements_each_converted(self):
        sql = (
            "PRAGMA foreign_keys = ON;\n"
            "CREATE TABLE a (id INTEGER PRIMARY KEY AUTOINCREMENT);\n"
            "INSERT OR IGNORE INTO a (id) VALUES (1);\n"
        )
        out = convert_sql(sql, "sqlite", "postgresql")
        assert "PRAGMA" not in out.upper()
        assert "SERIAL PRIMARY KEY" in out
        assert "ON CONFLICT DO NOTHING" in out.upper()

    def test_warnings_reset_between_calls(self):
        conv = DialectConverter("sqlite", "postgresql")
        conv.convert("SELECT json_extract(data, '$.a.b') FROM t")
        assert conv.warnings
        conv.convert("SELECT 1")
        assert conv.warnings == []
