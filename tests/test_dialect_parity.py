"""
Parity tests: sqloader.dialect ↔ FlowGate runtime translator.

Background (0007-NR): FlowGate's runtime translator
(``server/modules/flow_gate/db/dialect.py::translate``) rewrites SQLite-authored
inline queries for MySQL/PostgreSQL at execution time. The library converter was
written independently and was missing six of those runtime-DML rules on the
overlapping direction (SQLite → MySQL/PostgreSQL). 0008-T ports them.

These tests pin the library output to the exact string FlowGate ``translate()``
produces for a set of representative queries, so the parity can never silently
regress. The expected strings are literal FlowGate outputs (its translator is
deterministic and always escapes ``%`` + converts ``?`` → ``%s``); the library
reproduces them when called with ``placeholders=True, escape_percent=True``.

A best-effort *live* cross-check against the real FlowGate module runs too when
``FLOWGATE_DIALECT_PATH`` points at a checkout of ``db/dialect.py``; it is skipped
otherwise so the suite stays self-contained and portable.
"""
import importlib.util
import os

import pytest

from sqloader.dialect import (
    DialectConverter,
    convert_sql,
    convert_placeholders,
)


def _to(sql, target):
    """Library equivalent of FlowGate translate(): full runtime-DML rewrite."""
    return convert_sql(sql, "sqlite", target, placeholders=True, escape_percent=True)


# ---------------------------------------------------------------------------
# Parity fixtures: (source_sql, mysql_expected, postgresql_expected)
# The expected values are exact FlowGate translate() outputs.
# ---------------------------------------------------------------------------

PARITY_CASES = [
    pytest.param(
        "SELECT * FROM t WHERE note = 'why?' AND pct LIKE '%x%' AND id = ?",
        "SELECT * FROM t WHERE note = 'why?' AND pct LIKE '%%x%%' AND id = %s",
        "SELECT * FROM t WHERE note = 'why?' AND pct LIKE '%%x%%' AND id = %s",
        id="literal-aware-placeholder-and-percent-escape",
    ),
    pytest.param(
        "INSERT INTO t (created_at) VALUES (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        "INSERT INTO t (created_at) VALUES "
        "(DATE_FORMAT(UTC_TIMESTAMP(3), '%%Y-%%m-%%dT%%H:%%i:%%S.%%fZ'))",
        "INSERT INTO t (created_at) VALUES "
        "(to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.MS\"Z\"'))",
        id="strftime-now",
    ),
    pytest.param(
        "INSERT INTO t (ts) VALUES (datetime('now'))",
        "INSERT INTO t (ts) VALUES (DATE_FORMAT(UTC_TIMESTAMP(), '%%Y-%%m-%%d %%H:%%i:%%S'))",
        "INSERT INTO t (ts) VALUES (to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS'))",
        id="datetime-now",
    ),
    pytest.param(
        "SELECT last_insert_rowid()",
        "SELECT LAST_INSERT_ID()",
        "SELECT lastval()",
        id="last-insert-rowid",
    ),
    pytest.param(
        "INSERT INTO t (a) VALUES (?) ON CONFLICT (a) DO NOTHING",
        "INSERT IGNORE INTO t (a) VALUES (%s)",
        # ON CONFLICT DO NOTHING is PostgreSQL-native, so it passes through.
        "INSERT INTO t (a) VALUES (%s) ON CONFLICT (a) DO NOTHING",
        id="on-conflict-do-nothing",
    ),
    pytest.param(
        "INSERT INTO users (id, name) VALUES (?, ?) "
        "ON CONFLICT (id) DO UPDATE SET name = excluded.name, seen = users.seen + 1",
        "INSERT INTO users (id, name) VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE name = VALUES(name), seen = seen + 1",
        # ON CONFLICT ... DO UPDATE + excluded.* is PostgreSQL-native.
        "INSERT INTO users (id, name) VALUES (%s, %s) "
        "ON CONFLICT (id) DO UPDATE SET name = excluded.name, seen = users.seen + 1",
        id="on-conflict-do-update",
    ),
]


class TestFlowGateParityPinned:
    @pytest.mark.parametrize("sql,my_expected,pg_expected", PARITY_CASES)
    def test_mysql_matches_flowgate(self, sql, my_expected, pg_expected):
        assert _to(sql, "mysql") == my_expected

    @pytest.mark.parametrize("sql,my_expected,pg_expected", PARITY_CASES)
    def test_postgresql_matches_flowgate(self, sql, my_expected, pg_expected):
        assert _to(sql, "postgresql") == pg_expected


# ---------------------------------------------------------------------------
# Focused behaviour tests for each ported rule (readable failures).
# ---------------------------------------------------------------------------

class TestPlaceholderLiteralAware:
    def test_question_mark_inside_literal_is_kept(self):
        # gap 1: the '?' in 'why?' must survive; only the bind marker converts.
        out = convert_placeholders("WHERE note = 'why?' AND id = ?", "sqlite", "mysql")
        assert out == "WHERE note = 'why?' AND id = %s"

    def test_percent_escaped_before_placeholder(self):
        # gap 2: literal % doubled, bind marker introduced afterwards untouched.
        out = convert_placeholders(
            "WHERE pct LIKE '%x%' AND id = ?", "sqlite", "mysql", escape_percent=True
        )
        assert out == "WHERE pct LIKE '%%x%%' AND id = %s"

    def test_percent_not_escaped_by_default(self):
        out = convert_placeholders("WHERE pct LIKE '%x%' AND id = ?", "sqlite", "mysql")
        assert out == "WHERE pct LIKE '%x%' AND id = %s"

    def test_reverse_percent_marker_inside_literal_kept(self):
        # %s inside a literal is not un-bound when going back to sqlite.
        out = convert_placeholders("WHERE note = '50%s off' AND id = %s", "mysql", "sqlite")
        assert out == "WHERE note = '50%s off' AND id = ?"


class TestOnConflictToMysql:
    def test_sqlite_do_nothing_becomes_insert_ignore(self):
        # gap 6: previously passed through silently and broke MySQL.
        out = convert_sql(
            "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO NOTHING", "sqlite", "mysql"
        )
        assert out.strip().upper().startswith("INSERT IGNORE INTO")
        assert "ON CONFLICT" not in out.upper()

    def test_sqlite_do_update_becomes_on_duplicate_key_update(self):
        # gap 3.
        out = convert_sql(
            "INSERT INTO users (id, n) VALUES (1, 2) "
            "ON CONFLICT (id) DO UPDATE SET n = excluded.n",
            "sqlite", "mysql",
        )
        assert "ON DUPLICATE KEY UPDATE n = VALUES(n)" in out
        assert "ON CONFLICT" not in out.upper()

    def test_do_update_drops_conditional_where(self):
        out = convert_sql(
            "INSERT INTO t (id, v) VALUES (1, 2) "
            "ON CONFLICT (id) DO UPDATE SET v = excluded.v WHERE t.v < excluded.v",
            "sqlite", "mysql",
        )
        assert "WHERE" not in out.upper()
        assert "ON DUPLICATE KEY UPDATE v = VALUES(v)" in out

    def test_do_update_preserves_trailing_semicolon(self):
        out = convert_sql(
            "INSERT INTO t (id, v) VALUES (1, 2) "
            "ON CONFLICT (id) DO UPDATE SET v = excluded.v;",
            "sqlite", "mysql",
        )
        assert out.rstrip().endswith(";")

    def test_postgresql_do_update_now_converted_not_just_warned(self):
        # gap 3 also covered the pg → mysql path (was warning-only before).
        conv = DialectConverter("postgresql", "mysql")
        out = conv.convert(
            "INSERT INTO t (id, v) VALUES (1, 2) ON CONFLICT (id) DO UPDATE SET v = excluded.v"
        )
        assert "ON DUPLICATE KEY UPDATE v = VALUES(v)" in out
        assert not conv.warnings


class TestNowExpressions:
    def test_strftime_mysql(self):
        out = convert_sql(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')", "sqlite", "mysql"
        )
        assert "DATE_FORMAT(UTC_TIMESTAMP(3)" in out
        assert "strftime" not in out

    def test_strftime_postgresql(self):
        out = convert_sql(
            "SELECT strftime('%Y-%m-%dT%H:%M:%fZ', 'now')", "sqlite", "postgresql"
        )
        assert "to_char(now() AT TIME ZONE 'UTC'" in out
        assert "strftime" not in out


class TestLastInsertRowid:
    def test_mysql(self):
        assert "LAST_INSERT_ID()" in convert_sql(
            "SELECT last_insert_rowid()", "sqlite", "mysql"
        )

    def test_postgresql(self):
        assert "lastval()" in convert_sql(
            "SELECT last_insert_rowid()", "sqlite", "postgresql"
        )


# ---------------------------------------------------------------------------
# Optional live cross-check against the real FlowGate module.
# ---------------------------------------------------------------------------

def _load_flowgate_translate():
    path = os.environ.get("FLOWGATE_DIALECT_PATH")
    if not path or not os.path.isfile(path):
        return None
    spec = importlib.util.spec_from_file_location("_flowgate_dialect", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestFlowGateLiveCrossCheck:
    @pytest.mark.parametrize("sql,my_expected,pg_expected", PARITY_CASES)
    def test_live(self, sql, my_expected, pg_expected):
        fg = _load_flowgate_translate()
        if fg is None:
            pytest.skip("FLOWGATE_DIALECT_PATH not set; skipping live cross-check")
        assert fg.translate(sql, fg.MYSQL) == _to(sql, "mysql")
        assert fg.translate(sql, fg.POSTGRESQL) == _to(sql, "postgresql")
