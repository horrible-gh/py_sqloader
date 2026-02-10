"""
Unit tests for SQLoader — no database connection required.
Uses tests/test.json as the SQL fixture file.
"""
import os
import pytest
from unittest.mock import MagicMock
from sqloader.sqloader import SQLoader
from sqloader._prototype import MYSQL, SQLITE, POSTGRESQL

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------------------
# load_sql
# ---------------------------------------------------------------------------

class TestLoadSql:
    def test_simple_key(self):
        sq = SQLoader(TESTS_DIR)
        assert sq.load_sql("test", "get_test") == "SELECT 1"

    def test_nested_key_dot_notation(self):
        sq = SQLoader(TESTS_DIR)
        assert sq.load_sql("test", "grp.grp_test") == "SELECT 2"

    def test_file_not_found(self):
        sq = SQLoader(TESTS_DIR)
        with pytest.raises(FileNotFoundError):
            sq.load_sql("nonexistent_file", "get_test")

    def test_query_not_found(self):
        sq = SQLoader(TESTS_DIR)
        with pytest.raises(ValueError):
            sq.load_sql("test", "no_such_key")

    def test_json_suffix_handled(self):
        """Passing filename with .json extension should not double the suffix."""
        sq = SQLoader(TESTS_DIR)
        assert sq.load_sql("test.json", "get_test") == "SELECT 1"


# ---------------------------------------------------------------------------
# Placeholder parsing
# ---------------------------------------------------------------------------

class TestParsePlaceholder:
    def test_none_returns_none(self):
        sq = SQLoader(TESTS_DIR)
        assert sq._parse_placeholder(None) is None

    def test_single_string(self):
        sq = SQLoader(TESTS_DIR)
        assert sq._parse_placeholder("%s") == ["%s"]

    def test_comma_separated_string(self):
        sq = SQLoader(TESTS_DIR)
        assert sq._parse_placeholder("?,%s") == ["?", "%s"]

    def test_list_passthrough(self):
        sq = SQLoader(TESTS_DIR)
        assert sq._parse_placeholder(["?", "%s"]) == ["?", "%s"]

    def test_list_strips_whitespace(self):
        sq = SQLoader(TESTS_DIR)
        assert sq._parse_placeholder([" ? ", " %s "]) == ["?", "%s"]


# ---------------------------------------------------------------------------
# Placeholder conversion
# ---------------------------------------------------------------------------

class TestConvertPlaceholder:
    def test_sqlite_converts_percent_s_to_question_mark(self):
        sq = SQLoader(TESTS_DIR, db_type=SQLITE, placeholder=["%s"])
        result = sq._convert_placeholder("SELECT * FROM t WHERE id = %s")
        assert result == "SELECT * FROM t WHERE id = ?"

    def test_mysql_no_conversion_needed(self):
        sq = SQLoader(TESTS_DIR, db_type=MYSQL, placeholder=["?", "%s"])
        result = sq._convert_placeholder("SELECT * FROM t WHERE id = %s")
        assert result == "SELECT * FROM t WHERE id = %s"

    def test_postgresql_no_conversion_needed(self):
        sq = SQLoader(TESTS_DIR, db_type=POSTGRESQL, placeholder=["?", "%s"])
        result = sq._convert_placeholder("SELECT * FROM t WHERE id = %s")
        assert result == "SELECT * FROM t WHERE id = %s"

    def test_no_placeholder_config_returns_unchanged(self):
        sq = SQLoader(TESTS_DIR)
        original = "SELECT * FROM t WHERE id = ?"
        assert sq._convert_placeholder(original) == original


# ---------------------------------------------------------------------------
# DB injection and _require_db
# ---------------------------------------------------------------------------

class TestDbInjection:
    def test_no_db_raises_on_execute(self):
        sq = SQLoader(TESTS_DIR)
        with pytest.raises(RuntimeError, match="No database instance attached"):
            sq.execute("test", "get_test")

    def test_no_db_raises_on_fetch_one(self):
        sq = SQLoader(TESTS_DIR)
        with pytest.raises(RuntimeError):
            sq.fetch_one("test", "get_test")

    def test_no_db_raises_on_fetch_all(self):
        sq = SQLoader(TESTS_DIR)
        with pytest.raises(RuntimeError):
            sq.fetch_all("test", "get_test")

    def test_set_db_injects_instance(self):
        sq = SQLoader(TESTS_DIR)
        mock_db = MagicMock()
        sq.set_db(mock_db)
        assert sq.db is mock_db

    def test_db_via_constructor(self):
        mock_db = MagicMock()
        sq = SQLoader(TESTS_DIR, db=mock_db)
        assert sq.db is mock_db


# ---------------------------------------------------------------------------
# Integrated execution methods
# ---------------------------------------------------------------------------

class TestIntegratedExecution:
    def _sq_with_mock_db(self):
        mock_db = MagicMock()
        sq = SQLoader(TESTS_DIR, db=mock_db)
        return sq, mock_db

    def test_execute_calls_db_execute(self):
        sq, mock_db = self._sq_with_mock_db()
        sq.execute("test", "get_test")
        mock_db.execute.assert_called_once_with("SELECT 1", None)

    def test_execute_passes_params(self):
        sq, mock_db = self._sq_with_mock_db()
        sq.execute("test", "get_test", [42])
        mock_db.execute.assert_called_once_with("SELECT 1", [42])

    def test_fetch_one_calls_db_fetch_one(self):
        sq, mock_db = self._sq_with_mock_db()
        mock_db.fetch_one.return_value = {"id": 1}
        result = sq.fetch_one("test", "get_test")
        mock_db.fetch_one.assert_called_once_with("SELECT 1", None)
        assert result == {"id": 1}

    def test_fetch_all_calls_db_fetch_all(self):
        sq, mock_db = self._sq_with_mock_db()
        mock_db.fetch_all.return_value = [{"id": 1}, {"id": 2}]
        result = sq.fetch_all("test", "get_test")
        mock_db.fetch_all.assert_called_once_with("SELECT 1", None)
        assert len(result) == 2