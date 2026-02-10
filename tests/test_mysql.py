"""
Unit tests for MySqlWrapper using mocked pymysql.
No real MySQL connection required.
"""
import pytest
from unittest.mock import MagicMock, patch, call
import pymysql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wrapper():
    """Instantiate MySqlWrapper with pymysql.connect fully mocked."""
    with patch("sqloader.mysql.pymysql.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        from sqloader.mysql import MySqlWrapper
        wrapper = MySqlWrapper(
            host="localhost",
            user="root",
            password="pass",
            db="testdb",
            port=3306,
        )
        return wrapper


def _mock_connect_ctx(rows=None, rowcount=1):
    """
    Return a mock pymysql connection whose DictCursor context yields given rows.
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.execute.return_value = rowcount
    mock_cursor.fetchone.return_value = rows[0] if rows else None
    mock_cursor.fetchall.return_value = rows or []
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# execute_query / execute
# ---------------------------------------------------------------------------

class TestExecute:
    def test_execute_commits_by_default(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx()

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            wrapper.execute("INSERT INTO t (v) VALUES (%s)", ["x"])

        mock_conn.commit.assert_called_once()

    def test_execute_no_commit_when_false(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx()

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            wrapper.execute("INSERT INTO t (v) VALUES (%s)", ["x"], commit=False)

        mock_conn.commit.assert_not_called()

    def test_execute_closes_connection(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx()

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            wrapper.execute("INSERT INTO t (v) VALUES (%s)", ["x"])

        mock_conn.close.assert_called()

    def test_execute_rollback_and_reraise_on_error(self):
        wrapper = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.execute.side_effect = pymysql.MySQLError(1064, "SQL syntax error")
        mock_conn.cursor.return_value = mock_cursor

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            with pytest.raises(pymysql.MySQLError):
                wrapper.execute("BAD SQL")

        mock_conn.rollback.assert_called()

    def test_execute_without_params(self):
        wrapper = _make_wrapper()
        mock_conn, mock_cursor = _mock_connect_ctx()

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            wrapper.execute("DELETE FROM t")

        mock_cursor.execute.assert_called()


# ---------------------------------------------------------------------------
# fetch_one
# ---------------------------------------------------------------------------

class TestFetchOne:
    def test_returns_row(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx(rows=[{"id": 1, "name": "Alice"}])

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            result = wrapper.fetch_one("SELECT * FROM users WHERE id = %s", [1])

        assert result == {"id": 1, "name": "Alice"}

    def test_returns_none_when_not_found(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx(rows=[])

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            result = wrapper.fetch_one("SELECT * FROM users WHERE id = %s", [999])

        assert result is None

    def test_connection_closed_after_fetch(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx(rows=[])

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            wrapper.fetch_one("SELECT 1")

        mock_conn.close.assert_called()

    def test_raises_and_closes_on_db_error(self):
        wrapper = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.execute.side_effect = pymysql.MySQLError(1064, "syntax error")
        mock_conn.cursor.return_value = mock_cursor

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            with pytest.raises(pymysql.MySQLError):
                wrapper.fetch_one("BAD SQL")

        mock_conn.close.assert_called()


# ---------------------------------------------------------------------------
# fetch_all
# ---------------------------------------------------------------------------

class TestFetchAll:
    def test_returns_all_rows(self):
        rows = [{"id": 1}, {"id": 2}]
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx(rows=rows)

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            result = wrapper.fetch_all("SELECT * FROM t")

        assert result == rows

    def test_connection_closed_after_fetch(self):
        wrapper = _make_wrapper()
        mock_conn, _ = _mock_connect_ctx(rows=[])

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            wrapper.fetch_all("SELECT 1")

        mock_conn.close.assert_called()


# ---------------------------------------------------------------------------
# begin_transaction
# ---------------------------------------------------------------------------

class TestTransaction:
    def test_transaction_commits_on_success(self):
        wrapper = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            with wrapper.begin_transaction() as txn:
                txn.execute("INSERT INTO t (v) VALUES (%s)", ["x"])

        mock_conn.commit.assert_called_once()

    def test_transaction_rolls_back_on_exception(self):
        wrapper = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            with pytest.raises(ValueError):
                with wrapper.begin_transaction() as txn:
                    txn.execute("INSERT INTO t (v) VALUES (%s)", ["x"])
                    raise ValueError("forced error")

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()

    def test_transaction_fetchone(self):
        wrapper = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"id": 42}
        mock_conn.cursor.return_value = mock_cursor

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            with wrapper.begin_transaction() as txn:
                txn.execute("SELECT * FROM t WHERE id = %s", [42])
                row = txn.fetchone()

        assert row == {"id": 42}

    def test_transaction_fetchall(self):
        wrapper = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"id": 1}, {"id": 2}]
        mock_conn.cursor.return_value = mock_cursor

        with patch("sqloader.mysql.pymysql.connect", return_value=mock_conn):
            with wrapper.begin_transaction() as txn:
                txn.execute("SELECT * FROM t")
                rows = txn.fetchall()

        assert len(rows) == 2