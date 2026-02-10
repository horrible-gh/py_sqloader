"""
Unit tests for PostgreSQLWrapper using mocked psycopg2.
No real PostgreSQL connection required.
"""
import pytest
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wrapper(max_parallel_queries=5):
    """Instantiate PostgreSQLWrapper with a fully mocked connection pool."""
    with patch("sqloader.postgresql.psycopg2.pool.SimpleConnectionPool") as mock_pool_cls:
        from sqloader.postgresql import PostgreSQLWrapper
        mock_pool = MagicMock()
        mock_pool_cls.return_value = mock_pool

        wrapper = PostgreSQLWrapper(
            host="localhost",
            user="test",
            password="test",
            database="testdb",
            port=5432,
            max_parallel_queries=max_parallel_queries,
        )
        wrapper.pool = mock_pool
        return wrapper, mock_pool


def _mock_conn(rows=None, rowcount=1):
    """Return a mock connection whose cursor yields given rows."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)
    mock_cursor.fetchone.return_value = rows[0] if rows else None
    mock_cursor.fetchall.return_value = rows or []
    mock_cursor.rowcount = rowcount
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestInit:
    def test_pool_created_with_correct_params(self):
        with patch("sqloader.postgresql.psycopg2.pool.SimpleConnectionPool") as mock_cls:
            from sqloader.postgresql import PostgreSQLWrapper
            mock_cls.return_value = MagicMock()
            PostgreSQLWrapper(
                host="db.host",
                user="admin",
                password="secret",
                database="mydb",
                port=5433,
                max_parallel_queries=3,
            )
            mock_cls.assert_called_once_with(
                minconn=1,
                maxconn=3,
                host="db.host",
                port=5433,
                database="mydb",
                user="admin",
                password="secret",
            )


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------

class TestExecute:
    def test_execute_acquires_and_releases_connection(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn()
        pool.getconn.return_value = mock_conn

        wrapper.execute("INSERT INTO t (v) VALUES (%s)", ["x"])

        pool.getconn.assert_called_once()
        pool.putconn.assert_called_once_with(mock_conn)

    def test_execute_commits_by_default(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn()
        pool.getconn.return_value = mock_conn

        wrapper.execute("INSERT INTO t (v) VALUES (%s)", ["x"])

        mock_conn.commit.assert_called_once()

    def test_execute_no_commit_when_false(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn()
        pool.getconn.return_value = mock_conn

        wrapper.execute("INSERT INTO t (v) VALUES (%s)", ["x"], commit=False)

        mock_conn.commit.assert_not_called()

    def test_execute_returns_rowcount(self):
        wrapper, pool = _make_wrapper()
        mock_conn, mock_cursor = _mock_conn(rowcount=3)
        pool.getconn.return_value = mock_conn

        result = wrapper.execute("UPDATE t SET v = %s", ["y"])

        assert result == 3

    def test_execute_rollback_on_error(self):
        import psycopg2
        wrapper, pool = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.execute.side_effect = psycopg2.OperationalError("db error")
        mock_conn.cursor.return_value = mock_cursor
        pool.getconn.return_value = mock_conn

        with pytest.raises(psycopg2.OperationalError):
            wrapper.execute("BAD SQL")

        mock_conn.rollback.assert_called()
        pool.putconn.assert_called_once_with(mock_conn)


# ---------------------------------------------------------------------------
# fetch_one
# ---------------------------------------------------------------------------

class TestFetchOne:
    def test_returns_row(self):
        wrapper, pool = _make_wrapper()
        mock_conn, mock_cursor = _mock_conn(rows=[{"id": 1, "name": "Alice"}])
        pool.getconn.return_value = mock_conn

        result = wrapper.fetch_one("SELECT * FROM users WHERE id = %s", [1])

        assert result == {"id": 1, "name": "Alice"}

    def test_returns_none_when_not_found(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn(rows=[])
        pool.getconn.return_value = mock_conn

        result = wrapper.fetch_one("SELECT * FROM users WHERE id = %s", [999])

        assert result is None

    def test_connection_returned_to_pool(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn(rows=[])
        pool.getconn.return_value = mock_conn

        wrapper.fetch_one("SELECT 1")

        pool.putconn.assert_called_once_with(mock_conn)

    def test_rollback_called_after_select(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn(rows=[{"v": 1}])
        pool.getconn.return_value = mock_conn

        wrapper.fetch_one("SELECT 1")

        mock_conn.rollback.assert_called_once()


# ---------------------------------------------------------------------------
# fetch_all
# ---------------------------------------------------------------------------

class TestFetchAll:
    def test_returns_all_rows(self):
        rows = [{"id": 1}, {"id": 2}]
        wrapper, pool = _make_wrapper()
        mock_conn, mock_cursor = _mock_conn(rows=rows)
        pool.getconn.return_value = mock_conn

        result = wrapper.fetch_all("SELECT * FROM t")

        assert result == rows

    def test_rollback_called_after_select(self):
        wrapper, pool = _make_wrapper()
        mock_conn, _ = _mock_conn(rows=[])
        pool.getconn.return_value = mock_conn

        wrapper.fetch_all("SELECT 1")

        mock_conn.rollback.assert_called_once()

    def test_connection_returned_on_error(self):
        import psycopg2
        wrapper, pool = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.execute.side_effect = psycopg2.OperationalError("db error")
        mock_conn.cursor.return_value = mock_cursor
        pool.getconn.return_value = mock_conn

        with pytest.raises(psycopg2.OperationalError):
            wrapper.fetch_all("BAD SQL")

        pool.putconn.assert_called_once_with(mock_conn)


# ---------------------------------------------------------------------------
# begin_transaction
# ---------------------------------------------------------------------------

class TestTransaction:
    def test_transaction_commits_on_success(self):
        wrapper, pool = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        pool.getconn.return_value = mock_conn

        with wrapper.begin_transaction() as txn:
            txn.execute("INSERT INTO t (v) VALUES (%s)", ["x"])

        mock_conn.commit.assert_called_once()
        pool.putconn.assert_called_once_with(mock_conn)

    def test_transaction_rolls_back_on_exception(self):
        wrapper, pool = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        pool.getconn.return_value = mock_conn

        with pytest.raises(ValueError):
            with wrapper.begin_transaction() as txn:
                txn.execute("INSERT INTO t (v) VALUES (%s)", ["x"])
                raise ValueError("forced error")

        mock_conn.rollback.assert_called_once()
        mock_conn.commit.assert_not_called()
        pool.putconn.assert_called_once_with(mock_conn)

    def test_transaction_fetchall(self):
        wrapper, pool = _make_wrapper()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [{"id": 1}]
        mock_conn.cursor.return_value = mock_cursor
        pool.getconn.return_value = mock_conn

        with wrapper.begin_transaction() as txn:
            txn.execute("SELECT * FROM t")
            rows = txn.fetchall()

        assert rows == [{"id": 1}]


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestClose:
    def test_close_calls_closeall(self):
        wrapper, pool = _make_wrapper()
        wrapper.close()
        pool.closeall.assert_called_once()