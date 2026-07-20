"""
Unit tests for PostgreSQLWrapper using mocked psycopg2.
No real PostgreSQL connection required.
"""
import time

import pytest
from unittest.mock import MagicMock, patch, call


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wrapper(max_parallel_queries=5):
    """Instantiate PostgreSQLWrapper with a fully mocked connection pool."""
    with patch("sqloader.postgresql.psycopg2.pool.ThreadedConnectionPool") as mock_pool_cls:
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
    mock_conn.closed = 0
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
        with patch("sqloader.postgresql.psycopg2.pool.ThreadedConnectionPool") as mock_cls:
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
        mock_conn.closed = 0
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
        mock_conn.closed = 0
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
        mock_conn.closed = 0
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
        mock_conn.closed = 0
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
        mock_conn.closed = 0
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


# ---------------------------------------------------------------------------
# Pool sizing / concurrency limits
# ---------------------------------------------------------------------------

class TestPoolSizing:
    def test_pool_max_defaults_to_max_parallel_queries(self):
        with patch("sqloader.postgresql.psycopg2.pool.ThreadedConnectionPool") as mock_cls:
            from sqloader.postgresql import PostgreSQLWrapper
            mock_cls.return_value = MagicMock()
            PostgreSQLWrapper(host="h", user="u", password="p", database="d",
                              max_parallel_queries=30)
            assert mock_cls.call_args.kwargs["maxconn"] == 30

    def test_pool_max_can_exceed_query_limit(self):
        """Spare connections may be held in reserve above the concurrency limit."""
        with patch("sqloader.postgresql.psycopg2.pool.ThreadedConnectionPool") as mock_cls:
            from sqloader.postgresql import PostgreSQLWrapper
            mock_cls.return_value = MagicMock()
            PostgreSQLWrapper(host="h", user="u", password="p", database="d",
                              max_parallel_queries=20, pool_max=25, pool_min=5)
            assert mock_cls.call_args.kwargs["maxconn"] == 25
            assert mock_cls.call_args.kwargs["minconn"] == 5

    def test_pool_smaller_than_query_limit_is_rejected(self):
        """A pool below the concurrency limit would exhaust; fail loudly at init."""
        with patch("sqloader.postgresql.psycopg2.pool.ThreadedConnectionPool"):
            from sqloader.postgresql import PostgreSQLWrapper
            with pytest.raises(ValueError, match="pool_max"):
                PostgreSQLWrapper(host="h", user="u", password="p", database="d",
                                  max_parallel_queries=20, pool_max=5)

    def test_semaphore_is_per_instance(self):
        """A second wrapper must not clobber the first one's concurrency limit."""
        first, _ = _make_wrapper(max_parallel_queries=5)
        second, _ = _make_wrapper(max_parallel_queries=30)
        assert first.query_semaphore is not second.query_semaphore
        assert first.max_parallel_queries == 5
        assert second.max_parallel_queries == 30


# ---------------------------------------------------------------------------
# Acquire timeout
# ---------------------------------------------------------------------------

class TestAcquireTimeout:
    def test_times_out_when_no_slot_free(self):
        from sqloader._prototype import PoolTimeoutError
        wrapper, pool = _make_wrapper(max_parallel_queries=1)
        wrapper.acquire_timeout = 0.05
        mock_conn, _ = _mock_conn()
        pool.getconn.return_value = mock_conn

        wrapper.query_semaphore.acquire()  # occupy the only slot
        try:
            with pytest.raises(PoolTimeoutError):
                wrapper.fetch_all("SELECT 1")
        finally:
            wrapper.query_semaphore.release()

    def test_waits_indefinitely_by_default(self):
        """Default stays None so existing callers keep the old blocking behaviour."""
        wrapper, _ = _make_wrapper()
        assert wrapper.acquire_timeout is None

    def test_slot_released_after_timeout(self):
        from sqloader._prototype import PoolTimeoutError
        wrapper, pool = _make_wrapper(max_parallel_queries=1)
        wrapper.acquire_timeout = 0.05
        mock_conn, _ = _mock_conn()
        pool.getconn.return_value = mock_conn

        wrapper.query_semaphore.acquire()
        with pytest.raises(PoolTimeoutError):
            wrapper.fetch_all("SELECT 1")
        wrapper.query_semaphore.release()

        # The failed attempt must not have leaked the slot.
        wrapper.fetch_all("SELECT 1")
        pool.getconn.assert_called_once()


# ---------------------------------------------------------------------------
# Transactions and the concurrency limit
# ---------------------------------------------------------------------------

class TestTransactionSlot:
    def test_transaction_takes_a_slot(self):
        wrapper, pool = _make_wrapper(max_parallel_queries=2)
        mock_conn = MagicMock()
        mock_conn.closed = 0
        pool.getconn.return_value = mock_conn

        before = wrapper.query_semaphore._value
        with wrapper.begin_transaction():
            during = wrapper.query_semaphore._value
        after = wrapper.query_semaphore._value

        assert during == before - 1, "transaction bypassed the concurrency limit"
        assert after == before, "transaction did not release its slot"

    def test_transaction_releases_slot_on_exception(self):
        wrapper, pool = _make_wrapper(max_parallel_queries=2)
        mock_conn = MagicMock()
        mock_conn.closed = 0
        pool.getconn.return_value = mock_conn

        before = wrapper.query_semaphore._value
        with pytest.raises(ValueError):
            with wrapper.begin_transaction():
                raise ValueError("boom")

        assert wrapper.query_semaphore._value == before

    def test_transaction_releases_slot_if_checkout_fails(self):
        wrapper, pool = _make_wrapper(max_parallel_queries=2)
        pool.getconn.side_effect = RuntimeError("pool down")

        before = wrapper.query_semaphore._value
        with pytest.raises(RuntimeError):
            wrapper.begin_transaction()

        assert wrapper.query_semaphore._value == before


# ---------------------------------------------------------------------------
# Stale connection recycling
# ---------------------------------------------------------------------------

class TestConnectionRecycling:
    def test_closed_connection_is_discarded(self):
        wrapper, pool = _make_wrapper()
        dead, _ = _mock_conn()
        dead.closed = 1
        fresh, _ = _mock_conn()
        pool.getconn.side_effect = [dead, fresh]

        wrapper.fetch_all("SELECT 1")

        pool.putconn.assert_any_call(dead, close=True)
        pool.putconn.assert_any_call(fresh)

    def test_connection_older_than_max_lifetime_is_discarded(self):
        wrapper, pool = _make_wrapper()
        wrapper.max_lifetime = 60
        old, _ = _mock_conn()
        fresh, _ = _mock_conn()
        pool.getconn.side_effect = [old, fresh]
        # Backdate the connection past its lifetime.
        wrapper._conn_meta[id(old)] = {"created": time.monotonic() - 120, "returned": None}

        wrapper.fetch_all("SELECT 1")

        pool.putconn.assert_any_call(old, close=True)

    def test_connection_idle_beyond_max_idle_is_discarded(self):
        wrapper, pool = _make_wrapper()
        wrapper.max_idle = 30
        idle, _ = _mock_conn()
        fresh, _ = _mock_conn()
        pool.getconn.side_effect = [idle, fresh]
        wrapper._conn_meta[id(idle)] = {
            "created": time.monotonic() - 100,
            "returned": time.monotonic() - 60,
        }

        wrapper.fetch_all("SELECT 1")

        pool.putconn.assert_any_call(idle, close=True)

    def test_healthy_connection_is_reused(self):
        wrapper, pool = _make_wrapper()
        wrapper.max_lifetime = 3600
        wrapper.max_idle = 600
        conn, _ = _mock_conn()
        pool.getconn.return_value = conn

        wrapper.fetch_all("SELECT 1")
        wrapper.fetch_all("SELECT 2")

        assert pool.getconn.call_count == 2
        for call_args in pool.putconn.call_args_list:
            assert call_args == call(conn), "healthy connection should not be closed"

    def test_recycling_disabled_by_default(self):
        wrapper, _ = _make_wrapper()
        assert wrapper.max_lifetime is None
        assert wrapper.max_idle is None