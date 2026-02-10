"""
Integration tests for SQLiteWrapper.
- memory_mode=True  : execute / fetch_one / fetch_all (fast, no file I/O)
- file mode (tmpfile): transaction tests — each SQLiteTransaction opens a new
  connection to db_name, so :memory: would create a separate empty database.
No external dependencies required.
"""
import os
import pytest
import tempfile
from sqloader.sqlite3 import SQLiteWrapper


@pytest.fixture
def db():
    """In-memory SQLite instance with a fresh schema (for non-transaction tests)."""
    wrapper = SQLiteWrapper(db_name=":memory:", memory_mode=True)
    wrapper.execute(
        "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        commit=True
    )
    yield wrapper
    wrapper.close()


@pytest.fixture
def file_db():
    """File-based SQLite instance with a fresh schema (for transaction tests)."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        wrapper = SQLiteWrapper(db_name=db_path)
        wrapper.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
            commit=True
        )
        yield wrapper
        wrapper.close()
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------

class TestExecute:
    def test_insert_returns_lastrowid(self, db):
        rowid = db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        assert rowid == 1

    def test_insert_multiple_rows(self, db):
        db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        rowid = db.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
        assert rowid == 2

    def test_execute_without_params(self, db):
        db.execute("INSERT INTO users (name) VALUES ('Charlie')")
        rows = db.fetch_all("SELECT * FROM users")
        assert len(rows) == 1

    def test_invalid_query_raises(self, db):
        with pytest.raises(Exception):
            db.execute("INSERT INTO nonexistent_table (col) VALUES (?)", ("x",))


# ---------------------------------------------------------------------------
# fetch_one
# ---------------------------------------------------------------------------

class TestFetchOne:
    def test_returns_dict_like_row(self, db):
        db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        row = db.fetch_one("SELECT * FROM users WHERE name = ?", ("Alice",))
        assert row is not None
        assert row["name"] == "Alice"

    def test_returns_none_when_not_found(self, db):
        row = db.fetch_one("SELECT * FROM users WHERE id = ?", (999,))
        assert row is None

    def test_no_params(self, db):
        db.execute("INSERT INTO users (name) VALUES ('Alice')")
        row = db.fetch_one("SELECT * FROM users")
        assert row["name"] == "Alice"


# ---------------------------------------------------------------------------
# fetch_all
# ---------------------------------------------------------------------------

class TestFetchAll:
    def test_returns_all_rows(self, db):
        db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        db.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
        rows = db.fetch_all("SELECT * FROM users ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_returns_empty_list_when_no_rows(self, db):
        rows = db.fetch_all("SELECT * FROM users")
        assert rows == [] or list(rows) == []

    def test_with_params(self, db):
        db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        db.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
        rows = db.fetch_all("SELECT * FROM users WHERE name = ?", ("Alice",))
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# begin_transaction — commit path
# (uses file_db: SQLiteTransaction opens a new connection to db_name,
#  which would be a separate empty database if db_name were ':memory:')
# ---------------------------------------------------------------------------

class TestTransactionCommit:
    def test_transaction_commits_on_success(self, file_db):
        with file_db.begin_transaction() as txn:
            txn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            txn.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))

        rows = file_db.fetch_all("SELECT * FROM users ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"

    def test_transaction_fetchone_within_txn(self, file_db):
        with file_db.begin_transaction() as txn:
            txn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
            row = txn.fetchone()
        assert row is None  # INSERT has no result set; fetchone returns None


# ---------------------------------------------------------------------------
# begin_transaction — rollback path
# ---------------------------------------------------------------------------

class TestTransactionRollback:
    def test_transaction_rolls_back_on_exception(self, file_db):
        try:
            with file_db.begin_transaction() as txn:
                txn.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
                raise ValueError("forced error")
        except ValueError:
            pass

        rows = file_db.fetch_all("SELECT * FROM users")
        assert list(rows) == []