"""
Async integration tests for AsyncSQLiteWrapper.
Tests cover:
- execute / fetch_one / fetch_all in both memory and file modes
- RETURNING clause compatibility (Python 3.13+ / SQLite 3.35+)
- Regression: plain INSERT/UPDATE without RETURNING still works
"""
import os
import pytest
import tempfile
import pytest_asyncio
from sqloader.sqlite3_async import AsyncSQLiteWrapper

pytestmark = pytest.mark.asyncio

CREATE_TABLE = (
    "CREATE TABLE users "
    "(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
)


@pytest_asyncio.fixture
async def db():
    """In-memory AsyncSQLiteWrapper with a fresh schema."""
    wrapper = await AsyncSQLiteWrapper.create(db_name=":memory:", memory_mode=True)
    await wrapper.execute(CREATE_TABLE)
    yield wrapper
    await wrapper.close()


@pytest_asyncio.fixture
async def file_db():
    """File-based AsyncSQLiteWrapper with a fresh schema."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        wrapper = await AsyncSQLiteWrapper.create(db_name=db_path)
        await wrapper.execute(CREATE_TABLE)
        yield wrapper
        await wrapper.close()
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# execute
# ---------------------------------------------------------------------------

class TestAsyncExecute:
    async def test_insert_returns_lastrowid(self, db):
        rowid = await db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        assert rowid == 1

    async def test_insert_multiple_rows(self, db):
        await db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        rowid = await db.execute("INSERT INTO users (name) VALUES (?)", ("Bob",))
        assert rowid == 2

    async def test_execute_without_params(self, db):
        await db.execute("INSERT INTO users (name) VALUES ('Charlie')")
        rows = await db.fetch_all("SELECT * FROM users")
        assert len(rows) == 1


# ---------------------------------------------------------------------------
# RETURNING clause compatibility (Python 3.13+ / SQLite 3.35+)
# ---------------------------------------------------------------------------

class TestAsyncReturning:
    async def test_memory_insert_returning_no_exception(self, db):
        """RETURNING on memory-mode execute must not raise OperationalError."""
        rowid = await db.execute(
            "INSERT INTO users (name) VALUES (?) RETURNING id, name",
            ("ReturningUser",)
        )
        assert rowid is not None or rowid == 0

    async def test_memory_insert_returning_row_persisted(self, db):
        """Row inserted via RETURNING query is actually committed (memory mode)."""
        await db.execute(
            "INSERT INTO users (name) VALUES (?) RETURNING id, name",
            ("Persisted",)
        )
        row = await db.fetch_one("SELECT * FROM users WHERE name = ?", ("Persisted",))
        assert row is not None
        assert row["name"] == "Persisted"

    async def test_file_insert_returning_no_exception(self, file_db):
        """RETURNING on file-mode execute must not raise OperationalError."""
        rowid = await file_db.execute(
            "INSERT INTO users (name) VALUES (?) RETURNING id, name",
            ("FileReturning",)
        )
        assert rowid is not None or rowid == 0

    async def test_file_insert_returning_row_persisted(self, file_db):
        """Row inserted via RETURNING query is actually committed in file mode."""
        await file_db.execute(
            "INSERT INTO users (name) VALUES (?) RETURNING id, name",
            ("FilePersisted",)
        )
        row = await file_db.fetch_one(
            "SELECT * FROM users WHERE name = ?", ("FilePersisted",)
        )
        assert row is not None
        assert row["name"] == "FilePersisted"

    async def test_memory_insert_without_returning_still_works(self, db):
        """Regression: plain INSERT (no RETURNING) continues to work in memory mode."""
        rowid = await db.execute("INSERT INTO users (name) VALUES (?)", ("Plain",))
        assert rowid == 1
        row = await db.fetch_one("SELECT * FROM users WHERE id = ?", (rowid,))
        assert row["name"] == "Plain"

    async def test_file_insert_without_returning_still_works(self, file_db):
        """Regression: plain INSERT (no RETURNING) continues to work in file mode."""
        rowid = await file_db.execute("INSERT INTO users (name) VALUES (?)", ("FilePlain",))
        assert rowid == 1
        row = await file_db.fetch_one("SELECT * FROM users WHERE id = ?", (rowid,))
        assert row["name"] == "FilePlain"

    async def test_memory_update_returning_no_exception(self, db):
        """RETURNING on UPDATE must not raise OperationalError in memory mode."""
        await db.execute("INSERT INTO users (name) VALUES (?)", ("OldName",))
        await db.execute(
            "UPDATE users SET name = ? WHERE name = ? RETURNING id",
            ("NewName", "OldName")
        )
        row = await db.fetch_one("SELECT * FROM users WHERE name = ?", ("NewName",))
        assert row is not None
