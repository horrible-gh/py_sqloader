"""
Tests for fetchone/fetch_one and fetchall/fetch_all aliases.

Covers both sync (SQLiteWrapper / SQLiteTransaction) and
async (AsyncSQLiteWrapper / AsyncSQLiteTransaction) implementations.
All tests use in-memory or temp-file SQLite so no external services are needed.
"""
import os
import asyncio
import pytest
import tempfile

from sqloader.sqlite3 import SQLiteWrapper
from sqloader.sqlite3_async import AsyncSQLiteWrapper


# ---------------------------------------------------------------------------
# Fixtures — sync
# ---------------------------------------------------------------------------

@pytest.fixture
def db():
    wrapper = SQLiteWrapper(db_name=":memory:", memory_mode=True)
    wrapper.execute(
        "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
        commit=True,
    )
    wrapper.execute("INSERT INTO items (name) VALUES (?)", ("alpha",))
    wrapper.execute("INSERT INTO items (name) VALUES (?)", ("beta",))
    yield wrapper
    wrapper.close()


@pytest.fixture
def file_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        wrapper = SQLiteWrapper(db_name=db_path)
        wrapper.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
            commit=True,
        )
        wrapper.execute("INSERT INTO items (name) VALUES (?)", ("alpha",))
        wrapper.execute("INSERT INTO items (name) VALUES (?)", ("beta",))
        yield wrapper
        wrapper.close()
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Fixtures — async
# ---------------------------------------------------------------------------

@pytest.fixture
def async_db():
    async def _make():
        wrapper = await AsyncSQLiteWrapper.create(db_name=":memory:", memory_mode=True)
        await wrapper.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
            commit=True,
        )
        await wrapper.execute("INSERT INTO items (name) VALUES (?)", ("alpha",))
        await wrapper.execute("INSERT INTO items (name) VALUES (?)", ("beta",))
        return wrapper
    return asyncio.run(_make())


@pytest.fixture
def async_file_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    async def _make():
        wrapper = await AsyncSQLiteWrapper.create(db_name=db_path)
        await wrapper.execute(
            "CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)",
            commit=True,
        )
        await wrapper.execute("INSERT INTO items (name) VALUES (?)", ("alpha",))
        await wrapper.execute("INSERT INTO items (name) VALUES (?)", ("beta",))
        return wrapper

    wrapper = asyncio.run(_make())
    yield wrapper, db_path
    asyncio.run(wrapper.close())
    try:
        os.unlink(db_path)
    except OSError:
        pass


# ---------------------------------------------------------------------------
# Sync Wrapper — fetchone alias
# ---------------------------------------------------------------------------

class TestSyncWrapperFetchoneAlias:
    def test_fetchone_returns_row(self, db):
        row = db.fetchone("SELECT * FROM items WHERE name = ?", ("alpha",))
        assert row is not None
        assert row["name"] == "alpha"

    def test_fetchone_returns_none_when_missing(self, db):
        row = db.fetchone("SELECT * FROM items WHERE id = ?", (999,))
        assert row is None

    def test_fetchone_matches_fetch_one(self, db):
        r1 = db.fetchone("SELECT * FROM items WHERE name = ?", ("beta",))
        r2 = db.fetch_one("SELECT * FROM items WHERE name = ?", ("beta",))
        assert r1["name"] == r2["name"]


# ---------------------------------------------------------------------------
# Sync Wrapper — fetchall alias
# ---------------------------------------------------------------------------

class TestSyncWrapperFetchallAlias:
    def test_fetchall_returns_all_rows(self, db):
        rows = db.fetchall("SELECT * FROM items ORDER BY id")
        assert len(rows) == 2
        assert rows[0]["name"] == "alpha"
        assert rows[1]["name"] == "beta"

    def test_fetchall_empty(self, db):
        rows = db.fetchall("SELECT * FROM items WHERE id = ?", (999,))
        assert list(rows) == []

    def test_fetchall_matches_fetch_all(self, db):
        r1 = db.fetchall("SELECT * FROM items ORDER BY id")
        r2 = db.fetch_all("SELECT * FROM items ORDER BY id")
        assert [row["name"] for row in r1] == [row["name"] for row in r2]


# ---------------------------------------------------------------------------
# Sync Transaction — fetch_one / fetch_all aliases
# ---------------------------------------------------------------------------

class TestSyncTransactionAliases:
    def test_transaction_fetch_one(self, file_db):
        with file_db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items WHERE name = ?", ("alpha",))
            row = txn.fetch_one()
        assert row is not None
        assert row["name"] == "alpha"

    def test_transaction_fetch_all(self, file_db):
        with file_db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items ORDER BY id")
            rows = txn.fetch_all()
        assert len(rows) == 2
        assert rows[0]["name"] == "alpha"

    def test_transaction_fetch_one_matches_fetchone(self, file_db):
        with file_db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items WHERE name = ?", ("beta",))
            r1 = txn.fetchone()
        with file_db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items WHERE name = ?", ("beta",))
            r2 = txn.fetch_one()
        assert r1["name"] == r2["name"]

    def test_transaction_fetch_all_matches_fetchall(self, file_db):
        with file_db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items ORDER BY id")
            r1 = txn.fetchall()
        with file_db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items ORDER BY id")
            r2 = txn.fetch_all()
        assert [row["name"] for row in r1] == [row["name"] for row in r2]


# ---------------------------------------------------------------------------
# Async Wrapper — fetchone / fetchall aliases
# ---------------------------------------------------------------------------

def run(coro):
    return asyncio.run(coro)


class TestAsyncWrapperFetchoneAlias:
    def test_fetchone_returns_row(self, async_db):
        row = run(async_db.fetchone("SELECT * FROM items WHERE name = ?", ("alpha",)))
        assert row is not None
        assert row["name"] == "alpha"

    def test_fetchone_returns_none_when_missing(self, async_db):
        row = run(async_db.fetchone("SELECT * FROM items WHERE id = ?", (999,)))
        assert row is None

    def test_fetchone_matches_fetch_one(self, async_db):
        r1 = run(async_db.fetchone("SELECT * FROM items WHERE name = ?", ("beta",)))
        r2 = run(async_db.fetch_one("SELECT * FROM items WHERE name = ?", ("beta",)))
        assert r1["name"] == r2["name"]


class TestAsyncWrapperFetchallAlias:
    def test_fetchall_returns_all_rows(self, async_db):
        rows = run(async_db.fetchall("SELECT * FROM items ORDER BY id"))
        assert len(rows) == 2
        assert rows[0]["name"] == "alpha"
        assert rows[1]["name"] == "beta"

    def test_fetchall_empty(self, async_db):
        rows = run(async_db.fetchall("SELECT * FROM items WHERE id = ?", (999,)))
        assert rows == []

    def test_fetchall_matches_fetch_all(self, async_db):
        r1 = run(async_db.fetchall("SELECT * FROM items ORDER BY id"))
        r2 = run(async_db.fetch_all("SELECT * FROM items ORDER BY id"))
        assert [row["name"] for row in r1] == [row["name"] for row in r2]


# ---------------------------------------------------------------------------
# Async Transaction — fetch_one / fetch_all aliases
# ---------------------------------------------------------------------------

class TestAsyncTransactionAliases:
    def test_transaction_fetch_one(self, async_file_db):
        wrapper, _ = async_file_db

        async def _run():
            async with wrapper.begin_transaction() as txn:
                await txn.execute("SELECT * FROM items WHERE name = ?", ("alpha",))
                return await txn.fetch_one()

        row = run(_run())
        assert row is not None
        assert row["name"] == "alpha"

    def test_transaction_fetch_all(self, async_file_db):
        wrapper, _ = async_file_db

        async def _run():
            async with wrapper.begin_transaction() as txn:
                await txn.execute("SELECT * FROM items ORDER BY id")
                return await txn.fetch_all()

        rows = run(_run())
        assert len(rows) == 2
        assert rows[0]["name"] == "alpha"

    def test_transaction_fetch_one_matches_fetchone(self, async_file_db):
        wrapper, _ = async_file_db

        async def _run():
            async with wrapper.begin_transaction() as txn:
                await txn.execute("SELECT * FROM items WHERE name = ?", ("beta",))
                r1 = await txn.fetchone()
            async with wrapper.begin_transaction() as txn:
                await txn.execute("SELECT * FROM items WHERE name = ?", ("beta",))
                r2 = await txn.fetch_one()
            return r1, r2

        r1, r2 = run(_run())
        assert r1["name"] == r2["name"]

    def test_transaction_fetch_all_matches_fetchall(self, async_file_db):
        wrapper, _ = async_file_db

        async def _run():
            async with wrapper.begin_transaction() as txn:
                await txn.execute("SELECT * FROM items ORDER BY id")
                r1 = await txn.fetchall()
            async with wrapper.begin_transaction() as txn:
                await txn.execute("SELECT * FROM items ORDER BY id")
                r2 = await txn.fetch_all()
            return r1, r2

        r1, r2 = run(_run())
        assert [row["name"] for row in r1] == [row["name"] for row in r2]
