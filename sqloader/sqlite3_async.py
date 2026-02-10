import asyncio
import sqlite3
import aiosqlite
from pathlib import Path
from ._async_prototype import AsyncDatabasePrototype, AsyncTransaction
from ._prototype import SQLITE


class AsyncSQLiteWrapper(AsyncDatabasePrototype):
    """
    Async SQLite wrapper backed by aiosqlite.

    Because aiosqlite.connect() is a coroutine, the connection cannot be
    opened in __init__.  Use the async factory instead:

        db = await AsyncSQLiteWrapper.create(db_name="mydb.sqlite")

    Or initialise manually:

        db = AsyncSQLiteWrapper(db_name="mydb.sqlite")
        await db.connect()

    Memory mode vs file mode
    ------------------------
    memory_mode=True  : a single persistent :memory: connection + asyncio.Lock
                        (fast; all operations share one in-process database)
    memory_mode=False : a single persistent file connection + asyncio.Lock
                        (suitable for multi-operation workloads)

    Note: AsyncSQLiteTransaction always opens a *new* connection to db_name.
    For memory-mode databases each new connection creates a separate empty
    database, so transactions should only be used with file-mode databases.
    """

    db_type = SQLITE
    log_print = False
    external_sql_path = None

    def __init__(self, db_name, memory_mode=False, log=False, sql_path=None):
        self.db_name = db_name
        self.memory_mode = memory_mode
        self.log_print = log
        self.external_sql_path = sql_path
        self._conn = None
        self._lock = asyncio.Lock()

        if not memory_mode and db_name != ":memory:":
            Path(db_name).parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    async def create(cls, db_name, memory_mode=False, log=False, sql_path=None):
        """Async factory: instantiate and open the connection in one step."""
        wrapper = cls(db_name=db_name, memory_mode=memory_mode,
                      log=log, sql_path=sql_path)
        await wrapper.connect()
        return wrapper

    async def connect(self):
        """Open the aiosqlite connection and set the row factory."""
        target = ":memory:" if self.memory_mode else self.db_name
        self._conn = await aiosqlite.connect(target)
        self._conn.row_factory = sqlite3.Row

    async def reconnect(self):
        """Close the current connection and open a fresh one."""
        await self.close()
        await self.connect()

    def _log(self, msg):
        if self.log_print:
            print(msg)

    async def execute(self, query, params=None, commit=True):
        """
        Execute a write query (INSERT / UPDATE / DELETE).

        Returns cursor.lastrowid (useful for INSERT with AUTOINCREMENT).
        Rolls back automatically on error.
        """
        self._log(query)
        if params is not None:
            self._log(params)
        async with self._lock:
            try:
                if params is None:
                    cursor = await self._conn.execute(query)
                else:
                    cursor = await self._conn.execute(query, params)
                if commit:
                    await self._conn.commit()
                return cursor.lastrowid
            except Exception as e:
                print(f"Error executing query: {e}")
                await self._conn.rollback()
                raise

    async def fetch_one(self, query, params=None):
        """Fetch a single row as a sqlite3.Row (supports dict-style access), or None."""
        self._log(query)
        if params is not None:
            self._log(params)
        async with self._lock:
            try:
                if params is None:
                    cursor = await self._conn.execute(query)
                else:
                    cursor = await self._conn.execute(query, params)
                return await cursor.fetchone()
            except Exception as e:
                print(f"Error fetching data: {e}")
                raise

    async def fetch_all(self, query, params=None):
        """Fetch all matching rows as a list of sqlite3.Row objects."""
        self._log(query)
        if params is not None:
            self._log(params)
        async with self._lock:
            try:
                if params is None:
                    cursor = await self._conn.execute(query)
                else:
                    cursor = await self._conn.execute(query, params)
                return await cursor.fetchall()
            except Exception as e:
                print(f"Error fetching data: {e}")
                raise

    async def commit(self):
        async with self._lock:
            await self._conn.commit()

    async def rollback(self):
        async with self._lock:
            await self._conn.rollback()

    async def close(self):
        """Close the aiosqlite connection."""
        if self._conn:
            await self._conn.close()
            self._conn = None

    def begin_transaction(self):
        """Return an async transaction context manager.

        Usage:
            async with db.begin_transaction() as txn:
                await txn.execute("INSERT INTO ...")
                rows = await txn.fetchall()

        Note: opens a new connection to db_name; only works with file-mode
        databases (memory-mode connections cannot be shared across connections).
        """
        return AsyncSQLiteTransaction(self)


class AsyncSQLiteTransaction(AsyncTransaction):
    """
    Async transaction context manager for aiosqlite.

    Opens a dedicated connection to the database file in __aenter__ and
    commits or rolls back in __aexit__.  The cursor is kept open for the
    duration of the transaction so that fetchone() / fetchall() work
    correctly after execute().

    Only suitable for file-mode databases.
    """

    def __init__(self, wrapper: AsyncSQLiteWrapper):
        self.wrapper = wrapper
        self._conn = None
        self._cursor = None

    async def __aenter__(self):
        self._conn = await aiosqlite.connect(self.wrapper.db_name)
        self._conn.row_factory = sqlite3.Row
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        try:
            if exc_type:
                await self._conn.rollback()
            else:
                await self._conn.commit()
        finally:
            await self._conn.close()

    async def execute(self, query, params=None):
        """Execute a query within the transaction; stores the cursor for fetch calls."""
        if params is None:
            self._cursor = await self._conn.execute(query)
        else:
            self._cursor = await self._conn.execute(query, params)
        return self._cursor.rowcount

    async def fetchone(self):
        """Return the first row from the last executed query, or None."""
        if self._cursor is None:
            return None
        return await self._cursor.fetchone()

    async def fetchall(self):
        """Return all rows from the last executed query."""
        if self._cursor is None:
            return []
        return await self._cursor.fetchall()

    async def commit(self):
        await self._conn.commit()

    async def rollback(self):
        await self._conn.rollback()

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None