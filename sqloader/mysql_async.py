import aiomysql
from ._async_prototype import AsyncDatabasePrototype, AsyncTransaction
from ._prototype import MYSQL


class AsyncMySqlWrapper(AsyncDatabasePrototype):
    """
    Async MySQL wrapper backed by aiomysql connection pool.

    Because aiomysql.create_pool() is a coroutine, the pool cannot be created
    in __init__.  Use the async factory instead:

        db = await AsyncMySqlWrapper.create(host=..., user=..., ...)

    Or initialise manually:

        db = AsyncMySqlWrapper(host=..., user=..., ...)
        await db.connect()
    """

    db_type = MYSQL
    log_print = False
    external_sql_path = None

    def __init__(self, host, user, password, db, port=3306,
                 log=False, sql_path=None, max_size=5):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.log_print = log
        self.external_sql_path = sql_path
        self.max_size = max_size
        self.pool = None  # created by connect()

    @classmethod
    async def create(cls, host, user, password, db, port=3306,
                     log=False, sql_path=None, max_size=5):
        """Async factory: instantiate and connect the pool in one step."""
        wrapper = cls(host=host, user=user, password=password, db=db,
                      port=port, log=log, sql_path=sql_path, max_size=max_size)
        await wrapper.connect()
        return wrapper

    async def connect(self):
        """Create the aiomysql connection pool."""
        self.pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.db,
            minsize=1,
            maxsize=self.max_size,
            cursorclass=aiomysql.DictCursor,
            autocommit=False,
        )

    async def reconnect(self):
        """Close the current pool and create a fresh one."""
        await self.close()
        await self.connect()

    def _log(self, msg):
        if self.log_print:
            print(msg)

    async def execute(self, query, params=None, commit=True):
        """
        Execute a write query (INSERT / UPDATE / DELETE).

        Acquires a connection from the pool, runs the query, and commits
        if commit=True (default).  Returns cursor.lastrowid for INSERT
        statements; returns cursor.rowcount otherwise.
        """
        self._log(query)
        if params is not None:
            self._log(params)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    if commit:
                        await conn.commit()
                    return cursor.lastrowid
        except Exception as e:
            print(f"Error executing query: {e}")
            print(f"Last query: {query}")
            raise

    async def fetch_one(self, query, params=None):
        """Fetch a single row as a dict, or None if no row matches."""
        self._log(query)
        if params is not None:
            self._log(params)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    return await cursor.fetchone()
        except Exception as e:
            print(f"Error fetching data: {e}")
            print(f"Last query: {query}")
            raise

    async def fetch_all(self, query, params=None):
        """Fetch all matching rows as a list of dicts."""
        self._log(query)
        if params is not None:
            self._log(params)
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, params)
                    return await cursor.fetchall()
        except Exception as e:
            print(f"Error fetching data: {e}")
            print(f"Last query: {query}")
            raise

    async def commit(self):
        # Committed per-execute(); no-op here
        pass

    async def rollback(self):
        # Rollback is handled inside transaction context; no-op here
        pass

    async def close(self):
        """Close the connection pool and wait for all connections to be released."""
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

    def begin_transaction(self):
        """Return an async transaction context manager.

        Usage:
            async with db.begin_transaction() as txn:
                await txn.execute("INSERT INTO ...")
                rows = await txn.fetchall()
        """
        return AsyncMySqlTransaction(self)


class AsyncMySqlTransaction(AsyncTransaction):
    """
    Async transaction context manager for aiomysql.

    Acquires a dedicated connection from the pool in __aenter__, begins an
    explicit transaction, and commits or rolls back in __aexit__.

    The cursor stays open for the duration of the transaction so that
    fetchone() / fetchall() work correctly after execute().
    """

    def __init__(self, wrapper: AsyncMySqlWrapper):
        self.wrapper = wrapper
        self._conn = None
        self._cursor = None

    async def __aenter__(self):
        self._conn = await self.wrapper.pool.acquire()
        await self._conn.begin()
        self._cursor = await self._conn.cursor()
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        try:
            if exc_type:
                await self._conn.rollback()
            else:
                await self._conn.commit()
        finally:
            await self._cursor.close()
            await self.wrapper.pool.release(self._conn)

    async def execute(self, query, params=None):
        """Execute a query within the transaction."""
        await self._cursor.execute(query, params)
        return self._cursor.rowcount

    async def fetchone(self):
        """Return the next row from the last executed query, or None."""
        return await self._cursor.fetchone()

    async def fetchall(self):
        """Return all remaining rows from the last executed query."""
        return await self._cursor.fetchall()

    async def commit(self):
        await self._conn.commit()
        # Restart transaction so the connection remains usable
        await self._conn.begin()

    async def rollback(self):
        await self._conn.rollback()
        # Restart transaction so the connection remains usable
        await self._conn.begin()

    async def close(self):
        await self._cursor.close()
        await self.wrapper.pool.release(self._conn)