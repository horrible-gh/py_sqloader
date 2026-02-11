import asyncpg
from ._async_prototype import AsyncDatabasePrototype, AsyncTransaction
from ._prototype import POSTGRESQL


def _to_asyncpg_query(query: str) -> str:
    """
    Convert %s or ? placeholders to asyncpg's positional $1, $2, ... style.

    Examples:
        "SELECT * FROM t WHERE id = %s AND name = %s" -> "SELECT * FROM t WHERE id = $1 AND name = $2"
        "INSERT INTO t VALUES (?, ?)"                 -> "INSERT INTO t VALUES ($1, $2)"
    """
    count = 0
    parts = []
    i = 0
    while i < len(query):
        if query[i:i+2] == "%s":
            count += 1
            parts.append(f"${count}")
            i += 2
        elif query[i] == "?":
            count += 1
            parts.append(f"${count}")
            i += 1
        else:
            parts.append(query[i])
            i += 1
    return "".join(parts)


class AsyncPostgreSQLWrapper(AsyncDatabasePrototype):
    """
    Async PostgreSQL wrapper backed by asyncpg.

    Because asyncpg.create_pool() is a coroutine, the pool cannot be created
    in __init__.  Use the async factory instead:

        db = await AsyncPostgreSQLWrapper.create(host=..., user=..., ...)

    Or initialise manually:

        db = AsyncPostgreSQLWrapper(host=..., user=..., ...)
        await db.connect()
    """

    db_type = POSTGRESQL
    log_print = False
    external_sql_path = None

    def __init__(self, host, user, password, database, port=5432,
                 log=False, sql_path=None, max_size=5):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.log_print = log
        self.external_sql_path = sql_path
        self.max_size = max_size
        self.pool = None  # created by connect()

    @classmethod
    async def create(cls, host, user, password, database, port=5432,
                     log=False, sql_path=None, max_size=5):
        """Async factory: instantiate and connect the pool in one step."""
        wrapper = cls(host=host, user=user, password=password, database=database,
                      port=port, log=log, sql_path=sql_path, max_size=max_size)
        await wrapper.connect()
        return wrapper

    async def connect(self):
        """Create the asyncpg connection pool."""
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            min_size=1,
            max_size=self.max_size,
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

        asyncpg auto-commits every statement executed outside a transaction,
        so the commit parameter is accepted for API compatibility only.
        For explicit multi-statement transactions use begin_transaction().

        Returns the command-status string from asyncpg (e.g. "INSERT 0 1").
        """
        q = _to_asyncpg_query(query)
        self._log(q)
        if params is not None:
            self._log(params)
        try:
            async with self.pool.acquire() as conn:
                if params is None:
                    return await conn.execute(q)
                elif isinstance(params, (list, tuple)):
                    return await conn.execute(q, *params)
                else:
                    # Dict or other types: asyncpg doesn't support named params,
                    # so this will raise TypeError - which is correct behavior
                    return await conn.execute(q, params)
        except asyncpg.PostgresError as e:
            print(f"Error executing query: {e}")
            print(f"Last query: {q}")
            raise

    async def fetch_one(self, query, params=None):
        """Fetch a single row as a dict, or None if no row matches."""
        q = _to_asyncpg_query(query)
        self._log(q)
        if params is not None:
            self._log(params)
        try:
            async with self.pool.acquire() as conn:
                if params is None:
                    record = await conn.fetchrow(q)
                elif isinstance(params, (list, tuple)):
                    record = await conn.fetchrow(q, *params)
                else:
                    record = await conn.fetchrow(q, params)
                return dict(record) if record is not None else None
        except asyncpg.PostgresError as e:
            print(f"Error fetching data: {e}")
            print(f"Last query: {q}")
            raise

    async def fetch_all(self, query, params=None):
        """Fetch all matching rows as a list of dicts."""
        q = _to_asyncpg_query(query)
        self._log(q)
        if params is not None:
            self._log(params)
        try:
            async with self.pool.acquire() as conn:
                if params is None:
                    records = await conn.fetch(q)
                elif isinstance(params, (list, tuple)):
                    records = await conn.fetch(q, *params)
                else:
                    records = await conn.fetch(q, params)
                return [dict(r) for r in records]
        except asyncpg.PostgresError as e:
            print(f"Error fetching data: {e}")
            print(f"Last query: {q}")
            raise

    async def commit(self):
        # asyncpg auto-commits outside transactions; no-op
        pass

    async def rollback(self):
        # asyncpg handles rollback inside transaction context; no-op here
        pass

    async def close(self):
        """Close the connection pool and wait for all connections to be released."""
        if self.pool:
            await self.pool.close()

    def begin_transaction(self):
        """Return an async transaction context manager.

        Usage:
            async with db.begin_transaction() as txn:
                await txn.execute("INSERT INTO ...")
                rows = await txn.fetchall()
        """
        return AsyncPostgreSQLTransaction(self)


class AsyncPostgreSQLTransaction(AsyncTransaction):
    """
    Async transaction context manager for asyncpg.

    Acquires a connection from the pool in __aenter__, starts an asyncpg
    transaction, and commits or rolls back in __aexit__.

    asyncpg does not use a cursor model: execute() runs the statement on the
    connection and stores the results so that fetchone() / fetchall() can be
    called afterwards, mirroring the sync Transaction API.
    """

    def __init__(self, wrapper: AsyncPostgreSQLWrapper):
        self.wrapper = wrapper
        self._conn = None
        self._txn = None
        self._last_results = []  # stores rows from the most recent SELECT

    async def __aenter__(self):
        self._conn = await self.wrapper.pool.acquire()
        self._txn = self._conn.transaction()
        await self._txn.start()
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        try:
            if exc_type:
                await self._txn.rollback()
            else:
                await self._txn.commit()
        finally:
            await self.wrapper.pool.release(self._conn)

    async def execute(self, query, params=None):
        """
        Execute a query within the transaction.

        For SELECT / WITH queries the rows are stored and accessible via
        fetchone() / fetchall().  For write queries the results list is cleared.
        """
        q = _to_asyncpg_query(query)

        stripped = q.strip().upper()
        if stripped.startswith("SELECT") or stripped.startswith("WITH"):
            if params is None:
                records = await self._conn.fetch(q)
            elif isinstance(params, (list, tuple)):
                records = await self._conn.fetch(q, *params)
            else:
                records = await self._conn.fetch(q, params)
            self._last_results = [dict(r) for r in records]
        else:
            if params is None:
                await self._conn.execute(q)
            elif isinstance(params, (list, tuple)):
                await self._conn.execute(q, *params)
            else:
                await self._conn.execute(q, params)
            self._last_results = []

        return self._last_results

    async def fetchone(self):
        """Return the first row from the last SELECT, or None."""
        return self._last_results[0] if self._last_results else None

    async def fetchall(self):
        """Return all rows from the last SELECT."""
        return self._last_results

    async def commit(self):
        await self._txn.commit()
        # Start a new transaction so the connection remains usable
        self._txn = self._conn.transaction()
        await self._txn.start()

    async def rollback(self):
        await self._txn.rollback()
        # Start a new transaction so the connection remains usable
        self._txn = self._conn.transaction()
        await self._txn.start()

    async def close(self):
        await self.wrapper.pool.release(self._conn)