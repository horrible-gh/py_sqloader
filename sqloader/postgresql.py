import threading
import time
from contextlib import contextmanager

import psycopg2
import psycopg2.pool
import psycopg2.extras

from ._prototype import DatabasePrototype, Transaction, PoolTimeoutError, POSTGRESQL


class PostgreSQLWrapper(DatabasePrototype):
    db_type = POSTGRESQL
    log_print = False
    external_sql_path = None

    def __init__(self, host, user, password, database, port=5432, log=False,
                 sql_path=None, max_parallel_queries=5, pool_min=1, pool_max=None,
                 acquire_timeout=None, max_lifetime=None, max_idle=None):
        """
        :param max_parallel_queries: how many queries may run concurrently.
                                     This is the backpressure limit -- callers
                                     beyond it wait for a free slot.
        :param pool_min:             connections opened eagerly by the pool
        :param pool_max:             maximum physical connections. Defaults to
                                     max_parallel_queries. Raise it above that
                                     to keep spare connections in reserve.
        :param acquire_timeout:      seconds to wait for a free slot before
                                     raising PoolTimeoutError. None (default)
                                     waits indefinitely.
        :param max_lifetime:         discard a connection older than this many
                                     seconds at checkout. None disables.
        :param max_idle:             discard a connection idle in the pool for
                                     more than this many seconds. None disables.

        psycopg2's pool raises "connection pool exhausted" instead of waiting,
        so the semaphore -- not the pool -- provides the queueing here. Keeping
        pool_max >= max_parallel_queries is therefore what stops that error from
        ever surfacing.
        """
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.log_print = log
        self.external_sql_path = sql_path

        if pool_max is None:
            pool_max = max_parallel_queries
        if pool_max < max_parallel_queries:
            raise ValueError(
                f"pool_max ({pool_max}) must be >= max_parallel_queries "
                f"({max_parallel_queries}); a smaller pool would exhaust before "
                "the concurrency limit is reached."
            )

        self.max_parallel_queries = max_parallel_queries
        self.pool_min = pool_min
        self.pool_max = pool_max
        self.acquire_timeout = acquire_timeout
        self.max_lifetime = max_lifetime
        self.max_idle = max_idle

        # Per-instance, not module-global: two wrappers must not share a limit.
        self.query_semaphore = threading.Semaphore(max_parallel_queries)

        # psycopg2 connections are C objects and reject attribute assignment,
        # so connection ages are tracked here, keyed by id(). Entries are
        # dropped whenever a connection is discarded.
        self._conn_meta = {}
        self._meta_lock = threading.Lock()

        # Threaded, not Simple: SimpleConnectionPool has no locking and this
        # wrapper is used from request handler threads.
        self.pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=pool_min,
            maxconn=pool_max,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def log(self, msg):
        if self.log_print:
            print(msg)

    # -- slot / connection management ---------------------------------------
    def _acquire_slot(self):
        if self.acquire_timeout is None:
            self.query_semaphore.acquire()
            return
        if not self.query_semaphore.acquire(timeout=self.acquire_timeout):
            raise PoolTimeoutError(
                f"Timed out after {self.acquire_timeout}s waiting for a free "
                f"query slot (max_parallel_queries={self.max_parallel_queries})."
            )

    def _is_stale(self, conn):
        """True when a checked-out connection should be dropped rather than used."""
        if conn.closed:
            return True
        with self._meta_lock:
            meta = self._conn_meta.get(id(conn))
        if meta is None:
            return False
        now = time.monotonic()
        if self.max_lifetime is not None and now - meta["created"] > self.max_lifetime:
            return True
        if self.max_idle is not None and meta["returned"] is not None \
                and now - meta["returned"] > self.max_idle:
            return True
        return False

    def _discard(self, conn):
        with self._meta_lock:
            self._conn_meta.pop(id(conn), None)
        try:
            self.pool.putconn(conn, close=True)
        except Exception as e:
            print(f"Discarding stale connection failed: {e}")

    def _checkout(self):
        conn = self.pool.getconn()
        if self._is_stale(conn):
            self._discard(conn)
            conn = self.pool.getconn()
        with self._meta_lock:
            if id(conn) not in self._conn_meta:
                self._conn_meta[id(conn)] = {"created": time.monotonic(), "returned": None}
            self._conn_meta[id(conn)]["returned"] = None
        conn.autocommit = False
        return conn

    def _checkin(self, conn):
        with self._meta_lock:
            meta = self._conn_meta.get(id(conn))
            if meta is not None:
                meta["returned"] = time.monotonic()
        try:
            self.pool.putconn(conn)
        finally:
            # putconn closes the connection instead of pooling it whenever the
            # pool already holds minconn idle ones (psycopg2/pool.py:105-122).
            # Its metadata must go with it: entries are keyed by id(), which
            # CPython reuses once the object is freed, so a stale "created"
            # stamp would otherwise be inherited by an unrelated new connection
            # and make max_lifetime discard it far too early.
            if conn.closed:
                with self._meta_lock:
                    self._conn_meta.pop(id(conn), None)

    @contextmanager
    def _connection(self):
        """Acquire a slot and a connection; always release both."""
        self._acquire_slot()
        conn = None
        try:
            conn = self._checkout()
            yield conn
        finally:
            if conn is not None:
                self._checkin(conn)
            self.query_semaphore.release()

    def _cursor(self, conn):
        return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def _log_query(self, query, params):
        self.log(query)
        if params is not None:
            self.log(params)

    @staticmethod
    def _normalize_params(params):
        if params is not None and not isinstance(params, (tuple, list, dict)):
            return (params,)
        return params

    # -- queries -------------------------------------------------------------
    def execute(self, query, params=None, commit=True):
        with self._connection() as conn:
            try:
                with self._cursor(conn) as cursor:
                    self._log_query(query, params)
                    cursor.execute(query, self._normalize_params(params))
                    if commit:
                        conn.commit()
                    return cursor.rowcount
            except psycopg2.Error as e:
                print(f"Error executing query: {e}")
                print(f"Last query: {query}")
                try:
                    conn.rollback()
                except Exception as ex:
                    print(f"Rollback failed: {ex}")
                raise e

    def execute_query(self, query, params=None, commit=True):
        return self.execute(query, params, commit)

    def fetchone(self, query, params=None):
        return self.fetch_one(query, params)

    def fetch_one(self, query, params=None):
        return self._fetch(query, params, "fetchone")

    def fetchall(self, query, params=None):
        return self.fetch_all(query, params)

    def fetch_all(self, query, params=None):
        return self._fetch(query, params, "fetchall")

    def _fetch(self, query, params, method):
        with self._connection() as conn:
            try:
                with self._cursor(conn) as cursor:
                    self._log_query(query, params)
                    cursor.execute(query, self._normalize_params(params))
                    result = getattr(cursor, method)()
                conn.rollback()  # Close the implicit transaction before returning to pool
                return result
            except psycopg2.Error as e:
                print(f"Error fetching data: {e}")
                print(f"Last query: {query}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                raise e

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        if self.pool:
            self.pool.closeall()
            with self._meta_lock:
                self._conn_meta.clear()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass

    def begin_transaction(self):
        return PostgreSQLTransaction(self)


class PostgreSQLTransaction(Transaction):
    def __init__(self, wrapper: PostgreSQLWrapper):
        self.wrapper = wrapper
        # Transactions take a slot like any other query. Without this they can
        # drain the pool behind the semaphore's back and trigger
        # "connection pool exhausted" under load.
        wrapper._acquire_slot()
        self._slot_held = True
        self._closed = False
        self.conn = None
        self.cursor = None
        try:
            self.conn = wrapper._checkout()
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        except Exception:
            # The slot alone is not enough: if checkout succeeded and only the
            # cursor failed, the connection is already out of the pool and
            # nothing else will ever hand it back.
            if self.conn is not None:
                try:
                    wrapper._checkin(self.conn)
                except Exception as e:
                    print(f"Returning connection after failed transaction start failed: {e}")
                self.conn = None
            self._release_slot()
            raise

    def _release_slot(self):
        if self._slot_held:
            self._slot_held = False
            self.wrapper.query_semaphore.release()

    def execute(self, query, params=None):
        if params is not None and not isinstance(params, (tuple, list, dict)):
            params = (params,)
        return self.cursor.execute(query, params)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetch_all(self, query=None, params=None):
        if query is not None:
            self.execute(query, params)
        return self.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def fetch_one(self, query=None, params=None):
        if query is not None:
            self.execute(query, params)
        return self.fetchone()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        # Idempotent: a second close would putconn the same connection twice
        # and let two callers hold it at once.
        if self._closed:
            return
        self._closed = True
        try:
            if self.cursor is not None:
                self.cursor.close()
        finally:
            # cursor.close() failing must not strand the connection outside the
            # pool while its slot goes back -- that leaves more permits than
            # connections and revives "connection pool exhausted".
            try:
                if self.conn is not None:
                    self.wrapper._checkin(self.conn)
                    self.conn = None
            finally:
                self._release_slot()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        try:
            if exc_type:
                self.rollback()
            else:
                try:
                    self.commit()
                except Exception:
                    # commit can fail (serialization failure, lost server) and
                    # must not skip close(): the connection would never return
                    # to the pool. Roll back first so it goes back idle rather
                    # than inside an aborted transaction.
                    try:
                        self.rollback()
                    except Exception as e:
                        print(f"Rollback after failed commit failed: {e}")
                    raise
        finally:
            self.close()
