import sqlite3
import threading
from ._prototype import DatabasePrototype, Transaction, SQLITE
from pathlib import Path

query_semaphore = None

db_lock = threading.Lock()

class SQLiteWrapper(DatabasePrototype):
    db_type = SQLITE

    def __init__(self, db_name, memory_mode=False, max_parallel_queries=5):
        self.db_name = db_name
        self.memory_mode = memory_mode

        if not memory_mode:
            # Auto-create parent directories for the database file
            Path(self.db_name).parent.mkdir(parents=True, exist_ok=True)

        global query_semaphore
        query_semaphore = threading.Semaphore(max_parallel_queries)

        if self.memory_mode:
            # In-memory mode: single persistent connection + Lock (serialized access)
            self.conn = sqlite3.connect(":memory:", check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            self.cursor = self.conn.cursor()
            # To restore from a file, use:
            # backup_conn = sqlite3.connect(db_name)
            # backup_conn.backup(self.conn)
            # backup_conn.close()
        else:
            # File mode: new connection per query via semaphore; no persistent conn needed
            self.conn = None
            self.cursor = None

    def _execute_memory(self, query, params=None, commit=True):
        """In-memory mode: single connection + Lock (serialized)."""
        with db_lock:
            try:
                if params is None:
                    self.cursor.execute(query)
                else:
                    self.cursor.execute(query, params)
                if commit:
                    self.conn.commit()
                return self.cursor.lastrowid
            except sqlite3.DatabaseError as e:
                print(f"Error executing query: {e}")
                self.conn.rollback()
                raise e

    def _execute_file(self, query, params=None, commit=True):
        """File mode: semaphore + new connection per query (limited parallelism)."""
        query_semaphore.acquire()
        try:
            conn = sqlite3.connect(self.db_name, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try:
                if params is None:
                    cursor.execute(query)
                else:
                    cursor.execute(query, params)
                if commit:
                    conn.commit()
                return cursor.lastrowid
            except sqlite3.DatabaseError as e:
                print(f"Error executing query (file mode): {e}")
                conn.rollback()
                raise e
            finally:
                cursor.close()
                conn.close()
        finally:
            query_semaphore.release()

    def execute(self, query, params=None, commit=True):
        if self.memory_mode:
            return self._execute_memory(query, params, commit)
        else:
            return self._execute_file(query, params, commit)

    def fetch_one(self, query, params=None):
        if self.memory_mode:
            with db_lock:
                try:
                    if params is None:
                        self.cursor.execute(query)
                    else:
                        self.cursor.execute(query, params)
                    return self.cursor.fetchone()
                except sqlite3.DatabaseError as e:
                    print(f"Error fetching data (memory mode, fetch_one): {e}")
                    raise e
        else:
            # File mode: open a new connection for the fetch
            query_semaphore.acquire()
            try:
                conn = sqlite3.connect(self.db_name, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                try:
                    if params is None:
                        cursor.execute(query)
                    else:
                        cursor.execute(query, params)
                    return cursor.fetchone()
                except sqlite3.DatabaseError as e:
                    print(f"Error fetching data (file mode, fetch_one): {e}")
                    raise e
                finally:
                    cursor.close()
                    conn.close()
            finally:
                query_semaphore.release()

    def fetch_all(self, query, params=None):
        if self.memory_mode:
            with db_lock:
                try:
                    if params is None:
                        self.cursor.execute(query)
                    else:
                        self.cursor.execute(query, params)
                    return self.cursor.fetchall()
                except sqlite3.DatabaseError as e:
                    print(f"Error fetching data (memory mode, fetch_all): {e}")
                    raise e
        else:
            query_semaphore.acquire()
            try:
                conn = sqlite3.connect(self.db_name, check_same_thread=False)
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                try:
                    if params is None:
                        cursor.execute(query)
                    else:
                        cursor.execute(query, params)
                    return cursor.fetchall()
                except sqlite3.DatabaseError as e:
                    print(f"Error fetching data (file mode, fetch_all): {e}")
                    raise e
                finally:
                    cursor.close()
                    conn.close()
            finally:
                query_semaphore.release()

    def rollback(self):
        if self.memory_mode:
            with db_lock:
                self.conn.rollback()
        else:
            # File mode uses per-query connections; no persistent connection to rollback
            pass

    def commit(self):
        if self.memory_mode:
            with db_lock:
                self.conn.commit()
        else:
            # File mode uses per-query connections; commit is handled inside each call
            pass

    def close(self):
        if self.memory_mode:
            with db_lock:
                self.cursor.close()
                self.conn.close()
        else:
            # File mode has no persistent connection to close
            pass


    def begin_transaction(self):
        """Returns a transaction context manager backed by a dedicated connection."""
        return SQLiteTransaction(self)


class SQLiteTransaction:
    def __init__(self, wrapper: SQLiteWrapper):
        self.wrapper = wrapper
        self.conn = sqlite3.connect(
            wrapper.db_name,
            check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def execute(self, query, params=None):
        if params is None:
            return self.cursor.execute(query)
        return self.cursor.execute(query, params)

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        # Rollback on exception, commit otherwise
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()