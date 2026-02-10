import psycopg2
import psycopg2.pool
import psycopg2.extras
import threading
from ._prototype import DatabasePrototype, Transaction, POSTGRESQL

query_semaphore = None


class PostgreSQLWrapper(DatabasePrototype):
    db_type = POSTGRESQL
    log_print = False
    external_sql_path = None

    def __init__(self, host, user, password, database, port=5432, log=False, sql_path=None, max_parallel_queries=5):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.log_print = log
        self.external_sql_path = sql_path

        global query_semaphore
        query_semaphore = threading.Semaphore(max_parallel_queries)

        self.pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1,
            maxconn=max_parallel_queries,
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def log(self, msg):
        if self.log_print:
            print(msg)

    def execute(self, query, params=None, commit=True):
        query_semaphore.acquire()
        conn = None
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    self.log(query)
                    if params is not None:
                        self.log(params)
                    cursor.execute(query, params)
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
        finally:
            if conn:
                self.pool.putconn(conn)
            query_semaphore.release()

    def fetch_one(self, query, params=None):
        query_semaphore.acquire()
        conn = None
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    self.log(query)
                    if params is not None:
                        self.log(params)
                    cursor.execute(query, params)
                    result = cursor.fetchone()
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
        finally:
            if conn:
                self.pool.putconn(conn)
            query_semaphore.release()

    def fetch_all(self, query, params=None):
        query_semaphore.acquire()
        conn = None
        try:
            conn = self.pool.getconn()
            conn.autocommit = False
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    self.log(query)
                    if params is not None:
                        self.log(params)
                    cursor.execute(query, params)
                    result = cursor.fetchall()
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
        finally:
            if conn:
                self.pool.putconn(conn)
            query_semaphore.release()

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        if self.pool:
            self.pool.closeall()

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
        self.conn = wrapper.pool.getconn()
        self.conn.autocommit = False
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def execute(self, query, params=None):
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
        self.wrapper.pool.putconn(self.conn)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, traceback):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()