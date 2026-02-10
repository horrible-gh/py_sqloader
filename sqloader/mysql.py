import pymysql
import os
import threading
from ._prototype import DatabasePrototype, Transaction, MYSQL
from pymysql.cursors import DictCursor
from time import sleep

query_semaphore = None

class MySqlWrapper(DatabasePrototype):
    db_type = MYSQL
    log_print = False
    external_sql_path = None

    def __init__(self, host, user, password, db, port=3306, log=False, keep_alive_interval=-1, sql_path=None, max_parallel_queries=5):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.port = port
        self.log_print = log
        self.external_sql_path = sql_path

        global query_semaphore
        query_semaphore = threading.Semaphore(max_parallel_queries)

        # Kept for backwards compatibility; not used in execute_query
        self.connect()

        if keep_alive_interval > 0:
            self.keep_alive_interval = keep_alive_interval
            self.keep_alive_thread = threading.Thread(target=self.keep_alive, daemon=True)
            self.keep_alive_thread.start()

    def connect(self):
        """Kept for backwards compatibility; execute_query opens its own connection."""
        self.conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            db=self.db,
            port=self.port,
            cursorclass=DictCursor
        )
        self.cursor = self.conn.cursor()

    def reconnect(self):
        """Kept for backwards compatibility; less meaningful with per-query connections."""
        try:
            self.conn.ping(reconnect=True)
        except:
            self.connect()

    def log(self, msg):
        if self.log_print:
            print(msg)

    def normalize_params(self, params):
        if params is None:
            return None
        # Return as-is whether dict or list
        return params


    def execute_query(self, query, params=None, commit=True, retry=1):
        # Limit concurrent queries with semaphore
        query_semaphore.acquire()
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                port=self.port,
                cursorclass=DictCursor
            )
            try:
                with conn.cursor(DictCursor) as cursor:
                    if params is None:
                        self.log(query)
                        result = cursor.execute(query)
                    else:
                        self.log(query)
                        self.log(params)
                        params = self.normalize_params(params)
                        result = cursor.execute(query, params)

                    if commit:
                        conn.commit()
                return result

            except pymysql.MySQLError as e:
                print(f"Error executing query: {e}")
                print(f"Last query: {query}")
                # Attempt rollback
                try:
                    conn.rollback()
                except Exception as ex:
                    print(f"Rollback failed: {ex}")
                # Retry on lost connection (error 2006)
                if e.args[0] == 2006 and retry > 0:
                    print("MySQL server has gone away. Reconnecting and retrying query...")
                    conn.close()
                    return self.execute_query(query, params, commit, retry=retry-1)
                else:
                    raise e
            finally:
                try:
                    conn.close()
                except Exception as ex:
                    print(f"Closing connection failed: {ex}")
        finally:
            query_semaphore.release()


    def execute(self, query, params=None, commit=True):
        return self.execute_query(query, params, commit)

    def fetch_all(self, query, params=None):
        query_semaphore.acquire()
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                port=self.port,
                cursorclass=DictCursor
            )
            with conn.cursor(DictCursor) as cursor:
                params = self.normalize_params(params)
                if params is None:
                    self.log(query)
                    cursor.execute(query)
                else:
                    self.log(query)
                    self.log(params)
                    cursor.execute(query, params)
                return cursor.fetchall()
        except pymysql.MySQLError as e:
            print(f"Error fetching data: {e}")
            print(f"Last query: {query}")
            if e.args[0] == 2006:
                pass  # retry logic possible
            raise e
        finally:
            try:
                conn.close()
            except:
                pass
            query_semaphore.release()

    def fetch_one(self, query, params=None):
        query_semaphore.acquire()
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                db=self.db,
                port=self.port,
                cursorclass=DictCursor
            )
            with conn.cursor(DictCursor) as cursor:
                params = self.normalize_params(params)
                if params is None:
                    self.log(query)
                    cursor.execute(query)
                else:
                    self.log(query)
                    self.log(params)
                    cursor.execute(query, params)
                return cursor.fetchone()
        except pymysql.MySQLError as e:
            print(f"Error fetching data: {e}")
            print(f"Last query: {query}")
            if e.args[0] == 2006:
                pass  # retry logic possible
            raise e
        finally:
            try:
                conn.close()
            except:
                pass
            query_semaphore.release()

    def commit(self):
        # No persistent connection; no-op
        pass

    def rollback(self):
        pass

    def close(self):
        # Close the compatibility connection if it exists
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def load_sql(self, sql_file, directory="."):
        if self.external_sql_path:
            sql_path = f"{self.external_sql_path}/{directory}/{sql_file}"
            if os.path.exists(sql_path):
                with open(sql_path, 'r') as file:
                    return file.read()
            else:
                raise FileNotFoundError(f"File not found: {sql_path}")
        else:
            raise RuntimeError("External sql directory not initialized.")

    def keep_alive(self):
        """
        Periodically pings the compatibility connection.
        Has little effect in the per-query connection model.
        """
        while True:
            sleep(self.keep_alive_interval)
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
            except pymysql.MySQLError as e:
                print(f"Keep-alive query failed: {e}")
                if e.args[0] == 2006:
                    print("Reconnecting to the database for keep-alive...")
                    self.reconnect()

    def begin_transaction(self):
        """Returns a transaction context manager backed by a dedicated connection."""
        return MySqlTransaction(self)

class MySqlTransaction(Transaction):
    def __init__(self, wrapper: MySqlWrapper):
        self.wrapper = wrapper
        self.conn = pymysql.connect(
            host=wrapper.host,
            user=wrapper.user,
            password=wrapper.password,
            db=wrapper.db,
            port=wrapper.port,
            cursorclass=DictCursor
        )
        self.cursor = self.conn.cursor()

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