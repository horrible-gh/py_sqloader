import sqlite3
import threading
from ._prototype import DatabasePrototype, Transaction, SQLITE

query_semaphore = None

db_lock = threading.Lock()

class SQLiteWrapper(DatabasePrototype):
    db_type = SQLITE

    def __init__(self, db_name, memory_mode=False, max_parallel_queries=5):
        self.db_name = db_name
        self.memory_mode = memory_mode

        global query_semaphore
        query_semaphore = threading.Semaphore(max_parallel_queries)

        if self.memory_mode:
            # 인메모리: 단일 커넥션 + Lock
            self.conn = sqlite3.connect(":memory:", check_same_thread=False)
            self.cursor = self.conn.cursor()
            # 만약 파일에서 데이터를 복사해야 한다면, 필요에 따라 아래 로직 사용
            # backup_conn = sqlite3.connect(db_name)
            # backup_conn.backup(self.conn)
            # backup_conn.close()
        else:
            # 파일 모드: 나중에 세마포어 방식으로 새 커넥션을 여므로,
            # 여기서 굳이 conn/cursor를 만들지 않아도 됩니다.
            self.conn = None
            self.cursor = None

    def _execute_memory(self, query, params=None, commit=True):
        """인메모리 모드 - 단일 커넥션 + Lock(직렬화)."""
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
        """파일 모드 - 세마포어 + 새 커넥션(병렬 제한)."""
        query_semaphore.acquire()
        try:
            conn = sqlite3.connect(self.db_name, check_same_thread=False)
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
            # 파일 모드: 별도 새 커넥션으로 fetch
            query_semaphore.acquire()
            try:
                conn = sqlite3.connect(self.db_name, check_same_thread=False)
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
            # 파일 모드는 매번 새 커넥션을 쓰므로
            # rollback할 '지속 커넥션'이 없습니다
            pass

    def commit(self):
        if self.memory_mode:
            with db_lock:
                self.conn.commit()
        else:
            # 파일 모드는 매번 새 커넥션이므로 여기서 commit할 일이 없음
            pass

    def close(self):
        if self.memory_mode:
            with db_lock:
                self.cursor.close()
                self.conn.close()
        else:
            # 파일모드에선 conn이 따로 없으니 무처리
            pass


    def begin_transaction(self):
        """
        트랜잭션 컨텍스트를 생성하여 반환.
        트랜잭션 내에서는 동일한 커넥션을 사용하므로,
        여러 쿼리를 하나의 트랜잭션으로 묶을 수 있습니다.
        """
        return SQLiteTransaction(self)


class SQLiteTransaction:
    def __init__(self, wrapper: SQLiteWrapper):
        self.wrapper = wrapper
        self.conn = sqlite3.connect(
            wrapper.db_name,
            check_same_thread=False
        )
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
        return self  # 컨텍스트 매니저 지원

    def __exit__(self, exc_type, exc_val, traceback):
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()
