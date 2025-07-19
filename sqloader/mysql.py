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

        # 기존 self.conn, self.cursor를 제거하거나 사용 안 함
        # connect()와 keep_alive()는 사실상 필요 없어질 수 있음
        # 하지만 혹시 모를 호환을 위해 그대로 둠
        self.connect()

        if keep_alive_interval > 0:
            self.keep_alive_interval = keep_alive_interval
            self.keep_alive_thread = threading.Thread(target=self.keep_alive, daemon=True)
            self.keep_alive_thread.start()

    def connect(self):
        """기존 코드 호환을 위해 남겨둠, 하지만 execute_query에는 사용하지 않음."""
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
        """호환 유지. 다만 '매번 새 커넥션' 방식에서는 큰 의미가 없어집니다."""
        try:
            self.conn.ping(reconnect=True)
        except:
            self.connect()

    def log(self, msg):
        if self.log_print:
            print(msg)

    def normalize_params(self, params):
        """ dict 형태의 params를 list로 변환하는 유틸리티 함수 """
        if params is None:
            return None
        if isinstance(params, dict):
            return list(params.values())  # ✅ dict -> list 변환
        return params  # ✅ 이미 list라면 그대로 반환


    def execute_query(self, query, params=None, commit=True, retry=1):
        # 세마포어로 동시 실행 쿼리 수 제한
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
                        # 파라미터가 dict면 리스트로 변환하는 처리
                        params = self.normalize_params(params)
                        result = cursor.execute(query, params)

                    if commit:
                        conn.commit()
                return result

            except pymysql.MySQLError as e:
                print(f"Error executing query: {e}")
                print(f"Last query: {query}")
                # rollback 시도
                try:
                    conn.rollback()
                except Exception as ex:
                    print(f"Rollback failed: {ex}")
                # 세션 끊김(2006) 에러 발생 시 재시도
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


    # execute()도 동일하게 수정
    def execute(self, query, params=None, commit=True):
        return self.execute_query(query, params, commit)

    # fetch_all, fetch_one도 동일한 패턴으로 새 커넥션 쓰도록
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
                # 파라미터가 dict면 리스트로 변환하는 처리
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
                pass  # 재시도 로직 가능
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
                # 파라미터가 dict면 리스트로 변환하는 처리
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
                pass  # 재시도 로직 가능
            raise e
        finally:
            try:
                conn.close()
            except:
                pass
            query_semaphore.release()

    def commit(self):
        # 더 이상 self.conn을 쓰지 않으므로 의미가 없을 수 있음
        pass

    def rollback(self):
        pass

    def close(self):
        # 기존 self.cursor, self.conn 닫아두기 (호환성)
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
        원래는 self.conn에 주기적으로 ping을 보내는 로직.
        매번 새 커넥션을 쓰는 구조라면 필요 없어질 수 있음.
        일단 기존 코드 유지, 실제로는 큰 효과가 없을 것.
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
        """
        트랜잭션 컨텍스트를 생성하여 반환.
        트랜잭션 내에서는 동일한 커넥션을 사용하므로,
        여러 쿼리를 하나의 트랜잭션으로 묶을 수 있습니다.
        """
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
        return self  # Transaction 객체를 반환

    def __exit__(self, exc_type, exc_val, traceback):
        # 예외 발생 시 rollback, 그렇지 않으면 commit
        if exc_type:
            self.rollback()
        else:
            self.commit()
        self.close()
