import os

from ._prototype import SQLITE, MYSQL, POSTGRESQL, NATIVE_PLACEHOLDER


class AsyncDatabasePrototype:
    """
    Async counterpart of DatabasePrototype.
    All I/O methods are async def; method names match the sync version exactly.
    """
    db_type = ""

    async def connect(self):
        pass

    async def reconnect(self):
        pass

    async def execute(self, query, params=None, commit=True):
        pass

    async def execute_query(self, query, params=None, commit=True):
        pass

    async def commit(self):
        pass

    async def fetch_one(self, query, params=None):
        pass

    async def fetch_all(self, query, params=None):
        pass

    async def close(self):
        pass

    # escape_string is CPU-only, no I/O — kept as a regular static method
    @staticmethod
    def escape_string(value):
        if isinstance(value, str):
            replacements = {
                "'": "''",
                "--": "––",
                ";": "；",
                "\\": "\\\\",
                "%": "\\%",
                "_": "\\_",
            }
            for old, new in replacements.items():
                value = value.replace(old, new)
        return value

    async def keep_alive(self):
        pass

    async def rollback(self):
        pass

    def set_sql_path(self, sql_path):
        self.external_sql_path = sql_path

    def load_sql(self, sql_file, directory="."):
        """SQL file loading is synchronous (filesystem read; no network I/O)."""
        if self.external_sql_path:
            sql_path = f"{self.external_sql_path}/{directory}/{sql_file}"
            if os.path.exists(sql_path):
                with open(sql_path, "r") as f:
                    return f.read()
            else:
                raise FileNotFoundError(f"File not found: {sql_path}")
        else:
            raise RuntimeError("External sql directory not initialized.")

    async def begin_transaction(self):
        pass


class AsyncTransaction:
    """
    Async counterpart of Transaction.
    Supports async with via __aenter__ / __aexit__.
    Method names match the sync Transaction exactly.
    """

    def __init__(self, wrapper):
        pass

    async def execute(self, query, params=None):
        pass

    async def fetchall(self):
        pass

    async def fetchone(self):
        pass

    async def commit(self):
        pass

    async def rollback(self):
        pass

    async def close(self):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, traceback):
        pass