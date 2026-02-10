import os

SQLITE = 1
MYSQL = 2
POSTGRESQL = 3

# Native placeholder per database type
NATIVE_PLACEHOLDER = {
    MYSQL: "%s",
    SQLITE: "?",
    POSTGRESQL: "%s",
}


class DatabasePrototype:
    db_type = ""

    def connect(self):
        pass

    def reconnect(self):
        pass

    def execute(self, query, params=None, commit=True):
        pass

    def execute_query(self, query, params=None, commit=True):
        pass

    def commit(self):
        pass

    def fetch_one(self, query, params=None):
        pass

    def fetch_all(self, query, params=None):
        pass

    def close(self):
        pass

    def escape_string(value):
        if isinstance(value, str):
            replacements = {
                "'": "''",
                "--": "––",
                ";": "；",
                "\\": "\\\\",
                "%": "\\%",
                "_": "\\_"
            }

            for old, new in replacements.items():
                value = value.replace(old, new)

        return value

    def keep_alive(self, interval=600):
        pass

    def rollback(self):
        pass

    def set_sql_path(self, sql_path):
        self.external_sql_path = sql_path

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
        pass

    def begin_transaction(self):
        pass


class Transaction:
    def __init__(self, wrapper):
        pass

    def execute(self, query, params=None):
        pass

    def fetchall(self):
        pass

    def fetchone(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, traceback):
        pass
