import json
import os
import shutil
from ._prototype import NATIVE_PLACEHOLDER


class SQLoader:

    def __init__(self, dir, db_type=None, placeholder=None, db=None, async_db=None) -> None:
        self.sql_dir = dir
        self.db_type = db_type
        self.placeholder = self._parse_placeholder(placeholder)
        self.db = db
        self.async_db = async_db

    def set_db(self, db):
        """Inject a synchronous database instance after construction."""
        self.db = db

    def set_async_db(self, async_db):
        """Inject an async database instance after construction."""
        self.async_db = async_db

    def _parse_placeholder(self, placeholder):
        """
        Parse the placeholder setting into a list.

        Supported formats:
            - None:              no conversion
            - single str:        "%s"   -> ["%s"]
            - comma-separated:   "?,%s" -> ["?", "%s"]
            - list:              ["?", "%s"] -> returned as-is
        """
        if placeholder is None:
            return None

        if isinstance(placeholder, list):
            return [p.strip() for p in placeholder]

        if isinstance(placeholder, str):
            if "," in placeholder:
                return [p.strip().strip("'\"") for p in placeholder.split(",")]
            else:
                return [placeholder.strip()]

        return None

    def _convert_placeholder(self, query):
        """
        Replace generic placeholders in the query string with the DB-native placeholder.
        """
        if not self.placeholder or not self.db_type:
            return query

        native = NATIVE_PLACEHOLDER.get(self.db_type)
        if native is None:
            return query

        for p in self.placeholder:
            if p != native:
                query = query.replace(p, native)

        return query

    def check_file_exists(self, file_path):
        return os.path.isfile(file_path)

    def read_json_file(self, file_path):
        if self.check_file_exists(file_path):
            with open(file_path, 'r') as file:
                return json.load(file)
        else:
            raise FileNotFoundError(f"File not found: {file_path}")

    def read_sql_file(self, file_path, encode="utf-8"):
        if self.check_file_exists(file_path):
            with open(file_path, 'r', encoding=encode) as file:
                return file.read()
        else:
            raise FileNotFoundError(f"File not found: {file_path}")

    def deep_get(self, dictionary: dict, dotted_key: str):
        keys = dotted_key.split(".")
        for key in keys:
            if isinstance(dictionary, dict):
                dictionary = dictionary.get(key)
            else:
                return None
        return dictionary

    def load_sql(self, filename: str, query_name: str, encode="utf-8"):
        suffix = ".json"
        if suffix in filename:
            suffix = ""
        file_path = os.path.join(self.sql_dir, f"{filename}{suffix}")
        queries = self.read_json_file(file_path)

        query = self.deep_get(queries, query_name)
        if query is None:
            raise ValueError(f"Query not found: {query_name}")

        if isinstance(query, str) and query.endswith('.sql'):
            query_file_path = os.path.join(self.sql_dir, query)
            query = self.read_sql_file(query_file_path, encode)

        return self._convert_placeholder(query)

    def _require_db(self):
        if self.db is None:
            raise RuntimeError(
                "No database instance attached. Pass db= to SQLoader() or call set_db() first."
            )

    def _require_async_db(self):
        if self.async_db is None:
            raise RuntimeError(
                "No async database instance attached. "
                "Pass async_db= to SQLoader() or call set_async_db() first."
            )

    def execute(self, file: str, query_name: str, params=None):
        self._require_db()
        sql = self.load_sql(file, query_name)
        return self.db.execute(sql, params)

    def fetch_one(self, file: str, query_name: str, params=None):
        self._require_db()
        sql = self.load_sql(file, query_name)
        return self.db.fetch_one(sql, params)

    def fetch_all(self, file: str, query_name: str, params=None):
        self._require_db()
        sql = self.load_sql(file, query_name)
        return self.db.fetch_all(sql, params)

    async def async_execute(self, file: str, query_name: str, params=None):
        self._require_async_db()
        sql = self.load_sql(file, query_name)
        return await self.async_db.execute(sql, params)

    async def async_fetch_one(self, file: str, query_name: str, params=None):
        self._require_async_db()
        sql = self.load_sql(file, query_name)
        return await self.async_db.fetch_one(sql, params)

    async def async_fetch_all(self, file: str, query_name: str, params=None):
        self._require_async_db()
        sql = self.load_sql(file, query_name)
        return await self.async_db.fetch_all(sql, params)

    def sync(self, from_db: str, to_db: str, overwrite: bool = False):
        """
        Copy query files from the from_db directory to the to_db directory.

        :param from_db: Source DB type ("sqlite3", "mysql", "postgresql")
        :param to_db: Target DB type
        :param overwrite: If True, overwrite existing files; if False, skip them (default: False)
        :return: { "copied": [...], "skipped": [...] }
        """
        from_dir = os.path.join(self.sql_dir, from_db)
        to_dir = os.path.join(self.sql_dir, to_db)

        if not os.path.isdir(from_dir):
            raise FileNotFoundError(f"Source directory not found: {from_dir}")

        copied = []
        skipped = []

        for root, _, files in os.walk(from_dir):
            for filename in files:
                if not filename.endswith((".json", ".sql")):
                    continue

                src_path = os.path.join(root, filename)
                rel_path = os.path.relpath(src_path, from_dir)
                dst_path = os.path.join(to_dir, rel_path)

                if os.path.isfile(dst_path) and not overwrite:
                    skipped.append(rel_path)
                    continue

                os.makedirs(os.path.dirname(dst_path), exist_ok=True)
                shutil.copy2(src_path, dst_path)
                copied.append(rel_path)

        return {"copied": copied, "skipped": skipped}
