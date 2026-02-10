import json
import os
from ._prototype import NATIVE_PLACEHOLDER


class SQLoader:

    def __init__(self, dir, db_type=None, placeholder=None) -> None:
        self.sql_dir = dir
        self.db_type = db_type
        self.placeholder = self._parse_placeholder(placeholder)

    def _parse_placeholder(self, placeholder):
        """
        placeholder 설정을 파싱하여 리스트로 반환.

        지원 형식:
            - None: 변환 안 함
            - str 싱글: "%s" → ["%s"]
            - str 콤마 구분: "?,%s" → ["?", "%s"]
            - list: ["?", "%s"] → 그대로
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
        쿼리 문자열의 플레이스홀더를 DB 네이티브로 변환.
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
