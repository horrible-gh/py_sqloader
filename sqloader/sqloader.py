import json
import os


class SQLoader:

    def __init__(self, dir) -> None:
        self.sql_dir = dir

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

    def deep_get(self, dictionary:dict, dotted_key:str):
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
            return self.read_sql_file(query_file_path, encode)
        else:
            return query
