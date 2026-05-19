import sys
import os
import types
import unittest
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Stub pymysql so the package imports cleanly without the optional dependency
if "pymysql" not in sys.modules:
    _pymysql = types.ModuleType("pymysql")
    _cursors = types.ModuleType("pymysql.cursors")
    _cursors.DictCursor = None
    _pymysql.cursors = _cursors
    sys.modules["pymysql"] = _pymysql
    sys.modules["pymysql.cursors"] = _cursors

# Stub sqlparse (used by migrator) to avoid requiring the optional dependency
if "sqlparse" not in sys.modules:
    sys.modules["sqlparse"] = types.ModuleType("sqlparse")

from sqloader.sqlite3 import SQLiteWrapper


class TestSQLiteTransactionFetchAPI(unittest.TestCase):
    """Verify fetch_one / fetch_all work inside a transaction (file-mode DB).

    Note: SQLiteTransaction opens its own connection.  In-memory databases
    are per-connection, so the transaction would see an empty DB.  We use a
    temporary file so both the main wrapper and the transaction share the
    same on-disk data.
    """

    def setUp(self):
        self._tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._tmp.close()
        self.db = SQLiteWrapper(self._tmp.name)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS items "
            "(id INTEGER PRIMARY KEY, name TEXT, value INTEGER)"
        )
        self.db.execute("INSERT INTO items (name, value) VALUES (?, ?)", ("alpha", 1))
        self.db.execute("INSERT INTO items (name, value) VALUES (?, ?)", ("beta", 2))
        self.db.execute("INSERT INTO items (name, value) VALUES (?, ?)", ("gamma", 3))

    def tearDown(self):
        self.db.close()
        try:
            os.unlink(self._tmp.name)
        except OSError:
            pass

    # ------------------------------------------------------------------
    # fetch_one
    # ------------------------------------------------------------------
    def test_fetch_one_returns_single_row(self):
        with self.db.begin_transaction() as txn:
            row = txn.fetch_one("SELECT * FROM items WHERE id = ?", [1])
        self.assertIsNotNone(row)
        self.assertEqual(row["name"], "alpha")

    def test_fetch_one_with_no_params(self):
        with self.db.begin_transaction() as txn:
            row = txn.fetch_one("SELECT * FROM items ORDER BY id LIMIT 1")
        self.assertIsNotNone(row)
        self.assertEqual(row["id"], 1)

    def test_fetch_one_returns_none_when_not_found(self):
        with self.db.begin_transaction() as txn:
            row = txn.fetch_one("SELECT * FROM items WHERE id = ?", [999])
        self.assertIsNone(row)

    # ------------------------------------------------------------------
    # fetch_all
    # ------------------------------------------------------------------
    def test_fetch_all_returns_all_rows(self):
        with self.db.begin_transaction() as txn:
            rows = txn.fetch_all("SELECT * FROM items ORDER BY id")
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["name"], "alpha")
        self.assertEqual(rows[2]["name"], "gamma")

    def test_fetch_all_with_params(self):
        with self.db.begin_transaction() as txn:
            rows = txn.fetch_all("SELECT * FROM items WHERE value > ?", [1])
        self.assertEqual(len(rows), 2)

    def test_fetch_all_returns_empty_list_when_not_found(self):
        with self.db.begin_transaction() as txn:
            rows = txn.fetch_all("SELECT * FROM items WHERE value > ?", [100])
        self.assertEqual(rows, [])

    # ------------------------------------------------------------------
    # Regression: legacy DB-API methods still work unchanged
    # ------------------------------------------------------------------
    def test_legacy_fetchone_still_works(self):
        with self.db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items WHERE id = ?", [2])
            row = txn.fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["name"], "beta")

    def test_legacy_fetchall_still_works(self):
        with self.db.begin_transaction() as txn:
            txn.execute("SELECT * FROM items")
            rows = txn.fetchall()
        self.assertEqual(len(rows), 3)

    # ------------------------------------------------------------------
    # Signature parity: main class and transaction must accept same args
    # ------------------------------------------------------------------
    def test_signature_parity_fetch_one(self):
        query = "SELECT * FROM items WHERE id = ?"
        params = [1]
        row_main = self.db.fetch_one(query, params)
        with self.db.begin_transaction() as txn:
            row_txn = txn.fetch_one(query, params)
        self.assertEqual(row_main["id"], row_txn["id"])
        self.assertEqual(row_main["name"], row_txn["name"])

    def test_signature_parity_fetch_all(self):
        query = "SELECT * FROM items ORDER BY id"
        rows_main = self.db.fetch_all(query)
        with self.db.begin_transaction() as txn:
            rows_txn = txn.fetch_all(query)
        self.assertEqual(len(rows_main), len(rows_txn))
        for m, t in zip(rows_main, rows_txn):
            self.assertEqual(m["id"], t["id"])


if __name__ == "__main__":
    unittest.main()
