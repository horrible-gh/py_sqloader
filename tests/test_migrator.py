"""
Integration tests for DatabaseMigrator using file-based SQLite + temp files.

SQLiteTransaction opens a new connection to db_name on every transaction,
so :memory: mode would yield a separate empty database for each transaction.
File mode is used here to share state across connections.
No external dependencies required.
"""
import os
import pytest
import tempfile
from sqloader.sqlite3 import SQLiteWrapper
from sqloader.migrator import DatabaseMigrator


@pytest.fixture
def migration_dir():
    """Temporary directory with sample .sql migration files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # 001: create table
        with open(os.path.join(tmpdir, "001_create_users.sql"), "w") as f:
            f.write(
                "CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
            )
        # 002: add email column
        with open(os.path.join(tmpdir, "002_add_email.sql"), "w") as f:
            f.write("ALTER TABLE users ADD COLUMN email TEXT")
        yield tmpdir


@pytest.fixture
def db():
    """File-based SQLite so that multiple connections share the same database."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    try:
        wrapper = SQLiteWrapper(db_name=db_path)
        yield wrapper
        wrapper.close()
    finally:
        try:
            os.unlink(db_path)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# create_migrations_table
# ---------------------------------------------------------------------------

class TestCreateMigrationsTable:
    def test_table_created(self, db, migration_dir):
        DatabaseMigrator(db, migration_dir, auto_run=False)
        rows = db.fetch_all("SELECT name FROM sqlite_master WHERE type='table' AND name='migrations'")
        assert len(list(rows)) == 1

    def test_table_creation_is_idempotent(self, db, migration_dir):
        """Calling twice should not raise."""
        m = DatabaseMigrator(db, migration_dir, auto_run=False)
        m.create_migrations_table()  # second call should be safe


# ---------------------------------------------------------------------------
# get_migration_files
# ---------------------------------------------------------------------------

class TestGetMigrationFiles:
    def test_returns_sorted_filenames(self, db, migration_dir):
        m = DatabaseMigrator(db, migration_dir, auto_run=False)
        files = m.get_migration_files()
        assert files == ["001_create_users.sql", "002_add_email.sql"]

    def test_empty_dir_returns_empty_list(self, db):
        with tempfile.TemporaryDirectory() as empty_dir:
            m = DatabaseMigrator(db, empty_dir, auto_run=False)
            assert m.get_migration_files() == []


# ---------------------------------------------------------------------------
# apply_migrations
# ---------------------------------------------------------------------------

class TestApplyMigrations:
    def test_applies_all_pending_migrations(self, db, migration_dir):
        m = DatabaseMigrator(db, migration_dir, auto_run=False)
        m.apply_migrations()

        applied = m.get_applied_migrations()
        assert "001_create_users.sql" in applied
        assert "002_add_email.sql" in applied

    def test_migrations_are_idempotent(self, db, migration_dir):
        """Running apply_migrations twice should not re-apply or raise."""
        m = DatabaseMigrator(db, migration_dir, auto_run=False)
        m.apply_migrations()
        m.apply_migrations()  # second run — nothing new to apply

        applied = m.get_applied_migrations()
        assert len(applied) == 2

    def test_schema_is_actually_applied(self, db, migration_dir):
        """After migration, the users table should exist and be usable."""
        DatabaseMigrator(db, migration_dir, auto_run=True)

        db.execute("INSERT INTO users (name) VALUES (?)", ("Alice",))
        row = db.fetch_one("SELECT name FROM users WHERE name = ?", ("Alice",))
        assert row["name"] == "Alice"

    def test_second_migration_column_exists(self, db, migration_dir):
        """After both migrations, email column should be present."""
        DatabaseMigrator(db, migration_dir, auto_run=True)

        db.execute("INSERT INTO users (name, email) VALUES (?, ?)", ("Bob", "bob@example.com"))
        row = db.fetch_one("SELECT email FROM users WHERE name = ?", ("Bob",))
        assert row["email"] == "bob@example.com"


# ---------------------------------------------------------------------------
# auto_run
# ---------------------------------------------------------------------------

class TestAutoRun:
    def test_auto_run_true_applies_on_init(self, db, migration_dir):
        m = DatabaseMigrator(db, migration_dir, auto_run=True)
        applied = m.get_applied_migrations()
        assert len(applied) == 2

    def test_auto_run_false_does_not_apply(self, db, migration_dir):
        m = DatabaseMigrator(db, migration_dir, auto_run=False)
        applied = m.get_applied_migrations()
        assert len(applied) == 0