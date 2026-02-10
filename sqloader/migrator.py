import os
import glob
from ._prototype import DatabasePrototype, MYSQL, SQLITE, POSTGRESQL

class DatabaseMigrator:
    def __init__(self, db: DatabasePrototype, migrations_path, auto_run=False):
        """
        :param db:              MySQL, SQLite, or PostgreSQL wrapper instance
        :param migrations_path: Path to the directory containing .sql migration files
        :param auto_run:        If True, apply pending migrations immediately on init
        """
        self.db = db
        # Store migrations_path as an absolute path
        self.migrations_path = os.path.abspath(migrations_path)
        self.create_migrations_table()
        if auto_run:
            self.apply_migrations()

    def create_migrations_table(self):
        """Create the migrations tracking table if it does not exist."""
        if self.db.db_type == MYSQL:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    filename VARCHAR(255) PRIMARY KEY,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """, None, commit=True)
        elif self.db.db_type == SQLITE:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    filename TEXT PRIMARY KEY,
                    applied_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """, None, commit=True)
        elif self.db.db_type == POSTGRESQL:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS migrations (
                    filename VARCHAR(255) PRIMARY KEY,
                    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """, None, commit=True)
        self.db.commit()

    def apply_migrations(self):
        """Apply all pending .sql migration files in sorted order."""
        applied_migrations = self.get_applied_migrations()
        for migration in self.get_migration_files():
            # migration is a relative filename e.g. "001_init.sql"
            if migration not in applied_migrations:
                self.apply_migration(migration)

    def apply_migration(self, migration):
        full_path = os.path.join(self.migrations_path, migration)

        with open(full_path, 'r', encoding='utf-8') as f:
            sql_commands = f.read().split(';')

        try:
            # Execute all statements in a single transaction
            with self.db.begin_transaction() as txn:
                for command in sql_commands:
                    command = command.strip()
                    if command:
                        txn.execute(command)
                # Auto-commit on exit, auto-rollback on exception

            if self.db.db_type == SQLITE:
                placeholder = '?'
            else:  # MYSQL and POSTGRESQL both use %s
                placeholder = '%s'

            self.db.execute(
                f"INSERT INTO migrations (filename) VALUES ({placeholder})",
                (migration,),
                commit=True
            )

            print(f"Migration {migration} applied successfully.")
        except Exception as e:
            raise Exception(f"Failed to apply migration {migration}: {e}")

    def get_migration_files(self):
        """
        Return a sorted list of .sql filenames relative to migrations_path.
        e.g. ["001_init.sql", "002_add_index.sql"]
        """
        all_files = sorted(glob.glob(os.path.join(self.migrations_path, "*.sql")))
        migration_files = [
            os.path.relpath(f, self.migrations_path)
            for f in all_files
        ]
        return migration_files

    def get_applied_migrations(self):
        """
        Return a set of filenames that have already been applied.
        Filenames match the relative paths inserted by apply_migration().
        """
        rows = self.db.fetch_all("SELECT filename FROM migrations")
        return {row['filename'] for row in rows}