# sqloader

A lightweight Python utility for managing SQL migrations and loading SQL from JSON or .sql files.
Supports MySQL, PostgreSQL, and SQLite with clean integration for any Python backend (e.g., FastAPI).

---

## Installation

```powershell
pip install sqloader
```

## Features

- Easy database migration management
- Load SQL queries from `.json` or `.sql` files
- Supports MySQL, PostgreSQL, and SQLite
- Integrated execution: `sqloader.execute()`, `sqloader.fetch_one()`, `sqloader.fetch_all()`
- Thread-safe connection pooling (Semaphore + psycopg2 SimpleConnectionPool)
- Transaction context manager with automatic commit / rollback

---

## Quickstart

### MySQL

```python
from sqloader.init import database_init

config = {
    "type": "mysql",
    "placeholder": ["?", "%s"],
    "mysql": {
        "host": "localhost",
        "port": 3306,
        "user": "root",
        "password": "pass",
        "database": "mydb"
    },
    "service": {
        "sqloder": "res/sql/sqloader/mysql"
    },
    "migration": {
        "auto_migration": True,
        "migration_path": "res/sql/migration/mysql"
    },
}

db, sq, migrator = database_init(config)

# Classic usage
query = sq.load_sql("user", "get_user_by_id")
result = db.fetch_one(query, [123])

# Integrated usage (SQLoader runs the query directly)
result = sq.fetch_one("user", "get_user_by_id", [123])
rows   = sq.fetch_all("user", "get_all")
sq.execute("user", "update_name", ["Alice", 123])
```

### PostgreSQL

```python
config = {
    "type": "postgresql",
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "pass",
        "database": "mydb",
        "max_parallel_queries": 10   # optional, default 5
    },
    "service": {
        "sqloder": "res/sql/sqloader/postgresql"
    },
    "migration": {
        "auto_migration": True,
        "migration_path": "res/sql/migration/postgresql"
    },
}

db, sq, migrator = database_init(config)
result = sq.fetch_one("user", "get_user_by_id", [123])
```

### SQLite

```python
config = {
    "type": "sqlite3",
    "sqlite3": {
        "db_name": "local.db"
    },
    "service": {
        "sqloder": "res/sql/sqloader/sqlite"
    },
}

db, sq, migrator = database_init(config)
```

---

## SQL Loading Behavior

If a value in the `.json` file ends with `.sql`, the referenced file is loaded from the same directory.
Otherwise the value is used directly as a SQL string.

**user.json**

```json
{
  "get_user_by_id": "SELECT * FROM users WHERE id = %s",
  "get_all": "user_all.sql",
  "admin": {
    "bulk_delete": "DELETE FROM users WHERE id = %s"
  }
}
```

```python
# Simple key
sq.fetch_one("user", "get_user_by_id", [123])

# Nested key (dot notation)
sq.execute("user", "admin.bulk_delete", [999])
```

---

## Transaction

```python
with db.begin_transaction() as txn:
    txn.execute("INSERT INTO users (name) VALUES (%s)", ["Alice"])
    txn.execute("UPDATE stats SET count = count + 1")
    # Commits automatically on success, rolls back on exception
```

---

## Migration

Migration files are applied in filename-sorted order. Already applied files are skipped.

```
res/sql/migration/
    001_create_users.sql
    002_add_index.sql
```

Set `auto_migration: True` to apply pending migrations automatically during `database_init()`.

---

## SQLoader Standalone Usage

```python
from sqloader import SQLoader
from sqloader.mysql import MySqlWrapper

db = MySqlWrapper(host="localhost", user="root", password="pass", db="mydb")
sq = SQLoader("res/sql", db_type=2, db=db)  # db_type: MYSQL=2, POSTGRESQL=3, SQLITE=1

# Or inject later
sq = SQLoader("res/sql", db_type=2)
sq.set_db(db)
```

---

## Dependencies

| Package | Purpose |
|---------|---------|
| `pymysql >= 1.1.1` | MySQL connectivity |
| `psycopg2-binary >= 2.9.0` | PostgreSQL connectivity |
| `sqlite3` | SQLite (Python standard library) |