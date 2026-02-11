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
- Async support: `asyncpg`, `aiomysql`, `aiosqlite`
- Async integrated execution: `await sqloader.async_execute()`, `await sqloader.async_fetchone()`, `await sqloader.async_fetchall()`
- **Query file sync**: copy `.json`/`.sql` files between DB directories (`sync()`, `sync_from` config, CLI)

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

## Async Usage

### Async PostgreSQL (FastAPI example)
```python
from sqloader.init import async_database_init

config = {
    "type": "postgresql",
    "postgresql": {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "pass",
        "database": "mydb",
        "max_size": 10
    },
    "service": {
        "sqloder": "res/sql/sqloader/postgresql"
    },
}

db, sq = await async_database_init(config)

# Integrated async usage
result = await sq.async_fetchone("user", "get_user_by_id", [123])
rows   = await sq.async_fetchall("user", "get_all")
await sq.async_execute("user", "update_name", ["Alice", 123])

# Direct wrapper usage
result = await db.fetchone("SELECT * FROM users WHERE id = $1", [123])
```

### Async MySQL
```python
config = {
    "type": "mysql",
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
}

db, sq = await async_database_init(config)
result = await sq.async_fetchone("user", "get_user_by_id", [123])
```

### Async Transaction
```python
async with db.begin_transaction() as txn:
    await txn.execute("INSERT INTO users (name) VALUES (%s)", ["Alice"])
    await txn.execute("UPDATE stats SET count = count + 1")
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

## Query File Sync

Copy `.json` and `.sql` query files from one DB directory to another. Useful when you want to
share a base set of queries across multiple database backends.

### Directory structure

```
res/sql/sqloader/
├── sqlite3/
│   ├── user.json
│   ├── shared_link.json
│   └── sub/
│       └── detail.json
├── mysql/
│   └── user.json          ← already exists
└── postgresql/             ← empty
```

### Manual call

```python
sq = SQLoader("res/sql/sqloader")

# Copy sqlite3 → mysql (skip existing files)
result = sq.sync("sqlite3", "mysql")
# {"copied": ["shared_link.json", "sub\\detail.json"], "skipped": ["user.json"]}

# Copy sqlite3 → postgresql (overwrite existing files)
result = sq.sync("sqlite3", "postgresql", overwrite=True)
```

### Config-based auto sync (`database_init`)

Add `sync_from` to your config. The sync runs automatically before migration.

```python
config = {
    "type": "mysql",
    "sync_from": "sqlite3",   # sqlite3 → mysql on every init
    "mysql": { "host": "localhost", ... },
    "service": { "sqloder": "res/sql/sqloader" },
}

db, sq, migrator = database_init(config)
# Sync complete: 2 copied, 1 skipped
```

### CLI

```bash
# Basic sync
python -m sqloader sync --from sqlite3 --to mysql

# With custom path
python -m sqloader sync --from sqlite3 --to mysql --path res/sql/sqloader

# Overwrite existing files
python -m sqloader sync --from sqlite3 --to postgresql --overwrite --path res/sql/sqloader
```

Output:
```
Synced sqlite3 -> mysql
Copied: 2 files
  - shared_link.json
  - sub\detail.json
Skipped: 1 files
  - user.json
```

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
| `pymysql >= 1.1.1` | MySQL (sync) |
| `psycopg2-binary >= 2.9.0` | PostgreSQL (sync) |
| `sqlite3` | SQLite sync (Python standard library) |
| `aiomysql >= 0.2.0` | MySQL (async) |
| `asyncpg >= 0.29.0` | PostgreSQL (async) |
| `aiosqlite >= 0.20.0` | SQLite (async) |
