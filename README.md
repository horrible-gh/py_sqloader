# sqloader

A lightweight Python utility for managing SQL migrations and loading SQL from JSON or .sql files.
Supports MySQL, PostgreSQL, and SQLite with clean integration for any Python backend (e.g., FastAPI).

---

## Installation

```bash
# Basic installation (MySQL sync only)
pip install sqloader

# With PostgreSQL support
pip install sqloader[postgresql]

# With async MySQL support
pip install sqloader[async-mysql]

# With async PostgreSQL support
pip install sqloader[async-postgresql]

# With async SQLite support
pip install sqloader[async-sqlite]

# Install all optional dependencies
pip install sqloader[all]
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
- **Query file sync**: copy `.json`/`.sql` files between DB directories (`sync()`, `sync_from` config, CLI), with optional on-the-fly dialect conversion of `.sql` files (`sync(convert=True)`, `--convert`)
- **Dialect conversion**: rule-based translation of SQL between SQLite, MySQL/MariaDB and PostgreSQL (`convert_sql()`, `DialectConverter`, CLI)

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

#### Connection pool tuning (PostgreSQL)

All values are optional; the defaults reproduce the previous behaviour.

| Option | Default | Meaning |
|---|---|---|
| `max_parallel_queries` | `5` | How many queries may run at once. Callers beyond this wait for a free slot. |
| `pool_max` | = `max_parallel_queries` | Maximum physical connections. Raise above `max_parallel_queries` to keep spare connections in reserve. |
| `pool_min` | `1` | Connections opened eagerly at startup. |
| `acquire_timeout` | `None` | Seconds to wait for a free slot before raising `PoolTimeoutError`. `None` waits indefinitely. |
| `max_lifetime` | `None` | Discard a connection older than this many seconds when it is next checked out. |
| `max_idle` | `None` | Discard a connection that sat idle in the pool longer than this many seconds. |

```python
config = {
    "type": "postgresql",
    "postgresql": {
        "host": "localhost",
        "user": "postgres",
        "password": "pass",
        "database": "mydb",
        "max_parallel_queries": 30,   # concurrency limit
        "pool_max": 35,               # a few spare connections for transactions
        "acquire_timeout": 5,         # fail fast instead of queueing forever
        "max_lifetime": 3600,         # recycle connections hourly
        "max_idle": 600               # drop connections idle over 10 minutes
    },
}
```

Note that `max_parallel_queries` — not the pool — is what makes callers queue:
psycopg2's pool raises `connection pool exhausted` rather than waiting, so
`pool_max` is required to be `>= max_parallel_queries` and construction fails
loudly otherwise.

Without `acquire_timeout`, a request that cannot get a slot waits forever. If
you are debugging latency under load, setting it turns silent queueing into a
visible error.

MySQL accepts `max_parallel_queries` and `acquire_timeout` with the same
meaning. MySQL does not pool connections — each query opens its own.

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

# Copy AND translate .sql dialect while syncing (see "Dialect Conversion" below).
# .sql file contents are rewritten sqlite3 → postgresql; .json files are copied
# verbatim. result["warnings"] lists constructs with no safe automatic equivalent.
result = sq.sync("sqlite3", "postgresql", overwrite=True, convert=True)
# {"copied": [...], "skipped": [...], "warnings": ["001_schema.sql: ..."]}
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

# Sync AND translate .sql dialect in one step (.json copied verbatim);
# converter warnings are printed to stderr
python -m sqloader sync --from sqlite3 --to postgresql --convert --overwrite --path res/sql/sqloader
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

## Dialect Conversion

sqloader applies migration/query SQL verbatim — it never rewrites it. When you
keep a single canonical set of SQL (typically the SQLite migrations) and want to
target MySQL/MariaDB or PostgreSQL as well, `DialectConverter` translates the
constructs that differ between dialects.

### What it converts

| Construct | SQLite | MySQL/MariaDB | PostgreSQL |
|---|---|---|---|
| Auto-increment PK | `INTEGER PRIMARY KEY AUTOINCREMENT` | `... AUTO_INCREMENT` | `SERIAL PRIMARY KEY` |
| Conflict-ignore insert | `INSERT OR IGNORE` | `INSERT IGNORE` | `INSERT ... ON CONFLICT DO NOTHING` |
| Current timestamp | `datetime('now')` | `UTC_TIMESTAMP()` | `CURRENT_TIMESTAMP` |
| JSON field access | `json_extract(c,'$.x')` | `json_extract(c,'$.x')` | `c->>'x'` |
| Foreign-key pragma | `PRAGMA foreign_keys = ON` | *(dropped)* | *(dropped)* |
| Identifier quoting | `"x"` | `` `x` `` | `"x"` |
| MySQL table options | — | `... ENGINE=InnoDB ...` | *(dropped)* |
| Parameter placeholder | `?` | `%s` | `%s` |

Conversion is intentionally conservative. Constructs with no safe automatic
equivalent (`json_each`, `json_group_array`, multi-level JSON paths,
`ON CONFLICT ... DO UPDATE`, `INSERT OR REPLACE` targeting Postgres/MySQL) are
left in place and reported through `converter.warnings`. Column *type* mapping
(e.g. SQLite affinity → strict `VARCHAR`/`BOOLEAN`/`TIMESTAMP`) is **not**
performed automatically — review types after conversion.

### Python

```python
from sqloader import convert_sql, DialectConverter

# One-shot
pg_sql = convert_sql(sqlite_sql, "sqlite", "postgresql")

# With access to warnings, and optional placeholder translation
conv = DialectConverter("sqlite", "mysql")
mysql_sql = conv.convert(sqlite_sql, placeholders=True)
for w in conv.warnings:
    print("WARN:", w)
```

Dialect names accept `sqlite`/`sqlite3`, `mysql`/`mariadb`, `postgres`/`postgresql`
(case-insensitive), or the `SQLITE`/`MYSQL`/`POSTGRESQL` constants.

### CLI

```bash
# Convert a single file to stdout
python -m sqloader convert --from sqlite --to postgresql --path 001_schema.sql

# Convert a file to a target file
python -m sqloader convert --from sqlite --to mysql --path 001_schema.sql --out 001_schema.mysql.sql

# Convert every .sql in a directory into another directory
python -m sqloader convert --from sqlite --to postgresql \
    --path sql/migrations/sqlite --out sql/migrations/postgres

# Also translate parameter placeholders (? <-> %s)
python -m sqloader convert --from sqlite --to mysql --path query.sql --placeholders
```

Warnings are printed to stderr; converted SQL goes to stdout / the output path.

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

### Required
| Package | Purpose |
|---------|---------|
| `pymysql >= 1.1.1` | MySQL (sync) |
| `sqlite3` | SQLite sync (Python standard library) |

### Optional
Install only what you need:

| Package | Purpose | Install with |
|---------|---------|--------------|
| `psycopg2-binary >= 2.9.0` | PostgreSQL (sync) | `pip install sqloader[postgresql]` |
| `aiomysql >= 0.2.0` | MySQL (async) | `pip install sqloader[async-mysql]` |
| `asyncpg >= 0.29.0` | PostgreSQL (async) | `pip install sqloader[async-postgresql]` |
| `aiosqlite >= 0.20.0` | SQLite (async) | `pip install sqloader[async-sqlite]` |
