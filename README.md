# sqloader

A lightweight Python utility for managing SQL migrations and loading SQL from JSON or .sql files.
Supports common relational databases and is designed for simple, clean integration with any Python backend (e.g., FastAPI).

---

## Installation

```powershell
pip install sqloader
```

## Features

- ✅ Easy database migration management
- ✅ Load SQL queries from `.json` or `.sql` files
- ✅ Supports MySQL and SQLite
- ✅ Clean API for integration
- ✅ Lightweight and dependency-minimized

## Quickstart

```python
from sqloader.init import database_init

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
    "migration": {
        "auto_migration": True,
        "migration_path": "res/sql/migration/mysql"
    },
}

db, sqloader, migrator = database_init(config)

# Example usage
query = sqloader.load_sql("user_info", "user.get_user_by_id")
result = db.select_one(query, (123,))

```

## SQL Loading Behavior

- If the value in the .json file ends with .sql, the referenced file will be loaded from the same directory.
- Otherwise, the value is treated as a raw SQL string.

Example JSON file user.json:


```json
{
  "user": {
    "get_user_by_id": "SELECT * FROM users WHERE id = %s",
    "get_all_users": "user_all.sql"
  },
  "get_etc": "SELECT * FROM etc"
}
```
