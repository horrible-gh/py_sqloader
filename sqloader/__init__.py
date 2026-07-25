from ._prototype import DatabasePrototype
from .sqlite3 import SQLiteWrapper
from .mysql import MySqlWrapper
from .sqloader import SQLoader
from .migrator import DatabaseMigrator
from .dialect import DialectConverter, convert_sql, convert_placeholders, normalize_dialect
from ._async_prototype import AsyncDatabasePrototype, AsyncTransaction

# Optional imports: only load if dependencies are available
try:
    from .postgresql import PostgreSQLWrapper
except ImportError:
    PostgreSQLWrapper = None

try:
    from .postgresql_async import AsyncPostgreSQLWrapper
except ImportError:
    AsyncPostgreSQLWrapper = None

try:
    from .mysql_async import AsyncMySqlWrapper
except ImportError:
    AsyncMySqlWrapper = None

try:
    from .sqlite3_async import AsyncSQLiteWrapper
except ImportError:
    AsyncSQLiteWrapper = None

__all__ = [
    # core
    "SQLoader",
    "DatabaseMigrator",
    "DatabasePrototype",
    "AsyncDatabasePrototype",
    "AsyncTransaction",
    # dialect conversion
    "DialectConverter",
    "convert_sql",
    "convert_placeholders",
    "normalize_dialect",
    # backend wrappers
    "SQLiteWrapper",
    "MySqlWrapper",
    "PostgreSQLWrapper",
    "AsyncSQLiteWrapper",
    "AsyncMySqlWrapper",
    "AsyncPostgreSQLWrapper",
]
