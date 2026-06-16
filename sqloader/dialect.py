"""
SQL dialect conversion utilities.

By design sqloader applies migration/query SQL *verbatim* — it does not
translate between database dialects. This module adds rule-based conversion of
SQL text between the three dialects sqloader supports — SQLite, MySQL/MariaDB
and PostgreSQL — so a single set of source SQL (typically the SQLite migrations)
can be retargeted at another backend.

The converter is deliberately conservative. It rewrites the constructs that are
known to differ between dialects (auto-increment primary key, conflict handling,
``datetime('now')``, JSON access, identifier quoting, MySQL engine clauses and,
optionally, parameter placeholders) and records a *warning* for constructs that
have no safe automatic equivalent (``json_each``, ``json_group_array``,
multi-level JSON paths, ``ON CONFLICT ... DO UPDATE`` …) instead of emitting
silently broken SQL.

Typical usage::

    from sqloader import DialectConverter, convert_sql

    pg = convert_sql(sqlite_sql, "sqlite", "postgresql")

    conv = DialectConverter("sqlite", "mysql")
    mysql_sql = conv.convert(sqlite_sql)
    for w in conv.warnings:
        print("WARN:", w)
"""

import re

import sqlparse

from ._prototype import SQLITE, MYSQL, POSTGRESQL, NATIVE_PLACEHOLDER

# Canonical dialect names used internally and accepted by the public API.
SQLITE_NAME = "sqlite"
MYSQL_NAME = "mysql"
POSTGRESQL_NAME = "postgresql"

# Accepted aliases -> canonical name. Both the string spellings and the integer
# constants from _prototype (SQLITE/MYSQL/POSTGRESQL) are recognised.
_DIALECT_ALIASES = {
    "sqlite": SQLITE_NAME,
    "sqlite3": SQLITE_NAME,
    SQLITE: SQLITE_NAME,
    "mysql": MYSQL_NAME,
    "mariadb": MYSQL_NAME,
    MYSQL: MYSQL_NAME,
    "postgres": POSTGRESQL_NAME,
    "postgresql": POSTGRESQL_NAME,
    "psql": POSTGRESQL_NAME,
    POSTGRESQL: POSTGRESQL_NAME,
}

# Native parameter placeholder per canonical dialect.
_PLACEHOLDER = {
    SQLITE_NAME: NATIVE_PLACEHOLDER[SQLITE],
    MYSQL_NAME: NATIVE_PLACEHOLDER[MYSQL],
    POSTGRESQL_NAME: NATIVE_PLACEHOLDER[POSTGRESQL],
}


def normalize_dialect(dialect):
    """Map a dialect name/alias/constant to its canonical name.

    Accepts strings ("sqlite", "sqlite3", "mysql", "mariadb", "postgres",
    "postgresql", "psql") case-insensitively, or the integer constants
    SQLITE/MYSQL/POSTGRESQL from ``sqloader._prototype``.
    """
    key = dialect.strip().lower() if isinstance(dialect, str) else dialect
    if key not in _DIALECT_ALIASES:
        raise ValueError(
            f"Unknown SQL dialect: {dialect!r}. "
            "Supported: sqlite, mysql/mariadb, postgresql."
        )
    return _DIALECT_ALIASES[key]


_FLAGS = re.IGNORECASE

# --- shared patterns -------------------------------------------------------
RE_PRAGMA = re.compile(r"^\s*PRAGMA\b", _FLAGS)
RE_AUTOINC_PK = re.compile(r"\bINTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT\b", _FLAGS)
RE_AUTOINCREMENT = re.compile(r"\bAUTOINCREMENT\b", _FLAGS)
RE_AUTOINCREMENT_SP = re.compile(r"\s*\bAUTOINCREMENT\b", _FLAGS)
RE_INSERT_OR_IGNORE = re.compile(r"\bINSERT\s+OR\s+IGNORE\b", _FLAGS)
RE_INSERT_OR_REPLACE = re.compile(r"\bINSERT\s+OR\s+REPLACE\b", _FLAGS)
RE_DATETIME_NOW = re.compile(r"\bdatetime\s*\(\s*'now'\s*\)", _FLAGS)
RE_JSON_EXTRACT = re.compile(
    r"\bjson_extract\s*\(\s*([^,()]+?)\s*,\s*'\$\.([A-Za-z0-9_]+)'\s*\)", _FLAGS
)
RE_JSON_MULTI = re.compile(
    r"\bjson_extract\s*\(\s*[^,()]+?\s*,\s*'\$\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)+'\s*\)",
    _FLAGS,
)
RE_JSON_UNSUPPORTED = re.compile(r"\bjson_each\b|\bjson_group_array\b|\bjson_tree\b", _FLAGS)
RE_BACKTICK_IDENT = re.compile(r"`([A-Za-z_][A-Za-z0-9_]*)`")
RE_DQUOTE_IDENT = re.compile(r'"([A-Za-z_][A-Za-z0-9_]*)"')
RE_ENGINE_CLAUSE = re.compile(r"\)\s*ENGINE\s*=\s*[^;]*", _FLAGS)
RE_SERIAL_PK = re.compile(r"\b(?:BIG|SMALL)?SERIAL\s+PRIMARY\s+KEY\b", _FLAGS)
RE_SERIAL = re.compile(r"\b(?:BIG|SMALL)?SERIAL\b", _FLAGS)
RE_NOW = re.compile(r"\bNOW\s*\(\s*\)", _FLAGS)
RE_UTC_TS = re.compile(r"\bUTC_TIMESTAMP\s*\(\s*\)", _FLAGS)
RE_PG_JSON_OP = re.compile(r"([A-Za-z_][\w.]*)\s*->>\s*'([A-Za-z0-9_]+)'")
RE_INT_AI_PK = re.compile(r"\b(?:INT|INTEGER)\s+AUTO_INCREMENT\s+PRIMARY\s+KEY\b", _FLAGS)
RE_AUTO_INCREMENT = re.compile(r"\bAUTO_INCREMENT\b", _FLAGS)
RE_AUTO_INCREMENT_SP = re.compile(r"\s*\bAUTO_INCREMENT\b", _FLAGS)
RE_INSERT_IGNORE = re.compile(r"\bINSERT\s+IGNORE\s+INTO\b", _FLAGS)
RE_REPLACE_INTO = re.compile(r"\bREPLACE\s+INTO\b", _FLAGS)
RE_INSERT_INTO = re.compile(r"\bINSERT\s+INTO\b", _FLAGS)
RE_ON_CONFLICT_NOTHING = re.compile(r"\s*ON\s+CONFLICT(?:\s*\([^)]*\))?\s+DO\s+NOTHING", _FLAGS)
RE_ON_CONFLICT = re.compile(r"\bON\s+CONFLICT\b", _FLAGS)


def _one_line(stmt, limit=80):
    """Collapse a statement to a single short line for warning messages."""
    flat = " ".join(stmt.split())
    return flat if len(flat) <= limit else flat[:limit] + "..."


def _append_on_conflict_do_nothing(stmt):
    """Append ``ON CONFLICT DO NOTHING`` before any trailing semicolon."""
    body = stmt.rstrip()
    has_semicolon = body.endswith(";")
    if has_semicolon:
        body = body[:-1].rstrip()
    if not RE_ON_CONFLICT.search(body):
        body = body + " ON CONFLICT DO NOTHING"
    return body + ";" if has_semicolon else body


class DialectConverter:
    """Convert SQL text from one dialect to another.

    :param source: source dialect (name/alias/constant)
    :param target: target dialect (name/alias/constant)

    After :meth:`convert`, :attr:`warnings` holds human-readable notes about
    constructs that could not be translated automatically.
    """

    def __init__(self, source, target):
        self.source = normalize_dialect(source)
        self.target = normalize_dialect(target)
        self.warnings = []

    # -- public API ---------------------------------------------------------
    def convert(self, sql, placeholders=False):
        """Convert ``sql`` from the source dialect to the target dialect.

        :param sql:          SQL text (may contain several statements)
        :param placeholders: also translate parameter placeholders
                             (``?`` <-> ``%s``). Off by default because
                             SQLoader already converts placeholders at load
                             time via its ``placeholder`` setting.
        :return: converted SQL text
        """
        self.warnings = []
        if self.source == self.target:
            return convert_placeholders(sql, self.source, self.target) if placeholders else sql

        handler = self._handler()
        out = []
        for raw in sqlparse.split(sql) or [sql]:
            stmt = raw.strip()
            if not stmt:
                continue
            if self._should_drop(stmt):
                continue
            out.append(handler(stmt))

        result = "\n".join(out)
        if placeholders:
            result = convert_placeholders(result, self.source, self.target)
        return result

    # -- internals ----------------------------------------------------------
    def _handler(self):
        return {
            (SQLITE_NAME, MYSQL_NAME): self._sqlite_to_mysql,
            (SQLITE_NAME, POSTGRESQL_NAME): self._sqlite_to_postgresql,
            (MYSQL_NAME, SQLITE_NAME): self._mysql_to_sqlite,
            (MYSQL_NAME, POSTGRESQL_NAME): self._mysql_to_postgresql,
            (POSTGRESQL_NAME, SQLITE_NAME): self._postgresql_to_sqlite,
            (POSTGRESQL_NAME, MYSQL_NAME): self._postgresql_to_mysql,
        }[(self.source, self.target)]

    def _should_drop(self, stmt):
        # SQLite PRAGMA statements have no equivalent in MySQL/PostgreSQL
        # (foreign keys are always enforced there), so drop them.
        if self.source == SQLITE_NAME and self.target != SQLITE_NAME:
            return bool(RE_PRAGMA.match(stmt))
        return False

    def _warn_sqlite_only_json(self, stmt):
        if RE_JSON_UNSUPPORTED.search(stmt):
            self.warnings.append(
                "json_each/json_group_array/json_tree have no automatic "
                "equivalent; rewrite manually: " + _one_line(stmt)
            )

    # -- SQLite -> MySQL ----------------------------------------------------
    def _sqlite_to_mysql(self, stmt):
        self._warn_sqlite_only_json(stmt)
        stmt = RE_AUTOINC_PK.sub("INTEGER PRIMARY KEY AUTO_INCREMENT", stmt)
        stmt = RE_AUTOINCREMENT.sub("AUTO_INCREMENT", stmt)
        stmt = RE_INSERT_OR_IGNORE.sub("INSERT IGNORE", stmt)
        stmt = RE_INSERT_OR_REPLACE.sub("REPLACE", stmt)
        stmt = RE_DATETIME_NOW.sub("UTC_TIMESTAMP()", stmt)
        # json_extract(col, '$.x') is valid as-is in MySQL.
        return stmt

    # -- SQLite -> PostgreSQL ----------------------------------------------
    def _sqlite_to_postgresql(self, stmt):
        self._warn_sqlite_only_json(stmt)
        if RE_JSON_MULTI.search(stmt):
            self.warnings.append(
                "multi-level json_extract path needs a #>>'{...}' rewrite for "
                "PostgreSQL: " + _one_line(stmt)
            )
        stmt = RE_AUTOINC_PK.sub("SERIAL PRIMARY KEY", stmt)
        stmt = RE_AUTOINCREMENT_SP.sub("", stmt)
        stmt = RE_DATETIME_NOW.sub("CURRENT_TIMESTAMP", stmt)
        stmt = RE_JSON_EXTRACT.sub(r"\1->>'\2'", stmt)

        is_ignore = bool(RE_INSERT_OR_IGNORE.search(stmt))
        stmt = RE_INSERT_OR_IGNORE.sub("INSERT", stmt)
        if RE_INSERT_OR_REPLACE.search(stmt):
            self.warnings.append(
                "INSERT OR REPLACE needs an explicit ON CONFLICT ... DO UPDATE "
                "target in PostgreSQL; converted to plain INSERT: " + _one_line(stmt)
            )
            stmt = RE_INSERT_OR_REPLACE.sub("INSERT", stmt)
        if is_ignore:
            stmt = _append_on_conflict_do_nothing(stmt)
        return stmt

    # -- MySQL -> SQLite ----------------------------------------------------
    def _mysql_to_sqlite(self, stmt):
        stmt = RE_ENGINE_CLAUSE.sub(")", stmt)
        stmt = RE_INT_AI_PK.sub("INTEGER PRIMARY KEY AUTOINCREMENT", stmt)
        stmt = RE_AUTO_INCREMENT.sub("AUTOINCREMENT", stmt)
        stmt = RE_INSERT_IGNORE.sub("INSERT OR IGNORE INTO", stmt)
        stmt = RE_REPLACE_INTO.sub("INSERT OR REPLACE INTO", stmt)
        stmt = RE_UTC_TS.sub("datetime('now')", stmt)
        stmt = RE_NOW.sub("datetime('now')", stmt)
        stmt = RE_BACKTICK_IDENT.sub(r'"\1"', stmt)
        return stmt

    # -- MySQL -> PostgreSQL ------------------------------------------------
    def _mysql_to_postgresql(self, stmt):
        stmt = RE_ENGINE_CLAUSE.sub(")", stmt)
        stmt = RE_INT_AI_PK.sub("SERIAL PRIMARY KEY", stmt)
        if RE_AUTO_INCREMENT.search(stmt):
            self.warnings.append(
                "standalone AUTO_INCREMENT removed (PostgreSQL uses SERIAL/"
                "IDENTITY); review the column type: " + _one_line(stmt)
            )
            stmt = RE_AUTO_INCREMENT_SP.sub("", stmt)
        is_ignore = bool(RE_INSERT_IGNORE.search(stmt))
        stmt = RE_INSERT_IGNORE.sub("INSERT INTO", stmt)
        if RE_REPLACE_INTO.search(stmt):
            self.warnings.append(
                "REPLACE INTO needs an explicit ON CONFLICT ... DO UPDATE target "
                "in PostgreSQL; converted to plain INSERT: " + _one_line(stmt)
            )
            stmt = RE_REPLACE_INTO.sub("INSERT INTO", stmt)
        stmt = RE_UTC_TS.sub("CURRENT_TIMESTAMP", stmt)
        stmt = RE_BACKTICK_IDENT.sub(r'"\1"', stmt)
        if is_ignore:
            stmt = _append_on_conflict_do_nothing(stmt)
        return stmt

    # -- PostgreSQL -> SQLite ----------------------------------------------
    def _postgresql_to_sqlite(self, stmt):
        stmt = RE_SERIAL_PK.sub("INTEGER PRIMARY KEY AUTOINCREMENT", stmt)
        stmt = RE_SERIAL.sub("INTEGER", stmt)
        stmt = RE_NOW.sub("datetime('now')", stmt)
        stmt = RE_PG_JSON_OP.sub(r"json_extract(\1, '$.\2')", stmt)
        if RE_ON_CONFLICT_NOTHING.search(stmt):
            stmt = RE_ON_CONFLICT_NOTHING.sub("", stmt)
            stmt = RE_INSERT_INTO.sub("INSERT OR IGNORE INTO", stmt, count=1)
        elif RE_ON_CONFLICT.search(stmt):
            self.warnings.append(
                "ON CONFLICT ... DO UPDATE has no direct SQLite form "
                "(use INSERT OR REPLACE / UPSERT): " + _one_line(stmt)
            )
        return stmt

    # -- PostgreSQL -> MySQL ------------------------------------------------
    def _postgresql_to_mysql(self, stmt):
        stmt = RE_SERIAL_PK.sub("INT AUTO_INCREMENT PRIMARY KEY", stmt)
        if RE_SERIAL.search(stmt):
            self.warnings.append(
                "standalone SERIAL mapped to INT (MySQL AUTO_INCREMENT requires "
                "a key column); review: " + _one_line(stmt)
            )
            stmt = RE_SERIAL.sub("INT", stmt)
        stmt = RE_NOW.sub("NOW()", stmt)  # valid in both, kept explicit
        stmt = RE_DQUOTE_IDENT.sub(r"`\1`", stmt)
        if RE_ON_CONFLICT_NOTHING.search(stmt):
            stmt = RE_ON_CONFLICT_NOTHING.sub("", stmt)
            stmt = RE_INSERT_INTO.sub("INSERT IGNORE INTO", stmt, count=1)
        elif RE_ON_CONFLICT.search(stmt):
            self.warnings.append(
                "ON CONFLICT ... DO UPDATE has no direct MySQL form "
                "(use INSERT ... ON DUPLICATE KEY UPDATE): " + _one_line(stmt)
            )
        # col->>'x' is valid as-is in MySQL 5.7+.
        return stmt


def convert_placeholders(sql, source, target):
    """Translate parameter placeholders between dialects (``?`` <-> ``%s``).

    Best-effort string replacement intended for trusted query templates; it does
    not parse out string literals. SQLoader's own ``placeholder`` setting is the
    preferred mechanism for runtime queries.
    """
    src = _PLACEHOLDER[normalize_dialect(source)]
    dst = _PLACEHOLDER[normalize_dialect(target)]
    if src == dst:
        return sql
    return sql.replace(src, dst)


def convert_sql(sql, source, target, placeholders=False):
    """Convenience wrapper: convert ``sql`` from ``source`` to ``target``.

    Returns the converted SQL string. For access to conversion warnings,
    instantiate :class:`DialectConverter` directly.
    """
    return DialectConverter(source, target).convert(sql, placeholders=placeholders)
