from . import (
    MySqlWrapper, SQLiteWrapper, PostgreSQLWrapper,
    DatabaseMigrator, SQLoader,
    AsyncMySqlWrapper, AsyncPostgreSQLWrapper, AsyncSQLiteWrapper,
)


def check_and_get(config, target):
    val = config.get(target, None)
    if val != None:
        return val
    else:
        raise Exception(f"Require value {target}")


def database_init(db_config):
    db_instance = None

    db_type = check_and_get(db_config, "type")
    dbconn_info = db_config[db_type]

    if db_type == "mysql":
        host = check_and_get(dbconn_info, "host")
        user = check_and_get(dbconn_info, "user")
        password = check_and_get(dbconn_info, "password")
        database = check_and_get(dbconn_info, "database")
        port = dbconn_info.get("port", None)
        log = dbconn_info.get("log", False)

        if port != None:
            mysql = MySqlWrapper(host=host, user=user, password=password, db=database, port=port, log=log)
        else:
            mysql = MySqlWrapper(host=host, user=user, password=password, db=database, log=log)
        db_instance = mysql
        print("MySQL initialized")
    elif db_type == "postgresql" or db_type == "postgres":
        host = check_and_get(dbconn_info, "host")
        user = check_and_get(dbconn_info, "user")
        password = check_and_get(dbconn_info, "password")
        database = check_and_get(dbconn_info, "database")
        port = dbconn_info.get("port", 5432)
        log = dbconn_info.get("log", False)
        max_parallel_queries = dbconn_info.get("max_parallel_queries", 5)

        pg = PostgreSQLWrapper(
            host=host, user=user, password=password, database=database,
            port=port, log=log, max_parallel_queries=max_parallel_queries
        )
        db_instance = pg
        print("PostgreSQL initialized")
    elif db_type == "sqlite3" or db_type == "sqlite" or db_type == "local":
        db_name = check_and_get(dbconn_info, "db_name")
        sqlite3 = SQLiteWrapper(db_name=db_name)
        db_instance = sqlite3
        print("SQLite3 initialized")

    db_service = db_config.get("service", None)
    sqloader = None

    if db_service != None:
        sqloader_path = db_service.get('sqloder', None)
        if sqloader_path != None:
            # Pass db_instance's db_type and placeholder config to SQLoader
            placeholder = db_config.get('placeholder', None)
            sq_db_type = db_instance.db_type if db_instance else None
            sqloader = SQLoader(sqloader_path, db_type=sq_db_type, placeholder=placeholder, db=db_instance)
    print("SQLoader initialized")

    sync_from = db_config.get("sync_from", None)
    if sync_from is not None and sqloader is not None:
        result = sqloader.sync(sync_from, db_type)
        print(f"Sync complete: {len(result['copied'])} copied, {len(result['skipped'])} skipped")

    migration_config = db_config.get('migration', None)
    migrator = None
    print(migration_config)

    if migration_config != None:
        try:
            migration_path = check_and_get(migration_config, 'migration_path')
            auto_migration = migration_config.get("auto_migration", False)

            print("Starting Database Migrator")
            migrator = DatabaseMigrator(
                db_instance, migration_path, auto_migration)
            print("Database Migration Successfully")
        except Exception as e:
            print(f"Database Migration Failed.{e}")
            exit(1)

    return db_instance, sqloader, migrator


async def async_database_init(db_config):
    """
    Async counterpart of database_init().

    Creates an async DB wrapper using the same config structure as database_init(),
    then injects the instance into SQLoader as async_db.

    Returns (db_instance, sqloader, migrator) — same shape as database_init().

    Config example
    --------------
    {
        "type": "postgresql",
        "postgresql": {
            "host": "localhost", "user": "...", "password": "...",
            "database": "mydb", "port": 5432, "max_size": 5
        },
        "service": {"sqloder": "./sql"},
        "migration": {"migration_path": "./migrations", "auto_migration": True}
    }
    """
    db_instance = None

    db_type = check_and_get(db_config, "type")
    dbconn_info = db_config[db_type]

    if db_type == "mysql":
        host = check_and_get(dbconn_info, "host")
        user = check_and_get(dbconn_info, "user")
        password = check_and_get(dbconn_info, "password")
        database = check_and_get(dbconn_info, "database")
        port = dbconn_info.get("port", 3306)
        log = dbconn_info.get("log", False)
        max_size = dbconn_info.get("max_size", 5)

        db_instance = await AsyncMySqlWrapper.create(
            host=host, user=user, password=password, db=database,
            port=port, log=log, max_size=max_size
        )
        print("Async MySQL initialized")
    elif db_type == "postgresql" or db_type == "postgres":
        host = check_and_get(dbconn_info, "host")
        user = check_and_get(dbconn_info, "user")
        password = check_and_get(dbconn_info, "password")
        database = check_and_get(dbconn_info, "database")
        port = dbconn_info.get("port", 5432)
        log = dbconn_info.get("log", False)
        max_size = dbconn_info.get("max_size", 5)

        db_instance = await AsyncPostgreSQLWrapper.create(
            host=host, user=user, password=password, database=database,
            port=port, log=log, max_size=max_size
        )
        print("Async PostgreSQL initialized")
    elif db_type == "sqlite3" or db_type == "sqlite" or db_type == "local":
        db_name = check_and_get(dbconn_info, "db_name")
        memory_mode = dbconn_info.get("memory_mode", False)
        log = dbconn_info.get("log", False)

        db_instance = await AsyncSQLiteWrapper.create(
            db_name=db_name, memory_mode=memory_mode, log=log
        )
        print("Async SQLite3 initialized")

    db_service = db_config.get("service", None)
    sqloader = None

    if db_service is not None:
        sqloader_path = db_service.get("sqloder", None)
        if sqloader_path is not None:
            # Pass db_instance's db_type and placeholder config to SQLoader as async_db
            placeholder = db_config.get("placeholder", None)
            sq_db_type = db_instance.db_type if db_instance else None
            sqloader = SQLoader(sqloader_path, db_type=sq_db_type,
                                placeholder=placeholder, async_db=db_instance)
    print("SQLoader initialized")

    sync_from = db_config.get("sync_from", None)
    if sync_from is not None and sqloader is not None:
        result = sqloader.sync(sync_from, db_type)
        print(f"Sync complete: {len(result['copied'])} copied, {len(result['skipped'])} skipped")

    migration_config = db_config.get("migration", None)
    migrator = None
    print(migration_config)

    if migration_config is not None:
        try:
            migration_path = check_and_get(migration_config, "migration_path")
            auto_migration = migration_config.get("auto_migration", False)

            print("Starting Database Migrator")
            migrator = DatabaseMigrator(db_instance, migration_path, auto_migration)
            print("Database Migration Successfully")
        except Exception as e:
            print(f"Database Migration Failed.{e}")
            exit(1)

    return db_instance, sqloader, migrator