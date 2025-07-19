import LogAssist.log as Logger
from . import MySqlWrapper, SQLiteWrapper, DatabaseMigrator, SQLoader



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
        else :
            mysql = MySqlWrapper(host=host, user=user, password=password, db=database, log=log)
        db_instance = mysql
        Logger.debug("MySQL initialized")
    elif db_type == "sqlite3" or db_type == "sqlite" or db_type == "local":
        db_name = check_and_get(dbconn_info, "db_name")
        sqlite3 = SQLiteWrapper(db_name=db_name)
        db_instance = sqlite3
        Logger.debug("SQLite3 initialized")


    db_service = db_config.get("service", None)
    sqloader = None

    if db_service != None:
        sqloader_path = db_service.get('sqloder', None)
        if sqloader_path != None:
            sqloader = SQLoader(sqloader_path)
    Logger.debug("SQLoader initialized")

    migration_config = db_config.get('migration', None)
    migrator = None
    Logger.debug(migration_config)

    if migration_config != None:
        try:
            migration_path = check_and_get(migration_config, 'migration_path')
            auto_migration = migration_config.get("auto_migration", False)

            Logger.debug("Starting Database Migrator")
            migrator = DatabaseMigrator(
                db_instance, migration_path, auto_migration)
            Logger.debug("Database Migration Successfully")
        except Exception as e:
            Logger.error(f"Database Migration Failed.{e}")
            exit(1)

    return db_instance, sqloader, migrator



