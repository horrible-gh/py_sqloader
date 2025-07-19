import os
import glob
from ._prototype import DatabasePrototype, MYSQL, SQLITE

class DatabaseMigrator:
    def __init__(self, db: DatabasePrototype, migrations_path, auto_run=False):
        """
        :param db:       MySQL 또는 SQLite 래퍼 객체
        :param migrations_path: .sql 파일들이 들어 있는 폴더 경로 (절대 또는 상대)
        :param auto_run: True일 경우, 초기화 시점에 자동으로 마이그레이션 적용
        """
        self.db = db
        # migrations_path를 절대 경로로 저장 (os.path.abspath 등)
        self.migrations_path = os.path.abspath(migrations_path)
        self.create_migrations_table()
        if auto_run:
            self.apply_migrations()

    def create_migrations_table(self):
        """마이그레이션 기록용 테이블 생성"""
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
        self.db.commit()

    def apply_migrations(self):
        """아직 적용 안 된 마이그레이션 파일(.sql)들을 순서대로 실행"""
        applied_migrations = self.get_applied_migrations()
        for migration in self.get_migration_files():
            # migration: "001_init.sql", "002_update.sql" 처럼 '상대 경로'만
            if migration not in applied_migrations:
                self.apply_migration(migration)

    def apply_migration(self, migration):
        full_path = os.path.join(self.migrations_path, migration)

        with open(full_path, 'r', encoding='utf-8') as f:
            sql_commands = f.read().split(';')

        try:
            # begin_transaction()을 통해 트랜잭션 컨텍스트 사용
            with self.db.begin_transaction() as txn:
                for command in sql_commands:
                    command = command.strip()
                    if command:
                        txn.execute(command)
                # 트랜잭션 블록이 종료되면서 commit (예외 발생 시 rollback)

            if self.db.db_type == SQLITE:
                placeholder = '?'
            else:
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
        마이그레이션 폴더 내 *.sql 파일을 찾아,
        'migrations_path'로부터의 상대 경로 목록을 반환
        """
        # 예: ["C:/path/to/migrations/001_init.sql", ...]
        all_files = sorted(glob.glob(os.path.join(self.migrations_path, "*.sql")))
        # 상대 경로만 추출 => ["001_init.sql", ...]
        migration_files = [
            os.path.relpath(f, self.migrations_path)
            for f in all_files
        ]
        return migration_files

    def get_applied_migrations(self):
        """
        DB에 이미 적용된 마이그레이션의 filename 목록을 set으로 반환
        여기서 'filename'은 위에서 INSERT한 것과 동일(상대 경로)
        """
        rows = self.db.fetch_all("SELECT filename FROM migrations")
        return {row['filename'] for row in rows}
