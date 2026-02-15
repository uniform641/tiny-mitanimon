import duckdb
import uuid
from pathlib import Path


class DuckdbManager():
    def __init__(self, db_path):
        self.db_path: Path = Path(db_path)
    
    @property
    def exist(self) -> bool:
        return self.db_path.exists()
    
    @property
    def size(self) -> int:
        if self.exist:
            return self.db_path.stat().st_size
        return 0
    
    def create_db(self, overwrite: bool = False) -> None:
        if overwrite:
            self.remove_db()
        with duckdb.connect(self.db_path) as conn:
            current_dir = Path(__file__).parent
            with open(current_dir / 'init.sql') as ddl_file:
                ddl = ddl_file.read()
                conn.execute(ddl)

    def remove_db(self) -> None:
        if self.exist:
            self.db_path.unlink()
    
    def copy_db_file(self, source, destination) -> None:
        with duckdb.connect() as conn:
            sql = f"ATTACH '{source}' AS db1;\
                    ATTACH '{destination}' AS db2;\
                    COPY FROM DATABASE db1 TO db2;"
            conn.execute(sql)
    
    # according to official reference
    # https://duckdb.org/docs/stable/operations_manual/footprint_of_duckdb/reclaiming_space
    # Cation! This action takes extra space for data copy
    def compact(self) -> tuple[int, int]:
        original_db_size = self.size
        temp_db_filename = str(uuid.uuid4()).replace('-', '')
        temp_db_path = self.db_path.parent / temp_db_filename

        self.copy_db_file(self.db_path, temp_db_path)
        self.remove_db()
        self.copy_db_file(temp_db_path, self.db_path)

        return (original_db_size, self.size)

    def execute(self, sql: str, enable_spatial_extension: bool = True) -> None:
        with duckdb.connect(self.db_path) as conn:
            if enable_spatial_extension:
                conn.install_extension('spatial')
                conn.load_extension('spatial')
            conn.execute(sql)

