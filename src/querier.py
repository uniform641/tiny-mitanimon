import duckdb
from duckdb import DuckDBPyConnection

class QueryWorker:
    def __init__(self, db_path: str = "db/boundary.duckdb"):
        self.db_path: str = db_path
        self.connection: type[DuckDBPyConnection]
        self.create_connection()
    
    def create_connection(self):
        self.connection = duckdb.connect(self.db_path, read_only=True)
        self.connection.install_extension('spatial')
        self.connection.load_extension('spatial')
        self.check_healthy()
    
    def check_healthy(self) -> bool:
        try:
            result = self.connection.execute('select count(1) from relation')
            count = result.fetchone()[0]
            if count > 0:
                return True
        except:
            return False
    
    """
    return reverse geocoding result from top to down in a list
    """
    def query_boundary_name(self, lon: float, lat: float, name_suffix: str = '',
                            max_admin_level: int = 11) -> list[str]:
        result: list[str] = list()
        try:
            name_suffix = "_"+name_suffix if name_suffix else ""
            self.connection.execute(
                (f'select name{name_suffix} from relation '
                 f'where ST_Contains(geom, ST_Point({lon},{lat})) '
                 f'and admin_level <= {max_admin_level} '
                 'order by admin_level'))
            while True:
                row = self.connection.fetchone()
                if not row:
                    break
                result.append(row[0])
        except:
            return list()
        return result
