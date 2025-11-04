import duckdb
from duckdb import DuckDBPyConnection
from overpass_helper import OverpassHelper

class QueryWorker:
    def __init__(self, db_path: str = "db/boundary.duckdb",
                 overpass_endpoint: str = "https://overpass-api.de/api/interpreter"):
        self.db_path: str = db_path
        self.connection: type[DuckDBPyConnection]
        self.create_connection()
        self.overpass_helper = OverpassHelper(overpass_endpoint)
    
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
                            max_admin_level: int = 11,
                            overpass_fallback: bool = True) -> list[str]:
        result: list[str] = list()
        try:
            # TODO: only support en/zh now, preference is not record
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
            
            if not result and overpass_fallback:
                raw_result = self.overpass_helper.get_reverse_geocoding(
                    lon, lat, 'name'+name_suffix.replace('_', ':'))
                if not raw_result:
                    return result

                raw_result.sort(key=lambda x: x.admin_level)
                return [item.name_preference for item in raw_result 
                        if item.admin_level <= max_admin_level]
        except:
            return list()
        return result
