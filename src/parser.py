import osmium
import os
import duckdb
import logging
import shapely
from shapely.geometry import shape
from shapely import wkb
from datetime import datetime
from collections import Counter
from typing import Optional
from overpass_helper import OverpassHelper
from model import *
from utils import *
from utils_overpass import *

class Parser:
    def __init__(self, overpass_endpoint: str = "https://overpass-api.de/api/interpreter"):
        self.boundaries: dict[int, Boundary] = dict()
        self.root_boundary: int = None
        self.max_admin_level: int = None
        self.min_admin_level: int = None
        self.non_admin_boundary: set[int] = set()
        self.small_admin_level_boundaries: set[int] = set()
        self.ways: dict[int, Way] = dict()
        self.way_need: set[int] = set()
        self.overpass_helper = OverpassHelper(overpass_endpoint)
    
    def parse(self, file_path: str, root_boundary_id: Optional[int] = None, max_admin_level: int = 7, name_preference: Optional[str] = None):
        print(f"parse start {datetime.now()}")
        self.parse_relation(file_path, root_boundary_id, max_admin_level, name_preference)
        print(f"parse relation finished {datetime.now()}")
        self.parse_way(file_path)
        print(f"parse way finished {datetime.now()}")

    def parse_relation(self, file_path: str, root_boundary_id: Optional[int] = None, max_admin_level: int = 7, name_preference: Optional[str] = None):
        self.max_admin_level = max_admin_level

        for obj in osmium.FileProcessor(file_path)\
            .with_filter(osmium.filter.EntityFilter(osmium.osm.RELATION))\
            .with_filter(osmium.filter.TagFilter(('type','boundary'))):
                if obj.tags.get('boundary') == 'administrative':
                    osm_id = obj.id
                    name = obj.tags.get('name')
                    name_en = obj.tags.get('name:en')
                    name_zh = obj.tags.get('name:zh')
                    name_preference = obj.tags.get(name_preference)
                    admin_level = safe_cast(obj.tags.get('admin_level'), int)
                    subarea_id_list = list()
                    outer_boundary_id_list = list()
                    inner_boundary_id_list = list()
                    for member in obj.members:
                        match member.role:
                            case 'subarea':
                                # 暂不考虑 subarea 为 node 的情况
                                if member.type == 'r':
                                    subarea_id_list.append(member.ref)
                            case 'outer':
                                if member.type == 'w':
                                    outer_boundary_id_list.append(member.ref)
                            case 'inner':
                                if member.type == 'w':
                                    inner_boundary_id_list.append(member.ref)
                    
                    boundary = Boundary(osm_id, name, name_en, name_zh, name_preference, admin_level, subarea_id_list, outer_boundary_id_list, inner_boundary_id_list)
                    if osm_id not in self.boundaries:
                        self.boundaries[osm_id] = boundary
                    else:
                        print(f"boundary {boundary.name}({osm_id}) appears twice")
                else:
                    self.non_admin_boundary.add(obj.id)

        # 为每个 boundary 寻找父节点与根节点
        self.build_DAG()

        # 清除超过行政区划等级的行政边界，注意这里可能会有 admin_level = None 的行政边界，这些边界在目前的逻辑中被删除
        self.filter_by_admin_level(max_admin_level)
        
        self.filter_by_root_boundary(root_boundary_id)

        self.fix_missing_relation()
    
    def parse_way(self, file_path: str) -> None:
        for boundary in self.boundaries.values():
            for way in boundary.inner_boundary_id_list:
                self.way_need.add(way)
            for way in boundary.outer_boundary_id_list:
                self.way_need.add(way)
        
        for obj in osmium.FileProcessor(file_path, osmium.osm.NODE | osmium.osm.WAY)\
            .with_locations()\
            .with_filter(osmium.filter.EntityFilter(osmium.osm.WAY))\
            .with_filter(osmium.filter.GeoInterfaceFilter()):
                 if obj.id in self.way_need:
                    geom = shape(obj.__geo_interface__['geometry']).simplify(0.0001)
                    self.ways[obj.id] = Way(obj.id, geom, obj.is_closed())

        self.check_way_integrity()

        count_fail = 0
        for boundary in self.boundaries.values():
            outer_ways = list()
            inner_ways = list()
            for way in boundary.outer_boundary_id_list:
                if way in self.ways:
                    outer_ways.append(self.ways[way].geom)
            for way in boundary.inner_boundary_id_list:
                if way in self.ways:
                    inner_ways.append(self.ways[way].geom)
            
            outer_polygons = list(shapely.get_parts(shapely.polygonize(outer_ways)))
            inner_polygons = list(shapely.get_parts(shapely.polygonize(inner_ways)))
            if len(outer_ways) != 0 and len(outer_polygons) == 0 or len(inner_ways) != 0 and len(inner_polygons) == 0:
                print(f"boundary {boundary.name}({boundary.osm_id}) process fail")
                count_fail += 1
            result_polygons = list()
            for outer in outer_polygons:
                holes = [inner for inner in inner_polygons if inner.within(outer)]
                if holes:
                    processed = shapely.Polygon(outer.exterior, [h.exterior for h in holes])
                    result_polygons.append(processed)
                else:
                    result_polygons.append(outer)
            boundary.geom = shapely.MultiPolygon(result_polygons)
        print(f"parse way fail count: {count_fail}")

    def build_DAG(self):
        count_referenced_by_parent = Counter()
        # 计算每个节点被作为subarea的次数，得到没有被作为subarea的节点，这些节点是根节点
        for boundary in self.boundaries.values():
            for subarea in boundary.subarea_id_list:
                count_referenced_by_parent[subarea] += 1
        root_boundary: list[int] = [boundary_id for boundary_id in self.boundaries if count_referenced_by_parent[boundary_id] == 0]
        count_finish = 0
        # 从每个根节点开始，通过 BFS 的方式自顶向下传递属性 1. 根节点列表 2. 父节点
        # 注意每个节点的根节点和父节点都可能有多个，但是传递的时候根节点是继承自当前节点，父节点是只有当前节点
        while len(root_boundary) > 0:
            boundary_id = root_boundary.pop(0)
            if boundary_id not in self.boundaries:
                continue
            root_boundary_candidate_id_list = self.boundaries[boundary_id].root_boundary_candidate_id_list
            for subarea in self.boundaries[boundary_id].subarea_id_list:
                if subarea in self.boundaries:
                    self.boundaries[subarea].add_root_boundary_candidate(root_boundary_candidate_id_list)
                    self.boundaries[subarea].add_super_boundary(boundary_id)
                    count_referenced_by_parent[subarea] -= 1
                    if count_referenced_by_parent[subarea] == 0:
                        root_boundary.append(subarea)
            count_finish += 1
        
        # 如果一个 boundary 节点存在多个根节点，则将行政区等级最大的、存在的节点作为最终的根节点（如有多个相同最大等级的根节点则选第一个）
        # TODO: 这个候选逻辑可以放到上一个 pass 中，在向下传递根节点时进行 merge 操作
        for boundary in self.boundaries.values():
            boundary.root_boundary_id = boundary.root_boundary_candidate_id_list[0]
            if len(boundary.root_boundary_candidate_id_list) > 1:
                max_admin_level: int = boundary.admin_level if boundary.admin_level is not None else self.max_admin_level
                for root_boundary_candidate in boundary.root_boundary_candidate_id_list:
                    if root_boundary_candidate in self.boundaries and self.boundaries[root_boundary_candidate].admin_level is not None and self.boundaries[root_boundary_candidate].admin_level < max_admin_level:
                        boundary.root_boundary_id = root_boundary_candidate
                        max_admin_level = self.boundaries[root_boundary_candidate].admin_level
        
        if count_finish != len(self.boundaries):
            print(f"build_DAG fail. finish: {count_finish}, total: {len(self.boundaries)}")
        else:
            print(f"build_DAG success. finsh: {count_finish}")

    # 修补因裁切等原因不在文件中的子区域
    def fix_missing_relation(self) -> bool:
        success: bool = True
        for boundary in self.boundaries.values():
            for subarea in boundary.subarea_id_list:
                if subarea not in self.boundaries and subarea not in self.non_admin_boundary and subarea not in self.small_admin_level_boundaries:
                    print(f"subarea {subarea} of {boundary.name}({boundary.osm_id}) is missing")
                    success = False
        return success

    def check_way_integrity(self) -> bool:
        if len(self.way_need) == len(self.ways):
            return True
        print(f"way count expect: {len(self.way_need)}, way count actual: {len(self.ways)}")
        return False

    # @parent_boundary_id: None 表示无视与父节点关系删除节点，若填写具体的值则只切断与该节点之间的联系，如果仍与其他节点有连接，则并不删除
    def remove_boundary(self, osm_id: int, parent_boundary_id: Optional[int] = None):
        if osm_id in self.boundaries:
            super_area_id_list = self.boundaries[osm_id].super_area_id_list
            if parent_boundary_id is None:
                for super_boundary in super_area_id_list:
                    if super_boundary in self.boundaries and osm_id in self.boundaries[super_boundary].subarea_id_list:
                        self.boundaries[super_boundary].subarea_id_list.remove(osm_id)
                super_area_id_list = list()
            else:
                if parent_boundary_id in self.boundaries and osm_id in self.boundaries[parent_boundary_id].subarea_id_list:
                    self.boundaries[parent_boundary_id].subarea_id_list.remove(osm_id)
                if parent_boundary_id in super_area_id_list:
                    super_area_id_list.remove(parent_boundary_id)

            # 如果当前节点变为孤儿节点（没有父节点），则通过递归方式删除当前节点及所有子节点
            if len(super_area_id_list) <= 0:
                for subarea_id in list(self.boundaries[osm_id].subarea_id_list):
                    self.remove_boundary(subarea_id, osm_id)
                # print(f"delete boundary {self.boundaries[osm_id]}")
                del self.boundaries[osm_id]
    
    def init_db(self, db_path: str = "db/boundary.duckdb"):
        if os.path.exists(db_path):
            return
        conn = duckdb.connect(db_path)
        ddl = '''
        INSTALL spatial;
        LOAD spatial;

        CREATE TABLE IF NOT EXISTS relation (
            osm_id BIGINT PRIMARY KEY,
            name VARCHAR,
            name_en VARCHAR,
            name_zh VARCHAR,
            name_preference VARCHAR,
            admin_level INTEGER,
            super_area_id_list BIGINT[],
            subarea_id_list BIGINT[],
            root_boundary_id BIGINT,
            outer_boundary_id_list BIGINT[],
            inner_boundary_id_list BIGINT[],
            bbox GEOMETRY,  -- [min_lon, min_lat, max_lon, max_lat]
            geom GEOMETRY
        );
        '''
        result = conn.execute(ddl)
        print(f"init database: {result}")

        conn.close()
    
    def save_to_database(self, overwrite: bool = False, db_path: str = "db/boundary.duckdb") -> None:
        print(f"save to database start {datetime.now()}")
        self.save_relation_to_database(overwrite, db_path)
        print(f"save relation finished {datetime.now()}")

    def save_relation_to_database(self, overwrite: bool = False, db_path: str = "db/boundary.duckdb") -> None:
        self.init_db(db_path)

        conn = duckdb.connect(db_path)

        if overwrite:
            conn.execute("DELETE FROM relation")
        
        conn.execute('''
        INSTALL spatial;
        LOAD spatial;
        ''')
        
        insert_data: list[tuple] = list()
        for boundary in self.boundaries.values():
            insert_data.append((
                boundary.osm_id,
                boundary.name,
                boundary.name_en,
                boundary.name_zh,
                boundary.name_preference,
                boundary.admin_level,
                boundary.super_area_id_list,
                boundary.subarea_id_list,
                boundary.root_boundary_id,
                boundary.outer_boundary_id_list,
                boundary.inner_boundary_id_list,
                None,
                wkb.dumps(boundary.geom)
            ))
        conn.executemany("""
        INSERT OR REPLACE INTO relation VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromWKB(?)
        )
        """, insert_data)

        conn.close()
    
    def get_super_boundary(self, osm_id: int, super_boundary_max_admin_level: int) -> int:
        pass

    def filter_by_root_boundary(self, root_boundary_id: Optional[int] = None) -> None:
        if root_boundary_id is not None:
            self.root_boundary = root_boundary_id
            if root_boundary_id in self.boundaries:
                self.min_admin_level = self.boundaries[root_boundary_id].admin_level

            boundary_to_be_deleted = set()
            for boundary_id in self.boundaries:
                if boundary_id in self.boundaries and self.boundaries[boundary_id].root_boundary_id != root_boundary_id:
                    boundary_to_be_deleted.add(boundary_id)
            for boundary_id in boundary_to_be_deleted:
                self.remove_boundary(boundary_id)
    
    # 删除大于 admin_level 或 admin_level 为空的 boundary
    def filter_by_admin_level(self, max_admin_level: int) -> None:
        for boundary in self.boundaries.values():
            if boundary.admin_level is None or boundary.admin_level > max_admin_level:
                self.small_admin_level_boundaries.add(boundary.osm_id)
        for boundary in self.small_admin_level_boundaries:
            self.remove_boundary(boundary)

    def merge_boundaries_to_root(self, boundaries: dict[int, Boundary]) -> None:
        if self.root_boundary is None:
            return
        for osm_id, boundary in boundaries.items():
            if boundary.is_root_boundary():
                boundary.add_super_boundary(self.root_boundary)
                if osm_id not in self.boundaries[self.root_boundary].subarea_id_list:
                    self.boundaries[self.root_boundary].subarea_id_list.append(osm_id)
                # 处理新区域的边界
                root_inner_boundary_list = self.boundaries[self.root_boundary].inner_boundary_id_list
                root_outer_boundary_list = self.boundaries[self.root_boundary].outer_boundary_id_list
                subarea_inner_boundary_list = boundary.inner_boundary_id_list
                subarea_outer_boundary_list = boundary.outer_boundary_id_list
                for way in subarea_inner_boundary_list:
                    if way in root_inner_boundary_list:
                        root_inner_boundary_list.remove(way)
                    elif way in root_outer_boundary_list:
                        root_outer_boundary_list.remove(way)
                    else:
                        root_inner_boundary_list.append(way)
                for way in subarea_outer_boundary_list:
                    if way in root_inner_boundary_list:
                        root_inner_boundary_list.remove(way)
                    elif way in root_outer_boundary_list:
                        root_outer_boundary_list.remove(way)
                    else:
                        root_outer_boundary_list.append(way)

            boundary.root_boundary = self.root_boundary
            if osm_id not in self.boundaries:
                self.boundaries[osm_id] = boundary
            else:
                print(f"merge conflict with boundary: {boundary.name}({osm_id})")

    def print_status(self) -> None:
        print(f"total boundary count: {len(self.boundaries)} \n")
        print(f"root nodes:\n")
        for boundary in self.boundaries.values():
            if boundary.is_root_boundary():
                print(boundary)
    
    def print_hierarchy(self):
        has_printed: set[int] = set()
        root_boundaries: list[Boundary] = [boundary for boundary in self.boundaries.values() if boundary.is_root_boundary()]
        for i, boundary in enumerate(root_boundaries):
            self.__print_hierarchy(boundary, has_printed, "", i == len(root_boundaries) - 1)
                
    def __print_hierarchy(self, boundary: Boundary, has_printed: set[int], header: str, last: bool) -> None:
        if boundary is None or boundary.osm_id in has_printed:
            return
        elbow = "└──"
        pipe = "│  "
        tee = "├──"
        blank = "   "
        has_printed.add(boundary.osm_id)
        print(f"{header}{elbow if last else tee}{boundary.name}({boundary.admin_level})")
        children = [subarea for subarea in boundary.subarea_id_list if subarea in self.boundaries]
        for i, subarea in enumerate(children):
            self.__print_hierarchy(self.boundaries[subarea], has_printed, header+(blank if last else pipe), i == len(children) - 1)
        

if __name__ == "__main__":
    parser1 = Parser()
    parser1.parse("data/china-latest.osm.pbf", root_boundary_id=270056)
    parser2 = Parser()
    parser2.parse("data/taiwan-latest.osm.pbf", root_boundary_id=449220)
    parser1.print_status()
    parser2.print_status()
    parser1.merge_boundaries_to_root(parser2.boundaries)
    parser1.print_status()
    parser1.print_hierarchy()
    parser1.save_to_database()