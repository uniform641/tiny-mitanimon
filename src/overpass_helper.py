import overpass
from shapely.geometry import LineString, Polygon
from typing import Any, Optional
from model import *
from utils import *


class OverpassHelper():
    def __init__(self, endpoint: str = "https://overpass-api.de/api/interpreter", timeout: int = 30, max_retry: int = 3):
        self.api = overpass.API(endpoint = endpoint, timeout = timeout)
        self.max_retry = max_retry
    
    def get_ways(self, way_ids: list[int]):
        if len(way_ids) == 0:
            return dict()
        way_ids_str = ",".join([str(way_id) for way_id in way_ids])
        for _ in range(self.max_retry):
            try:
                result = self.api.get(f'way(id:{way_ids_str});', responseformat='json', verbosity='geom')
                if result and result['elements']:
                    return result
            except:
                pass
        print(f"overpass get ways fail with retry={self.max_retry}")
        raise Exception('OverpassRequestError')
    
    def get_relations(self, relation_ids: list[int]):
        if len(relation_ids) == 0:
            return dict()
        relation_ids_str = ",".join([str(relation_id) for relation_id in relation_ids])
        for _ in range(self.max_retry):
            try:
                result = self.api.get(f'relation(id:{relation_ids_str});', responseformat='json', verbosity='body')
                if result and result['elements']:
                    return result
            except:
                pass
        print(f"overpass get relations fail with retry={self.max_retry}")
        raise Exception('OverpassRequestError')
    
    def get_reverse_geocoding(self, lon: float, lat: float, name_preference: Optional[str] = None):
        try:
            result = self.api.get(f'is_in({lat},{lon});relation(pivot)[boundary=administrative];', responseformat='json', verbosity='tags')
            if result and result['elements']:
                ancestor_boundary_list: list[Boundary] = list()
                for relation in result['elements']:
                    osm_id = relation['id']
                    tags = relation['tags']
                    name = tags.get('name')
                    name_en = tags.get('name:en')
                    name_zh = tags.get('name:zh')
                    name_prefer = tags.get(name_preference)
                    admin_level = safe_cast(tags.get('admin_level'), int)
                    boundary = Boundary(osm_id, name, name_en, name_zh, name_prefer, admin_level, [], [], [])
                    ancestor_boundary_list.append(boundary)
                return ancestor_boundary_list
            else:
                return []
        except Exception as e:
            print(e)
        print(f"overpass get relations fail with retry={self.max_retry}")
        return []

    def build_relation_tree_from_root_relation(self, name_preference: str, max_admin_level: int,
                                               root_relation_list: list[int]) -> dict[int, Boundary]:
        print(f"relation to be fixed: {root_relation_list}")
        relation_tree: dict[int, Boundary] = dict()
        relation_to_parent: dict[int, list[int]] = dict()
        relation_to_query: list[int] = list(root_relation_list)
        # 防止出现环导致无限循环
        epoch: int = 0

        while epoch < 10 and relation_to_query:
            epoch += 1
            try:
                overpass_result: dict[str, Any] = self.get_relations(relation_to_query)
                relation_to_query.clear()
                if not overpass_result or not overpass_result['elements']:
                    continue 
                relations = overpass_result["elements"]
                for relation in relations:
                    if relation["type"] == "relation":
                        osm_id = relation["id"]
                        tags = relation["tags"]
                        name = tags.get("name")
                        name_en = tags.get("name:en")
                        name_zh = tags.get("name:zh")
                        name_prefer = tags.get(name_preference)
                        admin_level = safe_cast(tags.get('admin_level'), int)
                        subarea_id_list = [member["ref"] for member in relation["members"] if member["type"]=="relation" and member["role"]=="subarea"]
                        outer_boundary_id_list = [member["ref"] for member in relation["members"] if member["type"]=="way" and member["role"]=="outer"]
                        inner_boundary_id_list = [member["ref"] for member in relation["members"] if member["type"]=="way" and member["role"]=="inner"]
                        boundary = Boundary(osm_id, name, name_en, name_zh, name_prefer, admin_level,
                                            subarea_id_list, outer_boundary_id_list, inner_boundary_id_list)
                        boundary.super_area_id_list = relation_to_parent.setdefault(osm_id, list())
                        if admin_level <= max_admin_level:
                            relation_tree[osm_id] = boundary

                        # 继续补数据，这里应该先把数据一股脑补完，再用 parser 生成以最初补数据为根节点的数据，然后把节点列表merge进去
                        if admin_level is not None and admin_level < max_admin_level:
                            for subarea in subarea_id_list:
                                relation_to_parent.setdefault(subarea, list()).append(osm_id)
                                relation_to_query.append(subarea)

            except Exception as e:
                print(f"fail to get and parse overpass api result. {e}")
        print(f"total relation fetched from overpass api: {len(relation_tree)}")
        return relation_tree
    
    def build_way_dict(self, way_ids: list[int]) -> dict[int, Way]:
        # TODO: paging & size limit
        try:
            overpass_result = self.get_ways(way_ids)
            ways = overpass_result["elements"]
            result: dict[int, Way] = dict()
            for way in ways:
                if way["type"] == "way":
                    coords = [(p['lon'], p['lat']) for p in way["geometry"]]
                    close = False
                    if len(coords) > 3 and coords[0] == coords[-1]:
                        geom = Polygon(coords)
                        close = True
                    else:
                        geom = LineString(coords)
                    result[way["id"]] = Way(way["id"], geom, close)
            return result
        except:
            return dict()
