from model import *
from shapely.geometry import LineString, Polygon
from typing import Optional
from utils import *

def overpass_relation_to_boundary_dict(overpass_result: dict[str, any], name_preference: Optional[str], max_admin_level: Optional[int] = None) -> dict[int, Boundary]:
    try:
        relations = overpass_result["elements"]
        result: dict[int, Boundary] = dict()
        for relation in relations:
            if relation["type"] == "relation":
                osm_id = relation["id"]
                name = relation.get("name")
                name_en = relation.get("name:en")
                name_zh = relation.get("name:zh")
                name_prefer = relation.get(name_preference)
                admin_level = safe_cast(relation.get('admin_level'), int)
                if max_admin_level is not None and admin_level is not None and admin_level <= max_admin_level:
                    # 继续补数据，这里应该先把数据一股脑补完，再用 parser 生成以最初补数据为根节点的数据，然后把节点列表merge进去
                    pass
                subarea_id_list = [member["ref"] for member in relation["members"] if member["type"]=="relation" and member["role"]=="subarea"]
                outer_boundary_id_list = [member["ref"] for member in relation["members"] if member["type"]=="way" and member["role"]=="outer"]
                inner_boundary_id_list = [member["ref"] for member in relation["members"] if member["type"]=="way" and member["role"]=="inner"]
                result[osm_id] = Boundary(osm_id, name, name_en, name_zh, name_prefer, admin_level, subarea_id_list, outer_boundary_id_list, inner_boundary_id_list)
        return result
    except:
        return dict()

def overpass_way_to_way_dict(overpass_result: dict[str, any]) -> dict[int, Way]:
    try:
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


