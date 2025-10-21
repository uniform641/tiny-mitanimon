import overpass

class OverpassHelper():
    def __init__(self, endpoint: str = "https://overpass-api.de/api/interpreter", timeout: int = 25):
        self.api = overpass.API(endpoint = endpoint, timeout = timeout)
    
    def get_ways(self, way_ids: list[int]):
        if len(way_ids) == 0:
            return
        way_ids_str = ",".join([str(way_id) for way_id in way_ids])
        return self.api.get(f'way(id:{way_ids_str});', responseformat='json', verbosity='geom')
    
    def get_relations(self, relation_ids: list[int]):
        if len(relation_ids) == 0:
            return
        relation_ids_str = ",".join([str(relation_id) for relation_id in relation_ids])
        return self.api.get(f'relation(id:{relation_ids_str});', responseformat='json', verbosity='geom')