from dataclasses import dataclass
from typing import NamedTuple

@dataclass
class Boundary:
    osm_id: int
    name: str
    name_en: str
    name_zh: str
    name_preference: str
    admin_level: int
    super_area_id_list: list[int]
    subarea_id_list: list[int]
    root_boundary_id: int
    root_boundary_candidate_id_list: list[int]
    outer_boundary_id_list: list[int]
    inner_boundary_id_list: list[int]
    boundary: any

    def __init__(self, osm_id, name, name_en, name_zh, name_preference, admin_level, subarea_id_list, outer_boundary_id_list, inner_boundary_id_list):
        self.osm_id = osm_id
        self.name = name
        self.name_en = name_en
        self.name_zh = name_zh
        self.name_preference = name_preference
        self.admin_level = admin_level
        self.super_area_id_list = list([osm_id])
        self.subarea_id_list = subarea_id_list
        self.root_boundary_candidate_id_list = list([osm_id])
        self.outer_boundary_id_list = outer_boundary_id_list
        self.inner_boundary_id_list = inner_boundary_id_list
        self.geom = None

    def __repr__(self):
        return f"{self.name}({self.osm_id}), name_en: {self.name_en}, name_zh: {self.name_zh}, admin_level: {self.admin_level}\n"
    
    def is_root_boundary(self) -> bool:
        return len(self.super_area_id_list) == 1 and self.super_area_id_list[0] == self.osm_id
    
    def add_super_boundary(self, super_boundary_id: int) -> None:
        if self.is_root_boundary():
            self.super_area_id_list = list([super_boundary_id])
        else:
            self.super_area_id_list.append(super_boundary_id)

    def add_root_boundary_candidate(self, root_boundary_candidate_id_list: list[int]) -> None:
        if self.is_root_boundary():
            self.root_boundary_candidate_id_list = list(root_boundary_candidate_id_list)
        else:
            self.root_boundary_candidate_id_list += root_boundary_candidate_id_list


class Way(NamedTuple):
    osm_id: int
    geom: any
    close: bool
