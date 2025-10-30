from parser import *
from plot import *

def test1():
    parser1 = OsmAdminBoundaryParser()
    parser1.parse("data/china-latest.osm.pbf", root_boundary_id=270056)
    parser2 = OsmAdminBoundaryParser()
    parser2.parse("data/taiwan-latest.osm.pbf", root_boundary_id=449220)
    parser1.print_status()
    parser2.print_status()
    parser1.merge_boundaries_to_root(parser2.boundaries)
    parser1.print_status()
    parser1.print_hierarchy()
    parser1.save_to_database()

def test2():
    parser1 = OsmAdminBoundaryParser()
    parser1.parse("data/china-latest.osm.pbf", root_boundary_id=270056)
    parser2 = OsmAdminBoundaryParser()
    parser2.parse("data/taiwan-latest.osm.pbf", root_boundary_id=449220)
    parser1.print_status()
    parser2.print_status()
    parser1.merge_boundaries_to_root(parser2.boundaries)
    plot_boundary_with_highlight(parser1.boundaries, parser1.relation_fixed)


if __name__ == "__main__":
    #test1()
    test2()