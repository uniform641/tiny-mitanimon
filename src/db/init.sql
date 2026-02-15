-- for postgres/duckdb spatial extension

INSTALL spatial;
LOAD spatial;

-- table for meta information and geo boundary of a (administrative) region
CREATE TABLE IF NOT EXISTS osm_relation (
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

create index if not exists idx_geom on relation using RTREE (geom);


-- table for meta information of regional pieces of osm data
CREATE TABLE IF NOT EXISTS osm_download_source (
    osm_id BIGINT,
    name VARCHAR PRIMARY KEY,
    name_en VARCHAR,
    name_zh VARCHAR,
    name_preference VARCHAR,
    admin_level INTEGER,
    super_area_name VARCHAR,
    data_size BIGINT,
    download_link VARCHAR,
    source VARCHAR PRIMARY KEY,
    create_time TIMESTAMP,
    update_time TIMESTAMP
);
