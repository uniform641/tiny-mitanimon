import geopandas as gpd
import matplotlib.pyplot as plt

from model import *

def plot_multipolygon_collection(multipolygon_list: list[MultiPolygon],
                                 highlight_multipolygon_list: list[MultiPolygon],
                                 save_path="boundaries.png") -> None:
    fig, ax = plt.subplots(figsize=(70, 60))
    
    gdf1 = gpd.GeoDataFrame(
        geometry=multipolygon_list,
        crs="EPSG:4326"
    )
    gdf1.plot(
        ax=ax,
        color='lightblue',
        alpha=0.5,
        edgecolor='darkblue',
        linewidth=0.3,
    )

    if highlight_multipolygon_list:
        gdf2 = gpd.GeoDataFrame(
            geometry=highlight_multipolygon_list,
            crs="EPSG:4326"
        )
        gdf2.plot(
            ax=ax,
            color='lightcoral',
            alpha=0.5,
            edgecolor='coral',
            linewidth=0.5,
        )
    
    ax.set_aspect('equal')
    ax.set_xlabel('longitude')
    ax.set_ylabel('latitude')
    ax.set_title(f'boundary graph with {len(multipolygon_list)} boundaries')
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
    
    print(f"plot {len(multipolygon_list)} boundaries. saved as {save_path}")

def plot_boundary(boundary_dict: dict[int, Boundary]) -> None:
    multipolygon_list = list()
    for boundary in boundary_dict.values():
        multipolygon_list.append(boundary.geom)
    plot_multipolygon_collection(multipolygon_list, [], "boundary.png")

def plot_boundary_with_highlight(boundary_dict: dict[int, Boundary],
                                 highlight_boundary_id_list: list[int]) -> None:
    highlight_boundary_id_set = set(highlight_boundary_id_list)
    multipolygon_list = list()
    highlight_boundary_list = list()
    for boundary_id, boundary in boundary_dict.items():
        if boundary_id in highlight_boundary_id_set:
            highlight_boundary_list.append(boundary.geom)
        else:
            multipolygon_list.append(boundary.geom)
    plot_multipolygon_collection(multipolygon_list, 
                                 highlight_boundary_list, "boundary.png")