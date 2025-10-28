import geopandas as gpd
import matplotlib.pyplot as plt

from model import *

def plot_multipolygon_collection(multipolygon_list, save_path="large_boundaries.png"):
    """
    针对大量 MultiPolygon 的优化绘制方法
    """
    fig, ax = plt.subplots(figsize=(14, 12))
    
    # 创建 GeoDataFrame
    gdf = gpd.GeoDataFrame(
        geometry=multipolygon_list,
        crs="EPSG:4326"
    )
    
    # 对于大量区域，使用简单的颜色和较细的边界
    gdf.plot(
        ax=ax,
        color='lightblue',
        alpha=0.5,         # 降低透明度以避免颜色重叠
        edgecolor='darkblue',
        linewidth=0.3,     # 更细的边界线
    )
    
    # 设置图形属性
    ax.set_aspect('equal')
    ax.set_xlabel('经度')
    ax.set_ylabel('纬度')
    ax.set_title(f'行政区边界图\n共 {len(multipolygon_list)} 个区域')
    
    # 添加统计信息文本
    stats_text = f'总区域数: {len(multipolygon_list)}'
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes, 
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
            verticalalignment='top')
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.show()
    
    print(f"成功绘制 {len(multipolygon_list)} 个区域")

def plot_boundary(boundary_dict: dict[int, Boundary]) -> None:
    multipolygon_list = list()
    for boundary in boundary_dict.values():
        multipolygon_list.append(boundary.geom)
    plot_multipolygon_collection(multipolygon_list, "boundary.png")