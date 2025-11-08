"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-08-18 10:44:51
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 20:02:23
"""

import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import Voronoi
from shapely.geometry import Polygon, Point, MultiPolygon
import matplotlib.pyplot as plt

def arithmetic_mean(rainfall_values):
    """
    计算算术平均降雨量（无有效数据时返回 np.nan）
    """
    valid_values = [val for val in rainfall_values if not pd.isna(val)]
    if valid_values:
        return np.mean(valid_values)
    else:
        return np.nan


def thiessen_polygon_mean(station_points, rainfall_values, basin_polygon):
    """
    使用泰森多边形法计算面平均降雨量
    
    参数:
        station_points: 雨量站点坐标列表，格式为[(x1,y1), (x2,y2), ...]
        rainfall_values: 对应站点的降雨量列表
        basin_polygon: 流域边界多边形（shapely.geometry.Polygon对象）
        
    返回:
        泰森多边形法计算的面平均降雨量
    """
    # 有效数据筛选
    valid_indices = [i for i, val in enumerate(rainfall_values) if not pd.isna(val)]
    if not valid_indices:
        return np.nan
    # 筛选有效站点和降雨量
    station_points_valid = [station_points[i] for i in valid_indices]
    rainfall_values_valid = [rainfall_values[i] for i in valid_indices]
    # 确保输入数据是numpy数组
    points = np.array(station_points_valid)
    # 计算泰森多边形
    vor = Voronoi(points)
    # 获取泰森多边形
    regions = []
    weights = []
    total_area = 0
    weighted_sum = 0
    # 为每个站点创建泰森多边形
    for i, point in enumerate(points):
        # 获取该点的泰森多边形顶点索引
        region_idx = vor.point_region[i]
        region = vor.regions[region_idx]
        # 跳过无效区域
        if -1 in region or len(region) == 0:
            continue
        # 获取多边形顶点坐标
        polygon_vertices = [vor.vertices[j] for j in region]
        # 创建Shapely多边形
        thiessen_poly = Polygon(polygon_vertices)
        # 与流域边界相交
        intersection = thiessen_poly.intersection(basin_polygon)
        # 计算相交区域面积
        if not intersection.is_empty:
            area = intersection.area
            total_area += area
            weighted_sum += rainfall_values_valid[i] * area
            regions.append(intersection)
            weights.append(area)
    # 计算加权平均值
    if total_area > 0:
        return weighted_sum / total_area
    else:
        return np.nan