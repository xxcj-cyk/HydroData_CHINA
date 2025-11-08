import os
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import matplotlib.pyplot as plt
from datetime import datetime

# 导入降雨计算方法
from rainfall_methods import arithmetic_mean, thiessen_polygon_mean, inverse_distance_weighting

def calculate_basin_rainfall(rain_station_shp, basin_shp, rain_data_csv, station_id_field='STCD', 
                            date_field='TM', rainfall_field='DRP', output_dir=None):
    """
    读取雨量站shp、流域范围面shp和雨量站csv，计算面平均降雨
    
    参数:
        rain_station_shp: 雨量站shapefile路径
        basin_shp: 流域边界shapefile路径
        rain_data_csv: 雨量站降雨数据CSV路径
        station_id_field: 站点ID字段名
        date_field: 日期字段名
        rainfall_field: 降雨量字段名
        output_dir: 输出目录，如果为None则不保存结果
        
    返回:
        包含不同方法计算的面平均降雨量的DataFrame
    """
    # 读取雨量站shapefile
    print(f"读取雨量站shapefile: {rain_station_shp}")
    stations_gdf = gpd.read_file(rain_station_shp)
    
    # 读取流域shapefile
    print(f"读取流域shapefile: {basin_shp}")
    basin_gdf = gpd.read_file(basin_shp)
    
    # 确保流域数据只有一个多边形
    if len(basin_gdf) > 1:
        print(f"警告: 流域shapefile包含多个多边形，将使用第一个多边形")
    
    basin_polygon = basin_gdf.geometry.iloc[0]
    
    # 读取降雨数据CSV
    print(f"读取降雨数据CSV: {rain_data_csv}")
    rain_data = pd.read_csv(rain_data_csv)
    
    # 确保日期字段格式正确
    if date_field in rain_data.columns:
        try:
            rain_data[date_field] = pd.to_datetime(rain_data[date_field])
        except:
            print(f"警告: 无法将{date_field}转换为日期格式，将保持原格式")
    
    # 匹配雨量站shapefile和CSV数据
    print("匹配雨量站shapefile和CSV数据")
    
    # 获取唯一日期列表
    if date_field in rain_data.columns:
        dates = rain_data[date_field].unique()
    else:
        # 如果没有日期字段，假设只有一个时间步
        dates = [None]
    
    # 创建结果DataFrame
    results = pd.DataFrame()
    
    # 对每个日期计算面平均降雨
    for date in dates:
        if date is not None:
            print(f"处理日期: {date}")
            # 筛选当前日期的数据
            current_data = rain_data[rain_data[date_field] == date]
        else:
            current_data = rain_data
        
        # 获取有效的雨量站点
        valid_stations = []
        valid_rainfall = []
        valid_points = []
        
        for _, station in stations_gdf.iterrows():
            station_id = station[station_id_field]
            
            # 在CSV中查找对应的降雨数据
            station_data = current_data[current_data[station_id_field] == station_id]
            
            if not station_data.empty and rainfall_field in station_data.columns:
                rainfall = station_data[rainfall_field].iloc[0]
                
                # 检查降雨值是否有效
                if pd.notna(rainfall) and rainfall >= 0:
                    valid_stations.append(station_id)
                    valid_rainfall.append(rainfall)
                    
                    # 获取站点坐标
                    x, y = station.geometry.x, station.geometry.y
                    valid_points.append((x, y))
        
        # 检查是否有足够的有效站点
        if len(valid_stations) < 3:
            print(f"警告: 有效站点数量不足 ({len(valid_stations)}), 至少需要3个站点进行泰森多边形和IDW计算")
            if len(valid_stations) == 0:
                print("错误: 没有有效站点，跳过当前日期")
                continue
        
        # 1. 算术平均法
        arithmetic_result = arithmetic_mean(valid_rainfall)
        
        # 2. 泰森多边形法
        thiessen_result = thiessen_polygon_mean(valid_points, valid_rainfall, basin_polygon)
        
        # 3. 反距离权重法 (IDW)
        # 创建流域内的网格点用于IDW计算
        # 这里简化处理，使用流域边界框内的均匀网格
        minx, miny, maxx, maxy = basin_polygon.bounds
        grid_size = 20  # 网格数量，可以根据需要调整
        x = np.linspace(minx, maxx, grid_size)
        y = np.linspace(miny, maxy, grid_size)
        grid_points = []
        
        for i in range(len(x)):
            for j in range(len(y)):
                point = Point(x[i], y[j])
                if basin_polygon.contains(point):
                    grid_points.append((x[i], y[j]))
        
        idw_result = inverse_distance_weighting(valid_points, valid_rainfall, grid_points)
        
        # 保存结果
        result_row = {
            'Date': date if date is not None else 'All',
            'StationCount': len(valid_stations),
            'ArithmeticMean': arithmetic_result,
            'ThiessenPolygon': thiessen_result,
            'IDW': idw_result
        }
        
        results = pd.concat([results, pd.DataFrame([result_row])], ignore_index=True)
    
    # 保存结果到CSV
    if output_dir is not None:
        os.makedirs(output_dir, exist_ok=True)
        csv_path = os.path.join(output_dir, 'basin_rainfall_results.csv')
        results.to_csv(csv_path, index=False)
        print(f"结果已保存至: {csv_path}")
    
    return results


if __name__ == "__main__":
    # 示例用法
    rain_station_shp = "path/to/rain_stations.shp"  # 雨量站shapefile路径
    basin_shp = "path/to/basin.shp"  # 流域边界shapefile路径
    rain_data_csv = "path/to/rainfall_data.csv"  # 雨量站降雨数据CSV路径
    output_dir = "results"  # 输出目录
    
    # 根据实际数据调整字段名
    station_id_field = 'STCD'  # 站点ID字段名
    date_field = 'TM'  # 日期字段名
    rainfall_field = 'DRP'  # 降雨量字段名
    
    # 计算面平均降雨
    results = calculate_basin_rainfall(
        rain_station_shp, 
        basin_shp, 
        rain_data_csv, 
        station_id_field, 
        date_field, 
        rainfall_field, 
        output_dir
    )
    
    print("\n面平均降雨计算结果:")
    print(results)