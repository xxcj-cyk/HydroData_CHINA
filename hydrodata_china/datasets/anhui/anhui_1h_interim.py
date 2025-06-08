"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-27 17:33:16
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-05-28 16:56:39
"""

import os
import pandas as pd
import glob
from pathlib import Path
import xarray as xr
import re

# 添加流域面积字典 (km²)
BASIN_AREAS = {
    "50406910": 79.03,
    "50501200": 182.15,
    "50701100": 270.2,
    "50913900": 1390.24,
    "51004350": 573.46,
    "62549024": 989,
    "62700110": 471.74,
    "62700700": 127.24,
    "62802400": 421.68,
    "62802700": 540.87,
    "62803300": 151.6,
    "62902000": 1476.69,
    "62906900": 260.63,
    "62907100": 10.79,
    "62907600": 9.42,
    "62907601": 26.99,
    "62909400": 497.64,
    "62911200": 661.01,
    "62916110": 78.85,
    "70112150": 5.08,
    "70114100": 98.83
}

def read_flood_data(root_folder):
    """读取洪水数据文件夹中的所有Excel文件"""
    all_data = {}
    
    # 获取所有子文件夹（洪水场次文件夹）
    flood_folders = [f for f in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, f))]
    
    for flood_folder in flood_folders:
        folder_path = os.path.join(root_folder, flood_folder)
        all_data[flood_folder] = {}
        
        # 获取文件夹中所有的Excel文件
        excel_files = glob.glob(os.path.join(folder_path, "*.xls")) + glob.glob(os.path.join(folder_path, "*.xlsx"))
        
        for excel_file in excel_files:
            file_name = os.path.basename(excel_file)
            try:
                # 根据文件扩展名选择适当的引擎
                if file_name.endswith('.xls'):
                    df = pd.read_excel(excel_file, engine='xlrd')
                else:
                    df = pd.read_excel(excel_file, engine='openpyxl')
                
                # 重命名列
                column_mapping = {
                    '时间': 'time',
                    '实测流量': 'streamflow_obs',  # 改名为streamflow_obs
                    '预报流量': 'streamflow_pred_xaj',
                    '面雨量': 'P_Anhui'
                }
                
                # 应用列名映射
                df = df.rename(columns=column_mapping)
                
                # 处理雨量站列名
                for col in df.columns:
                    # 使用正则表达式匹配中文名称和数字编码
                    match = re.search(r'.*?([0-9]+)$', col)
                    if match and col not in ['time', 'streamflow_obs', 'streamflow_pred_xaj', 'P_Anhui']:
                        station_code = match.group(1)
                        df = df.rename(columns={col: f'P_{station_code}'})
                
                # 将所有P_开头的列转换为float64
                for col in df.columns:
                    if col.startswith('P_'):
                        df[col] = df[col].astype('float64')

                # 提取站点编码以获取流域面积
                station_code_match = re.search(r'_([0-9]+)_', file_name)
                if station_code_match:
                    station_code = station_code_match.group(1)
                    if station_code in BASIN_AREAS and 'streamflow_obs' in df.columns:
                        # 计算streamflow (mm/d)
                        # 公式: Q(mm/d) = Q(m³/s) * 86400 / (面积(km²) * 10^6) * 1000
                        # 简化后: Q(mm/d) = Q(m³/s) * 86.4 / 面积(km²)
                        basin_area = BASIN_AREAS[station_code]
                        # 对于小时数据，需要除以24(转换成小时单位)
                        df['streamflow'] = df['streamflow_obs'] * 86.4 / basin_area / 24
                    else:
                        print(f"警告: 无法为 {file_name} 找到匹配的流域面积或缺少streamflow_obs列")

                # 检查必要的列是否存在
                if 'time' in df.columns and 'streamflow_obs' in df.columns:
                    all_data[flood_folder][file_name] = df
                    print(f"成功读取: {flood_folder}/{file_name}")
                else:
                    print(f"警告: {excel_file} 缺少必要的列 ('time', 'streamflow_obs')")
            except Exception as e:
                print(f"错误: 无法读取 {excel_file}. 错误信息: {str(e)}")
    
    return all_data

def process_flood_data(data_dict):
    """处理读取的洪水数据"""
    # 统计每个场次的文件数量和数据行数
    stats = {}
    total_files = 0
    total_rows = 0
    
    for flood_name, files_data in data_dict.items():
        files_count = len(files_data)
        rows_count = sum(df.shape[0] for df in files_data.values())
        
        stats[flood_name] = {
            '文件数量': files_count,
            '数据总行数': rows_count
        }
        
        total_files += files_count
        total_rows += rows_count
    
    # 添加总体统计信息
    stats['总计'] = {
        '文件数量': total_files,
        '数据总行数': total_rows
    }
    
    return stats

def save_as_netcdf(data_dict, output_dir):
    """将数据保存为netCDF格式"""
    os.makedirs(output_dir, exist_ok=True)
    
    for flood_folder, files_data in data_dict.items():
        for file_name, df in files_data.items():
            # 提取站点编码
            station_code = re.search(r'_([0-9]+)_', file_name).group(1)
            # 提取时间戳
            timestamp = re.search(r'_([0-9]+)\.xls', file_name).group(1)
            
            # 创建新的文件名
            new_filename = f"Anhui_{station_code}_{timestamp}.nc"
            output_path = os.path.join(output_dir, new_filename)
            
            # 转换为xarray数据集
            ds = df.set_index('time').to_xarray()
            
            # 保存为netCDF文件
            ds.to_netcdf(output_path)
            print(f"已保存: {output_path}")

def main():
    # 设置根文件夹路径
    root_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    
    # 读取所有数据
    print("开始读取洪水数据...")
    flood_data = read_flood_data(root_folder)
    
    # 输出基本统计信息
    print("\n数据读取完成。基本统计信息:")
    print(f"共读取了 {len(flood_data)} 个洪水场次")
    
    total_files = sum(len(files) for files in flood_data.values())
    print(f"共读取了 {total_files} 个Excel文件")
    
    # 处理数据并输出统计信息
    stats = process_flood_data(flood_data)
    print("\n各场次统计信息:")
    for flood_name, stat in stats.items():
        if flood_name == '总计':
            print("\n总体统计:")
        print(f"{flood_name}: 包含{stat['文件数量']}个文件，共{stat['数据总行数']}行数据")
    
    # 保存为netCDF格式
    print("\n开始保存为netCDF格式...")
    save_as_netcdf(flood_data, output_dir)
    
    print("\n数据处理完成")
    return flood_data

if __name__ == "__main__":
    data = main()