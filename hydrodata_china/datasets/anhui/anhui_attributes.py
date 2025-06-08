"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 00:10:23
@Last Modified by:  Yikai CHAI
@Last Modified time:2025-05-29 10:28:58
"""

import os
import glob
import pandas as pd
import re
import xarray as xr
import numpy as np

def list_nc_files(directory_path):
    """
    读取指定目录下的所有nc文件并记录它们的名称（不含扩展名）
    
    参数:
        directory_path: 包含nc文件的目录路径
        
    返回:
        nc_files: 包含所有nc文件名称（不含扩展名）的列表
    """
    # 确保路径存在
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"目录 {directory_path} 不存在")
    
    # 使用glob获取所有nc文件
    nc_files_pattern = os.path.join(directory_path, "*.nc")
    nc_files = glob.glob(nc_files_pattern)
    
    # 提取文件名（不包含路径和扩展名）
    nc_filenames = [os.path.splitext(os.path.basename(file))[0] for file in nc_files]
    
    print(f"在 {directory_path} 中找到 {len(nc_filenames)} 个nc文件")
    
    return nc_filenames

def extract_basin_id_from_filename(filename):
    """
    从文件名中提取流域ID
    例如：从Anhui_62935423_2017092321提取出anhui_62935423
    
    参数:
        filename: nc文件名（不含扩展名）
        
    返回:
        basin_id: 小写形式的流域ID
    """
    # 使用正则表达式提取流域ID
    match = re.match(r'(?i)(Anhui_\d+)_\d+', filename)
    if match:
        # 转换为小写以匹配attributes.csv中的格式
        return match.group(1).lower()
    return None

def match_attributes_to_basins(nc_filenames, attributes_file):
    """
    匹配流域文件名与属性数据
    
    参数:
        nc_filenames: nc文件名列表
        attributes_file: 包含流域属性的CSV文件路径
        
    返回:
        matched_data: 包含文件名及其对应属性的DataFrame
    """
    # 读取属性数据
    try:
        attributes_df = pd.read_csv(attributes_file)
        print(f"成功读取属性文件，包含 {len(attributes_df)} 条记录")
    except Exception as e:
        print(f"读取属性文件时出错: {e}")
        return None
    
    # 创建结果列表
    result_data = []
    
    # 匹配每个文件名与属性
    for filename in nc_filenames:
        basin_id = extract_basin_id_from_filename(filename)
        if basin_id:
            # 在属性数据中查找匹配的流域ID
            basin_attributes = attributes_df[attributes_df['basin_id'] == basin_id]
            
            if not basin_attributes.empty:
                # 创建一行数据，包括文件名和属性
                row_data = {'basin': filename}
                for col in attributes_df.columns:
                    if col != 'basin_id':
                        row_data[col] = basin_attributes[col].values[0]
                
                result_data.append(row_data)
            else:
                print(f"警告: 无法找到与 {basin_id} 匹配的属性")
        else:
            print(f"警告: 无法从 {filename} 提取流域ID")
    
    # 创建结果DataFrame
    if result_data:
        return pd.DataFrame(result_data)
    else:
        print("警告: 没有匹配到任何数据")
        return None

def create_basin_attributes_nc(matched_data, output_nc_file):
    """
    将流域属性数据保存为NetCDF格式
    
    参数:
        matched_data: 包含流域文件名及属性的DataFrame
        output_nc_file: 输出的NetCDF文件路径
    """
    if matched_data is None or matched_data.empty:
        print("错误: 没有数据可供保存")
        return False
    
    # 创建数据集
    ds = xr.Dataset()
    
    # 添加基本坐标
    basins = matched_data['basin'].values
    ds.coords['basin'] = basins
    
    # 添加属性变量
    for col in matched_data.columns:
        if col != 'basin':
            # 确保数据是数值型
            try:
                values = matched_data[col].astype(float).values
                ds[col] = xr.DataArray(values, coords=[ds.basin], dims=['basin'])
                # 添加属性描述
                ds[col].attrs['long_name'] = col
                ds[col].attrs['units'] = '-'  # 可以根据实际情况修改
            except Exception as e:
                print(f"警告: 无法将 {col} 转换为数值型数据: {e}")
    
    # 添加全局属性
    ds.attrs['title'] = 'Anhui Basin Attributes'
    ds.attrs['description'] = 'Static attributes for Anhui basins'
    ds.attrs['created_by'] = 'Yikai CHAI'
    ds.attrs['creation_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 保存为NetCDF文件
    try:
        ds.to_netcdf(output_nc_file)
        print(f"流域属性数据已成功保存为NetCDF文件: {output_nc_file}")
        return True
    except Exception as e:
        print(f"保存NetCDF文件时出错: {e}")
        return False

if __name__ == "__main__":
    # 设置目录路径
    nc_directory_path = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    attributes_file = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\attributes.csv"
    
    # 获取nc文件列表（不含扩展名）
    nc_files = list_nc_files(nc_directory_path)
    
    # 匹配文件与属性
    matched_data = match_attributes_to_basins(nc_files, attributes_file)
    
    if matched_data is not None:
        # 输出结果预览
        print("\n匹配结果预览:")
        print(matched_data.head())
        
        # 保存结果到NetCDF文件
        output_nc = os.path.join(os.path.dirname(nc_directory_path), "attributes.nc")
        create_basin_attributes_nc(matched_data, output_nc)
    else:
        print("未能生成匹配结果")

