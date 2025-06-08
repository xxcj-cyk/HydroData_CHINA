"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 17:31:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-05-29 17:31:00
"""

import os
import glob
import re
import numpy as np
import pandas as pd
import xarray as xr
import logging
from collections import defaultdict
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("merge_nc_files.log"),
        logging.StreamHandler()
    ]
)

def extract_basin_id(filename):
    """
    从文件名中提取流域ID
    
    参数:
        filename: 文件名
        
    返回:
        basin_id: 流域ID
    """
    # 从文件名中提取流域ID（假设文件名格式为Anhui_XXXXXXXX_YYYYMMDD.nc）
    match = re.search(r'Anhui_([0-9]+)_', os.path.basename(filename))
    if match:
        return match.group(1)
    return None


def merge_nc_files_by_basin(input_folder, output_folder):
    """
    按流域ID合并nc文件
    
    参数:
        input_folder: 输入文件夹路径，包含所有nc文件
        output_folder: 输出文件夹路径，用于保存合并后的nc文件
    """
    # 确保输出文件夹存在
    os.makedirs(output_folder, exist_ok=True)
    
    # 获取所有nc文件
    nc_files = glob.glob(os.path.join(input_folder, "*.nc"))
    
    if not nc_files:
        logging.error(f"错误: 在 {input_folder} 中未找到任何nc文件")
        return
    
    logging.info(f"共找到 {len(nc_files)} 个nc文件待处理")
    
    # 按流域ID分组
    basin_files = defaultdict(list)
    for file_path in nc_files:
        basin_id = extract_basin_id(file_path)
        if basin_id:
            basin_files[basin_id].append(file_path)
    
    logging.info(f"共找到 {len(basin_files)} 个不同的流域")
    
    # 记录成功和失败的流域
    success_basins = []
    failed_basins = []
    
    # 处理每个流域的文件
    for basin_id, files in basin_files.items():
        logging.info(f"处理流域 {basin_id}，共 {len(files)} 个文件")
        
        # 读取该流域的所有nc文件
        datasets = []
        problem_files = []
        
        for file_path in files:
            try:
                ds = xr.open_dataset(file_path)
                # 提取事件ID作为新的维度
                event_id = os.path.basename(file_path).split('.')[0]  # 去掉.nc后缀
                ds = ds.expand_dims({"basin": [event_id]})
                datasets.append(ds)
            except Exception as e:
                logging.error(f"读取文件 {file_path} 时出错: {e}")
                problem_files.append(file_path)
        
        if not datasets:
            logging.warning(f"流域 {basin_id} 没有有效的数据集，跳过")
            failed_basins.append(basin_id)
            continue
        
        # 合并数据集
        try:
            merged_ds = xr.concat(datasets, dim="basin")
            
            # 添加流域ID作为全局属性
            merged_ds.attrs["basin_id"] = basin_id
            merged_ds.attrs["flood_event_count"] = len(files)
            merged_ds.attrs["problem_files_count"] = len(problem_files)
            
            # 添加PET_Anhui变量并赋值为0
            merged_ds["PET_Anhui"] = xr.zeros_like(merged_ds["P_Anhui"] if "P_Anhui" in merged_ds else merged_ds[list(merged_ds.data_vars)[0]])
            merged_ds["PET_Anhui"].attrs["units"] = "mm/h"
            merged_ds["PET_Anhui"].attrs["long_name"] = "Potential Evapotranspiration"
            
            # 添加变量单位信息
            if 'streamflow' in merged_ds:
                merged_ds['streamflow'].attrs['units'] = 'mm/h'
            if 'streamflow_obs' in merged_ds:
                merged_ds['streamflow_obs'].attrs['units'] = 'm3/s'
            if 'streamflow_pred_xaj' in merged_ds:
                merged_ds['streamflow_pred_xaj'].attrs['units'] = 'm3/s'
            if 'P_Anhui' in merged_ds:
                merged_ds['P_Anhui'].attrs['units'] = 'mm/h'
            
            # 获取第一个和最后一个事件ID
            event_ids = [os.path.basename(file_path).split('.')[0] for file_path in files if os.path.basename(file_path).split('.')[0] not in [os.path.basename(pf).split('.')[0] for pf in problem_files]]
            if event_ids:
                # 按照时间戳排序事件ID（假设格式为Anhui_XXXXXXXX_YYYYMMDD）
                event_ids.sort(key=lambda x: x.split('_')[-1] if len(x.split('_')) >= 3 else '')
                first_event_id = event_ids[0]
                last_event_id = event_ids[-1]
                
                # 创建新的文件名格式：timeseries_1h_batch_第一个场次的id_最后一个场次的id
                output_filename = f"timeseries_1h_batch_{first_event_id}_{last_event_id}.nc"
            else:
                # 如果没有有效的事件ID，使用流域ID作为备选
                output_filename = f"{basin_id}.nc"
            
            # 保存合并后的nc文件
            output_file = os.path.join(output_folder, output_filename)
            merged_ds.to_netcdf(output_file)
            logging.info(f"已将流域 {basin_id} 的 {len(files)} 个文件合并为 {output_file}")
            success_basins.append(basin_id)
        except Exception as e:
            logging.error(f"合并流域 {basin_id} 的文件时出错: {e}")
            failed_basins.append(basin_id)
    
    # 输出处理结果摘要
    logging.info("\n处理完成！摘要:")
    logging.info(f"成功处理的流域数量: {len(success_basins)}")
    logging.info(f"成功处理的流域ID: {', '.join(success_basins)}")
    logging.info(f"处理失败的流域数量: {len(failed_basins)}")
    logging.info(f"处理失败的流域ID: {', '.join(failed_basins)}")

    return success_basins, failed_basins


def check_problematic_files(input_folder, basin_id):
    """
    检查特定流域的问题文件
    
    参数:
        input_folder: 输入文件夹路径
        basin_id: 流域ID
    """
    nc_files = glob.glob(os.path.join(input_folder, f"*{basin_id}*.nc"))
    
    if not nc_files:
        logging.error(f"未找到流域 {basin_id} 的文件")
        return
    
    logging.info(f"检查流域 {basin_id} 的 {len(nc_files)} 个文件")
    
    for file_path in nc_files:
        try:
            # 尝试读取文件并检查其结构
            ds = xr.open_dataset(file_path)
            logging.info(f"文件 {os.path.basename(file_path)} 可以正常打开")
            logging.info(f"变量: {list(ds.variables)}")
            logging.info(f"维度: {ds.dims}")
            
            # 检查时间变量
            if 'time' in ds.dims:
                time_var = ds['time']
                logging.info(f"时间变量类型: {time_var.dtype}")
                logging.info(f"时间变量值: {time_var.values}")
            
            ds.close()
        except Exception as e:
            logging.error(f"检查文件 {file_path} 时出错: {e}")


def main():
    # 设置输入和输出文件夹路径
    input_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Merged"
    
    # 执行合并操作
    success_basins, failed_basins = merge_nc_files_by_basin(input_folder, output_folder)
    
    # 如果有失败的流域，可以检查第一个失败的流域的文件
    if failed_basins:
        logging.info(f"\n开始检查第一个失败的流域 {failed_basins[0]} 的文件...")
        check_problematic_files(input_folder, failed_basins[0])
    
    logging.info("处理完成！")


if __name__ == "__main__":
    main()