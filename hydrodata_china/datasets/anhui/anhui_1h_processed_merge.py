"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 17:31:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-10 15:30:50
"""

import os
import glob
import re
import xarray as xr
import logging
from collections import defaultdict
import warnings

warnings.filterwarnings("ignore", message="invalid value encountered in cast")

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
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


def reorder_dataset(ds):
    """
    重新排序Dataset的dimensions和data variables，同时将time_true的数据类型从datetime转换为字符串
    
    参数:
        ds: 输入的xarray.Dataset
        
    返回:
        reordered_ds: 重新排序后的xarray.Dataset，且time_true为字符串类型
    """
    # 定义期望的变量顺序
    desired_var_order = [
        'P_Anhui',
        'P_50406910',
        'P_50436450',
        'P_50436550',
        'P_50436650',
        'P_ERA5-Land',
        'ET_Anhui',
        'ET_ERA5-Land',
        'PET_ERA5-Land',
        'T_ERA5-Land',
        'streamflow_obs',
        'streamflow_pred_xaj',
        'streamflow',     
        'time_true'
    ]
    # 创建新的Dataset，保持原有的coordinates
    reordered_ds = xr.Dataset(coords={'basin': ds.basin, 'time': ds.time})
    # 按照期望的顺序添加变量
    for var_name in desired_var_order:
        if var_name in ds:
            if var_name == 'time_true':
                # 将time_true转换为字符串类型
                # 先将datetime转为字符串，然后创建为新的变量
                time_str_values = ds[var_name].dt.strftime('%Y-%m-%d %H:%M:%S').values
                reordered_ds[var_name] = xr.DataArray(
                    time_str_values,
                    dims=ds[var_name].dims,
                    coords=ds[var_name].coords
                )
                # 设置属性指明这是字符串表示的时间
                reordered_ds[var_name].attrs['description'] = 'String representation of datetime'
            else:
                reordered_ds[var_name] = ds[var_name]
    # 保持原有的属性
    reordered_ds.attrs.update(ds.attrs)
    return reordered_ds


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
        return [], []
    
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
        
        for file_path in files:
            ds = xr.open_dataset(file_path)
            # 提取事件ID作为新的维度
            event_id = os.path.basename(file_path).split('.')[0]  # 去掉.nc后缀
            ds = ds.expand_dims({"basin": [event_id]})
            datasets.append(ds)
        
        if not datasets:
            logging.warning(f"流域 {basin_id} 没有有效的数据集，跳过")
            failed_basins.append(basin_id)
            continue
        
        # 合并数据集
        merged_ds = xr.concat(datasets, dim="basin")
        # 添加流域ID作为全局属性
        merged_ds.attrs["basin_id"] = basin_id
        merged_ds.attrs["flood_event_count"] = len(files)
        
        # 添加变量单位信息
        if 'streamflow' in merged_ds:
            merged_ds['streamflow'].attrs['units'] = 'mm/h'
        if 'streamflow_obs' in merged_ds:
            merged_ds['streamflow_obs'].attrs['units'] = 'm3/s'
        if 'streamflow_pred_xaj' in merged_ds:
            merged_ds['streamflow_pred_xaj'].attrs['units'] = 'm3/s'
        if 'P_Anhui' in merged_ds:
            merged_ds['P_Anhui'].attrs['units'] = 'mm/h'
        if 'evaporation' in merged_ds:
            merged_ds = merged_ds.rename({'evaporation': 'ET_Anhui'})
            merged_ds['ET_Anhui'].attrs['units'] = 'mm/h'
        if 'total_precipitation_hourly' in merged_ds:
            merged_ds = merged_ds.rename({'total_precipitation_hourly': 'P_ERA5-Land'})
            merged_ds['P_ERA5-Land'].attrs['units'] = 'mm/h'
        if 'potential_evaporation_hourly' in merged_ds:
            merged_ds = merged_ds.rename({'potential_evaporation_hourly': 'PET_ERA5-Land'})
            merged_ds['PET_ERA5-Land'].attrs['units'] = 'mm/h'
        if 'total_evaporation_hourly' in merged_ds:
            merged_ds = merged_ds.rename({'total_evaporation_hourly': 'ET_ERA5-Land'})
            merged_ds['ET_ERA5-Land'].attrs['units'] = 'mm/h'
        if 'temperature_2m' in merged_ds:
            merged_ds = merged_ds.rename({'temperature_2m': 'T_ERA5-Land'})
            merged_ds['T_ERA5-Land'].attrs['units'] = '°C'
        # 获取第一个和最后一个事件ID
        event_ids = [os.path.basename(file_path).split('.')[0] for file_path in files]
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
        # 重新排序dimensions和variables
        merged_ds = reorder_dataset(merged_ds)
        # 保存合并后的nc文件
        output_file = os.path.join(output_folder, output_filename)
        merged_ds.to_netcdf(output_file)
        logging.info(f"已将流域 {basin_id} 的 {len(files)} 个文件合并为 {output_file}")
        success_basins.append(basin_id)
    
    # 输出处理结果摘要
    logging.info(f"成功处理的流域数量: {len(success_basins)}")
    if success_basins:
        logging.info(f"成功处理的流域ID: {', '.join(success_basins)}")
    logging.info(f"处理失败的流域数量: {len(failed_basins)}")
    if failed_basins:
        logging.info(f"处理失败的流域ID: {', '.join(failed_basins)}")

    return success_basins, failed_basins    

if __name__ == "__main__":
    input_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H"
    # 执行合并操作
    success_basins, failed_basins = merge_nc_files_by_basin(input_folder, output_folder)
    logging.info("处理完成！")