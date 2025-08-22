"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 17:31:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 11:30:37
"""

import os
import glob
import re
import pandas as pd
import logging
from collections import defaultdict
import warnings
import xarray as xr
 # 已不再需要 xarray


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


def merge_csv_files_by_basin(input_folder, output_folder):
    """
    按流域ID合并csv文件
    参数:
        input_folder: 输入文件夹路径，包含所有csv文件
        output_folder: 输出文件夹路径，用于保存合并后的csv文件
    """
    os.makedirs(output_folder, exist_ok=True)
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    if not csv_files:
        logging.error(f"错误: 在 {input_folder} 中未找到任何csv文件")
        return [], []
    logging.info(f"共找到 {len(csv_files)} 个csv文件待处理")
    basin_files = defaultdict(list)
    for file_path in csv_files:
        basin_id = extract_basin_id(file_path)
        if basin_id:
            basin_files[basin_id].append(file_path)
    logging.info(f"共找到 {len(basin_files)} 个不同的流域")
    success_basins = []
    failed_basins = []
    for basin_id, files in basin_files.items():
        logging.info(f"处理流域 {basin_id}，共 {len(files)} 个文件")
        dfs = []
        event_ids = []
        for file_path in files:
            try:
                df = pd.read_csv(file_path)
                event_id = os.path.basename(file_path).split('.')[0]
                # 不再添加 event_id 列
                dfs.append(df)
                event_ids.append(event_id)
            except Exception as e:
                logging.warning(f"文件 {file_path} 读取失败: {e}")
        if not dfs:
            logging.warning(f"流域 {basin_id} 没有有效的数据，跳过")
            failed_basins.append(basin_id)
            continue
        merged_df = pd.concat(dfs, ignore_index=True)
        # 统一重命名字段
        rename_dict = {
            'streamflow_obs_mm': 'streamflow',
            'total_precipitation_hourly_era5land': 'p_era5land',
            'potential_evaporation_hourly_era5land': 'pet_era5land',
            'total_evaporation_hourly_era5land': 'et_era5land',
            'temperature_2m_era5land': 't_era5land',
        }
        merged_df = merged_df.rename(columns=rename_dict)
        if event_ids:
            event_ids.sort(key=lambda x: x.split('_')[-1] if len(x.split('_')) >= 3 else '')
            first_event_id = event_ids[0]
            last_event_id = event_ids[-1]
            output_filename = f"timeseries_1h_batch_{first_event_id}_{last_event_id}"
        else:
            output_filename = f"{basin_id}"
        # 保存csv（已重命名字段）
        output_file_csv = os.path.join(output_folder, output_filename + ".csv")
        merged_df.to_csv(output_file_csv, index=False)
        logging.info(f"已将流域 {basin_id} 的 {len(files)} 个文件合并为 {output_file_csv}")
        # 保存nc（已重命名字段）
        ds = xr.Dataset.from_dataframe(merged_df)
        if 'streamflow_obs_m3s' in ds:
            ds['streamflow_obs_m3s'].attrs['units'] = 'm3/s'
        if 'streamflow' in ds:
            ds['streamflow'].attrs['units'] = 'mm/h'
        if 'p_Anhui' in ds:
            ds['p_Anhui'].attrs['units'] = 'mm/h'
        if 'pet_anhui' in ds:
            ds['pet_anhui'].attrs['units'] = 'mm/h'
        if 'p_era5land' in ds:
            ds['p_era5land'].attrs['units'] = 'mm/h'
        if 'pet_era5land' in ds:
            ds['pet_era5land'].attrs['units'] = 'mm/h'
        if 'et_era5land' in ds:
            ds['et_era5land'].attrs['units'] = 'mm/h'
        if 't_era5land' in ds:
            ds['t_era5land'].attrs['units'] = '°C'
        ds.attrs['title'] = 'Anhui Basin Flood Event 1H Timeseries Dataset (Merged)'
        ds.attrs['description'] = 'Merged hourly timeseries data for flood events in Anhui basins, including streamflow, precipitation, evaporation, and temperature. Data processed and standardized for hydrological analysis.'
        ds.attrs['created_by'] = 'Yikai CHAI'
        output_file_nc = os.path.join(output_folder, output_filename + ".nc")
        ds.to_netcdf(output_file_nc)
        logging.info(f"已保存流域 {basin_id} 的 nc 文件: {output_file_nc}")
        success_basins.append(basin_id)
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
    success_basins, failed_basins = merge_csv_files_by_basin(input_folder, output_folder)
    logging.info("处理完成！")