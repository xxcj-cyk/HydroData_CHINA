"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2023-07-13 10:00:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-11 11:20:21
"""

import os
import glob
import logging
import re
import xarray as xr


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def merge_et_pet_data(flood_data_dir, et_data_dir, pet_data_dir, output_dir):
    """
    将ET和PET数据与洪水数据关联并合并
    
    参数:
        flood_data_dir: 洪水数据目录路径
        et_data_dir: ET数据目录路径
        pet_data_dir: PET数据目录路径
        output_dir: 输出目录路径
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    # 获取所有洪水数据文件
    flood_files = glob.glob(os.path.join(flood_data_dir, "*.nc"))
    logging.info(f"找到 {len(flood_files)} 个洪水数据文件")
    # 获取所有ET和PET数据文件，使用正确的命名格式
    et_files = {}
    for f in glob.glob(os.path.join(et_data_dir, "*.nc")):
        match = re.search(r'Anhui_([0-9]+)_ET\.nc$', os.path.basename(f))
        if match:
            station_code = match.group(1)
            et_files[station_code] = f
    pet_files = {}
    for f in glob.glob(os.path.join(pet_data_dir, "*.nc")):
        match = re.search(r'Anhui_([0-9]+)_PET\.nc$', os.path.basename(f))
        if match:
            station_code = match.group(1)
            pet_files[station_code] = f
    logging.info(f"找到 {len(et_files)} 个ET数据文件")
    logging.info(f"找到 {len(pet_files)} 个PET数据文件")
    # 处理每个洪水数据文件
    processed_count = 0
    for flood_file in flood_files:
        # 提取站点编码
        match = re.search(r'Anhui_([0-9]+)_', os.path.basename(flood_file))
        if not match:
            logging.warning(f"无法从文件名 {flood_file} 提取站点编码，跳过该文件")
            continue
        station_code = match.group(1)
        # 检查是否有对应的ET和PET数据
        if station_code not in et_files:
            logging.warning(f"站点 {station_code} 没有对应的ET数据，跳过该文件")
            continue
        if station_code not in pet_files:
            logging.warning(f"站点 {station_code} 没有对应的PET数据，跳过该文件")
            continue
        # 读取洪水数据
        flood_ds = xr.open_dataset(flood_file)
        et_ds = xr.open_dataset(et_files[station_code])
        pet_ds = xr.open_dataset(pet_files[station_code])
        # 将ET数据按时间关联到洪水数据
        if 'evaporation' in et_ds:
            # 提取洪水数据的时间范围
            flood_times = flood_ds.time.values
            # 在ET数据中选择对应时间范围的数据
            et_subset = et_ds.sel(time=flood_times, method='nearest')
            # 将ET数据添加到洪水数据中
            flood_ds['evaporation'] = et_subset.evaporation
        # 将ERA5-Land的PET集数据按时间关联到洪水数据
        # 提取洪水数据的时间范围
        flood_times = flood_ds.time.values
        # 在PET数据中选择对应时间范围的数据
        pet_subset = pet_ds.sel(time=flood_times, method='nearest')
        if 'temperature_2m' in pet_ds:
            flood_ds['temperature_2m'] = pet_subset.temperature_2m
        if 'potential_evaporation_hourly' in pet_ds:
            flood_ds['potential_evaporation_hourly'] = pet_subset.potential_evaporation_hourly
        if 'total_evaporation_hourly' in pet_ds:
            flood_ds['total_evaporation_hourly'] = pet_subset.total_evaporation_hourly
        if 'total_precipitation_hourly' in pet_ds:
            flood_ds['total_precipitation_hourly'] = pet_subset.total_precipitation_hourly
        # 保存合并后的数据
        output_file = os.path.join(output_dir, os.path.basename(flood_file))
        flood_ds.to_netcdf(output_file)
        processed_count += 1
        logging.info(f"已处理文件 {os.path.basename(flood_file)}")
        # 关闭数据集
        flood_ds.close()
        et_ds.close()
        pet_ds.close()
    logging.info(f"处理完成，共处理了 {processed_count} 个文件")


if __name__ == "__main__":
    # 设置数据目录
    flood_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood"
    et_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_ET"
    pet_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_PET"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    # 合并数据
    merge_et_pet_data(flood_data_dir, et_data_dir, pet_data_dir, output_dir)