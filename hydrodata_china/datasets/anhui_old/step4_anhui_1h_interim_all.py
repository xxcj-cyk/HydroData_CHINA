"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2023-07-13 10:00:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-07-19 10:26:32
"""

import os
import glob
import logging
import re
import xarray as xr


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def merge_et_pet_data(flood_data_dir, et_data_dir, pet_data_dir, output_dir):
    """
    Merge ET and PET data with flood data
    
    Parameters:
        flood_data_dir: Path to flood data directory
        et_data_dir: Path to ET data directory
        pet_data_dir: Path to PET data directory
        output_dir: Path to output directory
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Get all flood data files
    flood_files = glob.glob(os.path.join(flood_data_dir, "*.nc"))
    logging.info(f"Found {len(flood_files)} flood data files")
    # Get all ET and PET data files, using correct naming format
    et_files = {}
    for f in glob.glob(os.path.join(et_data_dir, "*.nc")):
        match = re.search(r'Anhui_([0-9]+)_PET\.nc$', os.path.basename(f))
        if match:
            station_code = match.group(1)
            et_files[station_code] = f
    pet_files = {}
    for f in glob.glob(os.path.join(pet_data_dir, "*.nc")):
        match = re.search(r'Anhui_([0-9]+)_PET\.nc$', os.path.basename(f))
        if match:
            station_code = match.group(1)
            pet_files[station_code] = f
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step5_anhui_1h_interim_all.py
    logging.info(f"Found {len(et_files)} ET data files")
    logging.info(f"Found {len(pet_files)} PET data files")
    # Process each flood data file
=======
    logging.info(f"找到 {len(et_files)} 个anhui-PET数据文件")
    logging.info(f"找到 {len(pet_files)} 个era5land-PET数据文件")
    # 处理每个洪水数据文件
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step4_anhui_1h_interim_all.py
    processed_count = 0
    for flood_file in flood_files:
        # Extract station code
        match = re.search(r'Anhui_([0-9]+)_', os.path.basename(flood_file))
        if not match:
            logging.warning(f"Unable to extract station code from filename {flood_file}, skipping this file")
            continue
        station_code = match.group(1)
        # Check if corresponding ET and PET data exist
        if station_code not in et_files:
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step5_anhui_1h_interim_all.py
            logging.warning(f"Station {station_code} has no corresponding ET data, skipping this file")
            continue
        if station_code not in pet_files:
            logging.warning(f"Station {station_code} has no corresponding PET data, skipping this file")
=======
            logging.warning(f"站点 {station_code} 没有对应的anhui-PET数据，跳过该文件")
            continue
        if station_code not in pet_files:
            logging.warning(f"站点 {station_code} 没有对应的era5land-PET数据，跳过该文件")

>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step4_anhui_1h_interim_all.py
            continue
        # Read flood data
        flood_ds = xr.open_dataset(flood_file)
        et_ds = xr.open_dataset(et_files[station_code])
        pet_ds = xr.open_dataset(pet_files[station_code])
        # Merge ET data with flood data by time
        if 'evaporation' in et_ds:
            # Extract time range from flood data
            flood_times = flood_ds.time.values
            # Select corresponding time range data from ET data
            et_subset = et_ds.sel(time=flood_times, method='nearest')
            # Add ET data to flood data
            flood_ds['evaporation'] = et_subset.evaporation
        # Merge ERA5-Land PET data with flood data by time
        # Extract time range from flood data
        flood_times = flood_ds.time.values
        # Select corresponding time range data from PET data
        pet_subset = pet_ds.sel(time=flood_times, method='nearest')
        if 'temperature_2m' in pet_ds:
            flood_ds['temperature_2m'] = pet_subset.temperature_2m
        if 'potential_evaporation_hourly' in pet_ds:
            flood_ds['potential_evaporation_hourly'] = pet_subset.potential_evaporation_hourly
        if 'total_evaporation_hourly' in pet_ds:
            flood_ds['total_evaporation_hourly'] = pet_subset.total_evaporation_hourly
        if 'total_precipitation_hourly' in pet_ds:
            flood_ds['total_precipitation_hourly'] = pet_subset.total_precipitation_hourly
        # Save merged data
        output_file = os.path.join(output_dir, os.path.basename(flood_file))
        flood_ds.to_netcdf(output_file)
        processed_count += 1
        logging.info(f"Processed file {os.path.basename(flood_file)}")
        # Close datasets
        flood_ds.close()
        et_ds.close()
        pet_ds.close()
    logging.info(f"Processing completed, processed {processed_count} files")


if __name__ == "__main__":
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step5_anhui_1h_interim_all.py
    # Set data directories
    flood_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood_new"
    et_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_ET"
    pet_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_PET"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_new"
    # Merge data
=======
    # 设置数据目录
    flood_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood"
    et_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_anhui-PET"
    pet_data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_era5land-PET"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    # 合并数据
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step4_anhui_1h_interim_all.py
    merge_et_pet_data(flood_data_dir, et_data_dir, pet_data_dir, output_dir)