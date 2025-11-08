"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-27 17:33:16
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-07-19 10:13:50
"""

import os
import pandas as pd
import numpy as np
import glob
import logging
import re
import xarray as xr
from pathlib import Path


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


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
    """Read all Excel files in flood data folder"""
    all_data = {}
    # Get all subfolders (flood event folders)
    flood_folders = [f for f in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, f))]
    for flood_folder in flood_folders:
        folder_path = os.path.join(root_folder, flood_folder)
        all_data[flood_folder] = {}
        # Get all Excel files in the folder
        excel_files = glob.glob(os.path.join(folder_path, "*.xls")) + glob.glob(os.path.join(folder_path, "*.xlsx"))
        for excel_file in excel_files:
            file_name = os.path.basename(excel_file)
            # Select appropriate engine based on file extension
            engine = 'xlrd' if file_name.endswith('.xls') else 'openpyxl'
            df = pd.read_excel(excel_file, engine=engine)
            # Rename columns
            column_mapping = {
                '时间': 'time',
                '实测流量': 'streamflow_obs',
                '预报合计流量': 'streamflow_pred_xaj',
                '面雨量': 'P_Anhui'
            }
            df = df.rename(columns=column_mapping)
            # Convert time column to datetime format
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
            # Process rainfall station column names
            for col in df.columns:
                if col not in column_mapping.values():
                    match = re.search(r'.*?([0-9]+)$', col)
                    if match:
                        station_code = match.group(1)
                        df = df.rename(columns={col: f'P_{station_code}'})
            # Convert all columns starting with P_ to float64
            for col in df.columns:
                if col.startswith('P_'):
                    df[col] = df[col].astype('float64')
            # Extract station code to get basin area
            station_code_match = re.search(r'_([0-9]+)_', file_name)
            if station_code_match:
                station_code = station_code_match.group(1)
                if station_code in BASIN_AREAS:
                    basin_area = BASIN_AREAS[station_code]
                    # For hourly data, need to divide by 24 (convert to hourly units)
                    df['streamflow'] = df['streamflow_obs'] * 86.4 / basin_area / 24
            all_data[flood_folder][file_name] = df
    return all_data


def save_as_netcdf(data_dict, output_dir):
    """Save data as netCDF format"""
    os.makedirs(output_dir, exist_ok=True)
    saved_files = 0
    skipped_files = 0
    
    for flood_folder, files_data in data_dict.items():
        for file_name, df in files_data.items():
            # Extract station code
            station_match = re.search(r'_([0-9]+)_', file_name)
            if not station_match:
                logging.warning(f"Unable to extract station code from filename {file_name}, skipping this file")
                continue
            station_code = station_match.group(1)
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step2_anhui_1h_interim_flood.py
            # Extract timestamp
=======
            
            # 提取时间戳
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step1_anhui_1h_interim_flood.py
            timestamp_match = re.search(r'_([0-9]+)\.xls', file_name)
            if not timestamp_match:
                logging.warning(f"Unable to extract timestamp from filename {file_name}, skipping this file")
                continue
            timestamp = timestamp_match.group(1)
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step2_anhui_1h_interim_flood.py
            # Create new filename
            new_filename = f"Anhui_{station_code}_{timestamp}.nc"
            output_path = os.path.join(output_dir, new_filename)
            # Convert to xarray dataset
            ds = df.set_index('time').to_xarray()
            # Save as netCDF file
            ds.to_netcdf(output_path)
            saved_files += 1
            logging.info(f"Saved: {output_path}")
=======
            
            # 检查数据长度是否超过30天（30天 * 24小时 = 720小时）
            if len(df) <= 720:  # 720小时 = 30天
                logging.warning(f"文件 {file_name} 数据不足30天（{len(df)}小时），跳过该文件")
                skipped_files += 1
                continue
            
            # 创建数据副本
            df_processed = df.copy()
            
            # 添加flood_event列，初始化为NaN
            df_processed['flood_event'] = np.nan
            
            # 30天之后的数据标记为1
            df_processed.loc[df_processed.index[720:], 'flood_event'] = 1
            
            # 创建新的文件名
            new_filename = f"Anhui_{station_code}_{timestamp}.nc"
            output_path = os.path.join(output_dir, new_filename)
            
            # 转换为xarray数据集
            ds = df_processed.set_index('time').to_xarray()
            
            # 保存为netCDF文件
            ds.to_netcdf(output_path, engine='netcdf4')
            saved_files += 1
            
            # 统计flood_event标记情况
            flood_event_count = (df_processed['flood_event'] == 1).sum()
            logging.info(f"已保存: {output_path} (总数据{len(df_processed)}小时，flood_event=1的数据{flood_event_count}小时)")
    
    logging.info(f"跳过了 {skipped_files} 个数据不足30天的文件")
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step1_anhui_1h_interim_flood.py
    return saved_files


if __name__ == "__main__":
    # Set root folder path
    root_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21_new"
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step2_anhui_1h_interim_flood.py
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood_new"
    # Read all data
    logging.info("Starting to read flood data...")
=======
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood"
    # 读取所有数据
    logging.info("开始读取洪水数据...")
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step1_anhui_1h_interim_flood.py
    flood_data = read_flood_data(root_folder)
    # Output basic statistics
    flood_count = len(flood_data)
    total_files = sum(len(files) for files in flood_data.values())
    total_rows = sum(sum(df.shape[0] for df in files.values()) for files in flood_data.values())
    logging.info("\nData reading completed. Basic statistics:")
    logging.info(f"Read {flood_count} flood events")
    logging.info(f"Read {total_files} Excel files")
    logging.info(f"Read {total_rows} rows of data")
    # Output statistics for each event
    logging.info("\nStatistics for each event:")
    for flood_name, files_data in flood_data.items():
        files_count = len(files_data)
        rows_count = sum(df.shape[0] for df in files_data.values())
        logging.info(f"{flood_name}: Contains {files_count} files, total {rows_count} rows of data")
    # Save as netCDF format
    logging.info("\nStarting to save as netCDF format...")
    saved_files = save_as_netcdf(flood_data, output_dir)
    logging.info(f"\nData processing completed, saved {saved_files} netCDF files")