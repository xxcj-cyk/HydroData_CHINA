"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-27 17:33:16
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-07-05 11:36:48
"""

import os
import pandas as pd
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
    for flood_folder, files_data in data_dict.items():
        for file_name, df in files_data.items():
            # Extract station code
            station_match = re.search(r'_([0-9]+)_', file_name)
            if not station_match:
                logging.warning(f"Unable to extract station code from filename {file_name}, skipping this file")
                continue
            station_code = station_match.group(1)
            # Extract timestamp
            timestamp_match = re.search(r'_([0-9]+)\.xls', file_name)
            if not timestamp_match:
                logging.warning(f"Unable to extract timestamp from filename {file_name}, skipping this file")
                continue
            timestamp = timestamp_match.group(1)
            # Create new filename
            new_filename = f"Anhui_{station_code}_{timestamp}.nc"
            output_path = os.path.join(output_dir, new_filename)
            # Convert to xarray dataset
            ds = df.set_index('time').to_xarray()
            # Save as netCDF file
            ds.to_netcdf(output_path)
            saved_files += 1
            logging.info(f"Saved: {output_path}")
    return saved_files


if __name__ == "__main__":
    # Set root folder path
    root_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21_new"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood_new"
    # Read all data
    logging.info("Starting to read flood data...")
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