"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-09 10:15:56
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-11 11:21:36
"""

import os
import pandas as pd
import glob
import logging
import xarray as xr  # Add xarray library import


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_csv_files(input_dir, output_dir):
    """
    Read CSV files organized by year and reorganize them as NC files organized by basin ID
    
    Parameters:
    input_dir (str): Directory path containing CSV files organized by year
    output_dir (str): Directory path for saving output files organized by basin ID
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    # Get all CSV files
    csv_files = glob.glob(os.path.join(input_dir, '*.csv'))
    logging.info(f'Found {len(csv_files)} CSV files')
    if not csv_files:
        logging.error(f'No CSV files found in {input_dir}')
        return
    # Create a dictionary to store data for each basin
    basin_data = {}
    # Process each CSV file
    for csv_file in csv_files:
        # Read CSV file
        df = pd.read_csv(csv_file)
        # Convert time column to datetime format
        df['time_start'] = pd.to_datetime(df['time_start'])
        # Convert temperature from Kelvin to Celsius
        df['temperature_2m'] = df['temperature_2m'] - 273.15
        # Multiply evaporation and precipitation variables by 1000
        df['potential_evaporation_hourly'] = df['potential_evaporation_hourly'] * 1000
        df['total_evaporation_hourly'] = df['total_evaporation_hourly'] * 1000
        df['total_precipitation_hourly'] = df['total_precipitation_hourly'] * 1000
        # Group by basin ID
        for basin_id, group in df.groupby('basin_id'):
            if basin_id not in basin_data:
                basin_data[basin_id] = []
            basin_data[basin_id].append(group)
        logging.info(f'Processed file {csv_file}')
    # Merge and save data for each basin
    for basin_id, data_frames in basin_data.items():
        # Merge all data for this basin
        basin_df = pd.concat(data_frames)
        # Sort by time
        basin_df = basin_df.sort_values('time_start')
        # Remove possible duplicate data
        original_len = len(basin_df)
        basin_df = basin_df.drop_duplicates()
        if len(basin_df) < original_len:
            logging.info(f'Removed duplicate data for basin {basin_id}, total {original_len - len(basin_df)} records')
        # Remove basin_id column
        basin_df = basin_df.drop(columns=['basin_id'])
        # Convert DataFrame to xarray Dataset
        ds = xr.Dataset(
            {
                column: ("time", basin_df[column].values) for column in basin_df.columns if column != 'time_start'
            },
            coords={
                "time": basin_df['time_start'].values,
            },
            attrs={
                "description": f"Hourly potential evapotranspiration data for basin {basin_id}",
                "basin_id": basin_id,
            }
        )
        # Convert UTC time to China time (UTC+8)
        time_china = pd.to_datetime(ds.time.values) + pd.Timedelta(hours=8)
        ds = ds.assign_coords(time=time_china)
        logging.info(f"Converted time from UTC to China time (UTC+8) for basin {basin_id}")
        # Filter data for 1960~2022
        ds = ds.sel(time=slice('1960-01-01', '2022-12-31 23:59:59'))
        logging.info(f'After filtering, data range for basin {basin_id} is from {ds.time.values.min()} to {ds.time.values.max()}, total {len(ds.time)} records')
        # Save as NC file
        output_file = os.path.join(output_dir, f'{basin_id}_PET.nc')
        ds.to_netcdf(output_file)
        logging.info(f'Saved data for basin {basin_id} to {output_file}, total {len(basin_df)} records')
    logging.info(f'Processing completed, processed data for {len(basin_data)} basins')


if __name__ == '__main__':
    # Set input and output directories
    input_directory = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_ERA5-Land_21"
    output_directory = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_PET"
    # Process CSV files and output as NC files
    process_csv_files(input_directory, output_directory)