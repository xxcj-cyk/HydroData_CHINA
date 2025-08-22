"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-06-09 10:15:56
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-21 11:38:53
"""


"""
Process ERA5-Land meteorological data and output hourly CSVs by basin.
Main workflow:
    1. Read all original CSV files.
    2. Group and merge data by basin ID.
    3. Data preprocessing (unit conversion, time conversion, deduplication, year filtering).
    4. Output CSV for each basin.
"""

import os
import pandas as pd
import glob


# File paths
PET_ERA5LAND_FOLDER = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_ERA5-Land_21"
OUTPUT_FOLDER = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_era5land-PET"


def process_csv_files(input_dir, output_dir):
    """
    Process all original ERA5-Land meteorological CSV files and output hourly CSVs by basin.
    Steps:
        1. Read all CSV files.
        2. Group and merge data by basin ID.
        3. Data preprocessing (temperature unit conversion, evaporation/precipitation unit conversion, time conversion, deduplication, year filtering).
        4. Output CSV for each basin.
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    # Get all CSV files
    csv_files = glob.glob(os.path.join(input_dir, '*.csv'))
    print(f'Found {len(csv_files)} CSV files')
    if not csv_files:
        print(f'No CSV files found in {input_dir}')
        return
    basin_data = {}
    # Traverse all CSV files and group by basin
    for csv_file in csv_files:
        df = pd.read_csv(csv_file)
        # Time format conversion
        df['time_start'] = pd.to_datetime(df['time_start'])
        # Temperature unit conversion (K→℃)
        df['temperature_2m'] = df['temperature_2m'] - 273.15
        # Evaporation/precipitation unit conversion (mm/h)
        df['potential_evaporation_hourly'] = df['potential_evaporation_hourly'] * 1000
        df['total_evaporation_hourly'] = df['total_evaporation_hourly'] * 1000
        df['total_precipitation_hourly'] = df['total_precipitation_hourly'] * 1000
        # Group by basin ID
        for basin_id, group in df.groupby('basin_id'):
            if basin_id not in basin_data:
                basin_data[basin_id] = []
            basin_data[basin_id].append(group)
        print(f'Processed file {csv_file}')
    # Merge and output data for each basin
    for basin_id, data_frames in basin_data.items():
        basin_df = pd.concat(data_frames)
        # Sort by time
        basin_df = basin_df.sort_values('time_start')
        # Deduplication
        original_len = len(basin_df)
        basin_df = basin_df.drop_duplicates()
        if len(basin_df) < original_len:
            print(f'Removed {original_len - len(basin_df)} duplicate records for basin {basin_id}')
        # Remove basin ID column
        basin_df = basin_df.drop(columns=['basin_id'])
        # UTC→China time
        basin_df['time_start'] = pd.to_datetime(basin_df['time_start']) + pd.Timedelta(hours=8)
        print(f"Converted time for basin {basin_id} from UTC to China time (UTC+8)")
        # Filter data from 1960 to 2022
        basin_df = basin_df[(basin_df['time_start'] >= '1960-01-01') & (basin_df['time_start'] <= '2022-12-31 23:59:59')]
        print(f'After filtering, basin {basin_id} data range: {basin_df["time_start"].min()} to {basin_df["time_start"].max()}, total {len(basin_df)} records')
        # 列重命名
        basin_df = basin_df.rename(columns={
            'time_start': 'time',
            'temperature_2m': 'temperature_2m_era5land',
            'potential_evaporation_hourly': 'potential_evaporation_hourly_era5land',
            'total_evaporation_hourly': 'total_evaporation_hourly_era5land',
            'total_precipitation_hourly': 'total_precipitation_hourly_era5land'
        })
        # Output CSV
        output_file = os.path.join(output_dir, f'{basin_id}_PET_ERA5Land.csv')
        basin_df.to_csv(output_file, index=False)
        print(f'Saved data for basin {basin_id} to {output_file}, total {len(basin_df)} records')
    print(f'Processing complete, processed data for {len(basin_data)} basins')


if __name__ == '__main__':
    process_csv_files(PET_ERA5LAND_FOLDER, OUTPUT_FOLDER)