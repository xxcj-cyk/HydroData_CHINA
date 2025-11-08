"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-09 11:06:49
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-07-19 10:24:00
"""

import os
import pandas as pd
import numpy as np
import datetime
import calendar
import logging
import xarray as xr  # Add xarray library import


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def read_basin_evaporation_mapping(mapping_file_path):
    """
    Read mapping relationship between basin IDs and corresponding evaporation stations
    Args:
        mapping_file_path: Path to xlsx file containing mapping relationship  
    Returns:
        Dictionary with basin IDs as keys and corresponding evaporation station names as values
    """
    # Read sheet2
    mapping_df = pd.read_excel(mapping_file_path, sheet_name=1)
    # Create mapping dictionary
    basin_to_station = dict(zip(mapping_df["流域ID"], mapping_df["对应蒸发站"]))
    logging.info(f"Successfully read mapping relationship, total {len(basin_to_station)} basins")
    return basin_to_station


def read_evaporation_data(evaporation_file_path, station_names):
    """
    Read evaporation station data from evaporation data file
    Args:
        evaporation_file_path: Path to xlsx file containing evaporation data
        station_names: List of evaporation station names to read  
    Returns:
        Dictionary with station names as keys, values are DataFrames containing monthly average evaporation data and multi-year averages
    """
    station_data = {}
    # Get all sheet names in the file
    xl = pd.ExcelFile(evaporation_file_path)
    available_sheets = xl.sheet_names
    logging.info(f"Starting to read evaporation data, need to process {len(station_names)} evaporation stations")
    # Iterate through required stations
    for station in station_names:
        if station in available_sheets:
            # Read data from corresponding sheet
            df = pd.read_excel(evaporation_file_path, sheet_name=station)
            # Process table format: set column names
            if len(df.columns) >= 13:  # Ensure enough columns (year + 12 months)
                # Rename columns
                new_columns = ['年'] + [f'{i}月' for i in range(1, 13)]
                df.columns = new_columns[:len(df.columns)]
                
                # Save multi-year average (last row)
                multi_year_avg = None
                if len(df) > 0:
                    multi_year_avg = df.iloc[-1].copy()
                    # Remove last row (multi-year average) for regular processing
                    df = df.iloc[:-1].copy()
                # Ensure year column is numeric type
                df['年'] = pd.to_numeric(df['年'], errors='coerce')
                # Filter out rows with invalid years
                df = df.dropna(subset=['年'])
                station_data[station] = {'data': df, 'multi_year_avg': multi_year_avg}
                logging.info(f"Successfully read evaporation station '{station}' data, total {len(df)} years")
            else:
                logging.warning(f"Evaporation station '{station}' data format is incorrect, insufficient columns")
        else:
            logging.warning(f"Evaporation station '{station}' does not exist in file")
    return station_data


def convert_monthly_to_hourly(monthly_data, start_year, end_year):
    """
    Convert monthly average evaporation data to hourly scale data
    Args:
        monthly_data: Dictionary containing monthly average evaporation data, with 'data' and 'multi_year_avg' keys
        start_year: Start year
        end_year: End year
    Returns:
        DataFrame containing hourly scale evaporation data
    """
    # Create empty DataFrame to store hourly data
    hourly_data = []
    monthly_df = monthly_data['data']
    multi_year_avg = monthly_data['multi_year_avg']
    logging.info(f"Starting to convert monthly average data to hourly scale data, processing {start_year} to {end_year}")
    # Get year column and month columns
    year_col = '年'
    month_cols = [f'{i}月' for i in range(1, 13)]  # Column names for months 1-12
    # Iterate through year range
    for year in range(start_year, end_year + 1):
        # Find data for this year
        year_data = monthly_df[monthly_df[year_col] == year]
        if len(year_data) == 0:
            # If no data for this year, use multi-year average
            if multi_year_avg is not None:
                start_date = datetime.datetime(year, 1, 1, 0, 0, 0)
                end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='h')
                # Use multi-year average to calculate hourly data for each month
                month_hourly_data = []
                for month_idx, month_col in enumerate(month_cols, 1):
                    # Get number of days in this month
                    days_in_month = calendar.monthrange(year, month_idx)[1]
                    # Get multi-year average evaporation for this month
                    monthly_evap = multi_year_avg[month_col] if month_col in multi_year_avg.index else np.nan
                    # If monthly average is NaN, set all hours in this month to NaN
                    if pd.isna(monthly_evap):
                        daily_evap = np.nan
                        hourly_evap = np.nan
                    else:
                        # Convert monthly average to daily average (assuming daily averages are the same for each day in the month)
                        daily_evap = monthly_evap / days_in_month
                        # Convert daily average to hourly average (assuming hourly averages are the same for each hour in the day)
                        hourly_evap = daily_evap / 24
                    # Create hourly time series for this month
                    start_date = datetime.datetime(year, month_idx, 1, 0, 0, 0)
                    if month_idx < 12:
                        end_date = datetime.datetime(year, month_idx + 1, 1, 0, 0, 0)
                    else:
                        end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                    dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='h')
                    month_hourly = pd.DataFrame({'time': dates, 'evaporation': hourly_evap})
                    month_hourly_data.append(month_hourly)
                # Merge hourly data for all months of this year
                year_hourly = pd.concat(month_hourly_data, ignore_index=True)
                hourly_data.append(year_hourly)
                logging.warning(f"Data for year {year} does not exist, used multi-year average for calculation")
            else:
                # If no multi-year average, set to NaN
                start_date = datetime.datetime(year, 1, 1, 0, 0, 0)
                end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='h')
                year_hourly = pd.DataFrame({'time': dates, 'evaporation': np.nan})
                hourly_data.append(year_hourly)
                logging.warning(f"Data for year {year} does not exist and no multi-year average available, set to NaN")
        else:
            # Get monthly average evaporation data for this year
            row = year_data.iloc[0]
            # Iterate through each month
            for month_idx, month_col in enumerate(month_cols, 1):
                # Get number of days in this month
                days_in_month = calendar.monthrange(year, month_idx)[1]
                # Get monthly average evaporation for this month
                monthly_evap = row[month_col] if month_col in row.index else np.nan
                # If monthly average is NaN, set all hours in this month to NaN
                if pd.isna(monthly_evap):
                    daily_evap = np.nan
                    hourly_evap = np.nan
                else:
                    # Convert monthly average to daily average (assuming daily averages are the same for each day in the month)
                    daily_evap = monthly_evap / days_in_month
                    # Convert daily average to hourly average (assuming hourly averages are the same for each hour in the day)
                    hourly_evap = daily_evap / 24
                # Create hourly time series for this month
                start_date = datetime.datetime(year, month_idx, 1, 0, 0, 0)
                if month_idx < 12:
                    end_date = datetime.datetime(year, month_idx + 1, 1, 0, 0, 0)
                else:
                    end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='h')
                month_hourly = pd.DataFrame({'time': dates, 'evaporation': hourly_evap})
                hourly_data.append(month_hourly)
    # Merge all hourly data
    hourly_df = pd.concat(hourly_data, ignore_index=True)
    logging.info(f"Hourly scale data conversion completed, generated {len(hourly_df)} records")
    return hourly_df


def process_evaporation_data(basin_station_mapping, station_data, output_dir, start_year, end_year):
    """
    Process evaporation data and generate NC files by basin
    Args:
        basin_station_mapping: Dictionary mapping basin IDs to evaporation stations
        station_data: Dictionary of evaporation station data
        output_dir: Output directory
        start_year: Start year
        end_year: End year
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Starting to process evaporation data, total {len(basin_station_mapping)} basins")
    # Iterate through each basin
    processed_count = 0
    for basin_id, station_name in basin_station_mapping.items():
        if station_name not in station_data:
            logging.warning(f"Evaporation station '{station_name}' corresponding to basin {basin_id} is not available, skipping")
            continue
        # Get monthly average data for this evaporation station
        monthly_data = station_data[station_name]  # Now a dictionary containing 'data' and 'multi_year_avg'
        # Convert to hourly scale
        hourly_df = convert_monthly_to_hourly(monthly_data, start_year, end_year)
        # Convert DataFrame to xarray Dataset
        ds = xr.Dataset(
            {
                "evaporation": ("time", hourly_df["evaporation"].values),
            },
            coords={
                "time": hourly_df["time"].values,
            },
            attrs={
                "description": f"Hourly evaporation data for basin {basin_id}",
                "station_name": station_name,
                "basin_id": basin_id,
            }
        )
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step3_anhui_1h_interim_et.py
        # Save as NC file
        output_file = os.path.join(output_dir, f"{basin_id}_ET.nc")
=======
        # 保存为NC文件
        output_file = os.path.join(output_dir, f"{basin_id}_PET.nc")
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step2_anhui_1h_interim_anhuipet.py
        ds.to_netcdf(output_file)
        processed_count += 1
        logging.info(f"Saved evaporation data for basin {basin_id} to {output_file}, total {len(hourly_df)} records")
    logging.info(f"Processing completed, processed data for {processed_count} basins")


if __name__ == "__main__":
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step3_anhui_1h_interim_et.py
    # Set file paths
    mapping_file_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\ET_Station_21\流域蒸发站对应表.xlsx"
    evaporation_file_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\ET_Station_21\多年平均月蒸散发.xlsx"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_ET"
    # Set year range
=======
    # 设置文件路径
    mapping_file_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_Station_21\流域蒸发站对应表.xlsx"
    evaporation_file_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_Station_21\多年平均月蒸散发.xlsx"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_anhui-PET"
    # 设置年份范围
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step2_anhui_1h_interim_anhuipet.py
    start_year = 1960
    end_year = 2022
    logging.info("Starting to process Anhui basin evaporation data")
    # Read mapping relationship between basins and evaporation stations
    basin_station_mapping = read_basin_evaporation_mapping(mapping_file_path)
    # Get all required evaporation station names
    station_names = set(basin_station_mapping.values())
    # Read evaporation station data
    station_data = read_evaporation_data(evaporation_file_path, station_names)
    # Process data and generate NC files
    process_evaporation_data(basin_station_mapping, station_data, output_dir, start_year, end_year)
    logging.info("Anhui basin evaporation data processing completed")