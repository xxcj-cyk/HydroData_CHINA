"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-11 11:26:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-30 16:48:26
"""

import os
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
import re

def read_netcdf_files(obs_file_path, pred_file_path):
    """
    Read NetCDF files for observed and predicted values
    
    Parameters:
    obs_file_path: Path to observed NetCDF file
    pred_file_path: Path to predicted NetCDF file
    
    Returns:
    obs_ds: Observed dataset
    pred_ds: Predicted dataset
    """
    # Read NetCDF files
    obs_ds = xr.open_dataset(obs_file_path)
    pred_ds = xr.open_dataset(pred_file_path)
    
    return obs_ds, pred_ds

def get_basin_list(mode):
    """
    Read different basin lists based on mode
    mode: 'T' reads train_sets.csv, 'V' reads validation_sets.csv
    Returns: basin_list
    """
    if mode == 'T':
        basin_csv = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood\train_sets.csv"
    elif mode == 'V':
        basin_csv = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood\validation_sets.csv"
    else:
        raise ValueError("mode parameter can only be 'T' or 'V'")
    basin_df = pd.read_csv(basin_csv)
    basin_list = basin_df['basin'].astype(str).tolist()
    return basin_list

def export_to_csv_by_basin(obs_ds, pred_ds, output_dir, mode='V'):
    """
    Split data into multiple CSV files by basin, only export specified basins
    mode: 'T' only exports train_sets, 'V' only exports validation_sets
    """
    basin_list = get_basin_list(mode)
    os.makedirs(output_dir, exist_ok=True)
    basin_ids = obs_ds.basin.values
    basin_ids = [bid for bid in basin_ids if bid in basin_list]
    count = 0
    for basin_id in basin_ids:
        obs_basin = obs_ds.sel(basin=basin_id)
        pred_basin = pred_ds.sel(basin=basin_id)
        df = pd.DataFrame({
            'time': obs_basin.time.values,
            'streamflow_obs': obs_basin.streamflow.values,
            'streamflow_pred': pred_basin.streamflow.values
        })
        df.set_index('time', inplace=True)
        output_file = os.path.join(output_dir, f'{basin_id}_month.csv')
        df.to_csv(output_file)
        count += 1
        if count % 50 == 0:
            print(f'Processed {count}/{len(basin_ids)} basins')
    print(f'Total processed {count} basins')

if __name__ == '__main__':
    # Set file paths
    obs_file_path = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\epoch_best_flow_obs.nc"
    pred_file_path = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\epoch_best_flow_pred.nc"
    obs_file_path_T = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui_train\epoch_best_model.pth_flow_obs.nc"
    pred_file_path_T = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui_train\epoch_best_model.pth_flow_pred.nc"

    # Set output directory
    output_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\nc2csv_month"
    output_dir_T = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\nc2csv_month"

    # Read NetCDF files and export V set
    print('Reading NetCDF files (V set)...')
    obs_ds, pred_ds = read_netcdf_files(obs_file_path, pred_file_path)
    mode = 'V'
    print(f'Exporting {mode} set basins as CSV files...')
    export_to_csv_by_basin(obs_ds, pred_ds, output_dir, mode)
    
    # Read NetCDF files and export T set
    print('Reading NetCDF files (T set)...')
    obs_ds_T, pred_ds_T = read_netcdf_files(obs_file_path_T, pred_file_path_T)
    mode_T = 'T'
    print(f'Exporting {mode_T} set basins as CSV files...')
    export_to_csv_by_basin(obs_ds_T, pred_ds_T, output_dir_T, mode_T)
    
    print('Processing completed!')