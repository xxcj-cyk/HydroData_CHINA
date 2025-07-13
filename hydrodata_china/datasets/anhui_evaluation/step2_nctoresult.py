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
    读取观测值和预测值的NetCDF文件
    
    参数:
    obs_file_path: 观测值NetCDF文件路径
    pred_file_path: 预测值NetCDF文件路径
    
    返回:
    obs_ds: 观测值数据集
    pred_ds: 预测值数据集
    """
    # 读取NetCDF文件
    obs_ds = xr.open_dataset(obs_file_path)
    pred_ds = xr.open_dataset(pred_file_path)
    
    return obs_ds, pred_ds

def get_basin_list(mode):
    """
    根据mode读取不同的basin列表
    mode: 'T' 读取train_sets.csv, 'V' 读取validation_sets.csv
    返回: basin_list
    """
    if mode == 'T':
        basin_csv = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood\train_sets.csv"
    elif mode == 'V':
        basin_csv = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood\validation_sets.csv"
    else:
        raise ValueError("mode参数只能为 'T' 或 'V'")
    basin_df = pd.read_csv(basin_csv)
    basin_list = basin_df['basin'].astype(str).tolist()
    return basin_list

def export_to_csv_by_basin(obs_ds, pred_ds, output_dir, mode='V'):
    """
    按basin将数据分成多个CSV文件，只导出指定的basin
    mode: 'T' 只导出train_sets, 'V' 只导出validation_sets
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
            print(f'已处理 {count}/{len(basin_ids)} 个basin')
    print(f'总共处理了 {count} 个basin')

if __name__ == '__main__':
    # 设置文件路径
    obs_file_path = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\epoch_best_flow_obs.nc"
    pred_file_path = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\epoch_best_flow_pred.nc"
    obs_file_path_T = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui_train\epoch_best_model.pth_flow_obs.nc"
    pred_file_path_T = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui_train\epoch_best_model.pth_flow_pred.nc"

    # 设置输出目录
    output_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\nc2csv_month"
    output_dir_T = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\nc2csv_month"

    # 读取NetCDF文件并导出V集
    print('正在读取NetCDF文件（V集）...')
    obs_ds, pred_ds = read_netcdf_files(obs_file_path, pred_file_path)
    mode = 'V'
    print(f'正在按{mode}集basin导出为CSV文件...')
    export_to_csv_by_basin(obs_ds, pred_ds, output_dir, mode)
    
    # 读取NetCDF文件并导出T集
    print('正在读取NetCDF文件（T集）...')
    obs_ds_T, pred_ds_T = read_netcdf_files(obs_file_path_T, pred_file_path_T)
    mode_T = 'T'
    print(f'正在按{mode_T}集basin导出为CSV文件...')
    export_to_csv_by_basin(obs_ds_T, pred_ds_T, output_dir_T, mode_T)
    
    print('处理完成！')