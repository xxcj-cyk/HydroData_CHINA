"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-19 18:32:14
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-30 16:41:52
"""

import os
import pandas as pd

# Folder paths
filtered_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_FloodEvent_Period"
flow_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_LSTM\anhui21_797_PET_Anhui\nc2csv_month"
save_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Period\Anhui_LSTM\anhui21_797_PET_Anhui\nc2csv_period"

# Get all CSV filenames
filtered_files = [f for f in os.listdir(filtered_dir) if f.endswith('_period.csv')]
flow_files = [f for f in os.listdir(flow_dir) if f.endswith('_month.csv')]

# Build mapping from filename prefix to filename
filtered_map = {f.replace('_period.csv', ''): f for f in filtered_files}
flow_map = {f.replace('_month.csv', ''): f for f in flow_files}

# Find common prefixes
common_keys = set(filtered_map.keys()) & set(flow_map.keys())

# Associate and process
for key in common_keys:
    filtered_path = os.path.join(filtered_dir, filtered_map[key])
    flow_path = os.path.join(flow_dir, flow_map[key])
    
    df_filtered = pd.read_csv(filtered_path)
    df_flow = pd.read_csv(flow_path)
    
    # Keep only 'time' column from _filtered.csv
    df_filtered = df_filtered[['time']]
    
    n = len(df_filtered)
    df_flow_tail = df_flow.tail(n).reset_index(drop=True)
    df_filtered_reset = df_filtered.reset_index(drop=True)
    
    # Delete 'time' column in _flow.csv (if exists)
    if 'time' in df_flow_tail.columns:
        df_flow_tail = df_flow_tail.drop(columns=['time'])
    
    # Merge (concatenate by column)
    merged = pd.concat([df_filtered_reset, df_flow_tail], axis=1)
    
    # Save to specified path
    os.makedirs(save_dir, exist_ok=True)
    merged.to_csv(os.path.join(save_dir, f"{key}.csv"), index=False)
    print(f"{key} merged and saved")