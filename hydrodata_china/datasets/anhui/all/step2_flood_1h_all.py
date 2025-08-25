"""
@Author: Yikai CHAI
@Email: chaiyikai@mail.dlut.edu.cn
@Company: Dalian University of Technology
@Date: 2025-08-22
@Description: 按场次拆分1H数据，生成每场降雨的csv文件
"""

import os
import pandas as pd
import numpy as np

# 场次信息文件
event_excel = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21\FloodEvent20_705.xlsx"
# 1H数据文件夹
input_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H'
# 输出文件夹
output_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood_Selected'
os.makedirs(output_dir, exist_ok=True)

# 读取场次信息
df_event = pd.read_excel(event_excel)

for idx, row in df_event.iterrows():
    event_id = row['FloodEvent_797']
    warmup_start = pd.to_datetime(row['Warmup_Start'])
    flood_start = pd.to_datetime(row['FloodEvent_Start'])
    flood_end = pd.to_datetime(row['FloodEvent_End'])
    basin_code, event_code = event_id.split('_')
    input_file = os.path.join(input_dir, f'Anhui_{basin_code}_1H.csv')
    if not os.path.exists(input_file):
        print(f'缺少流域数据文件: {input_file}')
        continue
    df = pd.read_csv(input_file)
    df['time'] = pd.to_datetime(df['time'])
    # 只保留 warmup_start 到 flood_end 之间的数据
    df_event_split = df[(df['time'] >= warmup_start) & (df['time'] <= flood_end)].copy()
    # 标记洪水事件区间
    df_event_split['flood_event'] = np.nan
    mask = (df_event_split['time'] >= flood_start) & (df_event_split['time'] <= flood_end)
    df_event_split.loc[mask, 'flood_event'] = 1
    df_event_split.insert(0, 'basin', f'Anhui_{basin_code}_{event_code}')
    out_file = os.path.join(output_dir, f'Anhui_{basin_code}_{event_code}.csv')
    df_event_split.to_csv(out_file, index=False, encoding='utf-8')
    print(f'已保存: {out_file}')

print('全部场次拆分完成！')
