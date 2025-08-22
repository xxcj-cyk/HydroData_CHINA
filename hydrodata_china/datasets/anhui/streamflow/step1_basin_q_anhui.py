"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-08-21 23:00:36
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 00:14:05
"""

import os
import pandas as pd
import re


# 流量数据文件夹路径
BASIN_AREAS = {
    "50406910": 79.03,
    "50501200": 182.15,
    "50701100": 270.2,
    "50913900": 736.09,
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

# 流量数据文件夹路径
q_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Q_Station_21"
output_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\\Anhui_1H_Q"

# 洪水事件文件路径
flood_event_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21\FloodEvent_797.xlsx"
# 读取洪水事件表
flood_event_df = pd.read_excel(flood_event_path)
flood_event_df["station_code"] = flood_event_df["FloodEvent_797"].apply(lambda x: str(x).split('_')[0])

os.makedirs(output_folder, exist_ok=True)

def get_basin_code(filename):
    # 从文件名中提取流域编码
    match = re.search(r"ST_RIVER_(\d+)_R.*\.xlsx", filename)
    return match.group(1) if match else None


def main():
    for q_file in os.listdir(q_folder):
        if q_file.endswith(".xlsx"):
            basin_code = get_basin_code(q_file)
            if not basin_code:
                continue
            q_filepath = os.path.join(q_folder, q_file)
            q_df = pd.read_excel(q_filepath)
            # 只保留 TM 和 Q 两列
            q_df_main = q_df[["TM", "Q"]].copy()
            # 转换 TM 列为 datetime 类型
            q_df_main["TM"] = pd.to_datetime(q_df_main["TM"])
            # 去重，保留每个时间点的第一条数据
            q_df_main = q_df_main.drop_duplicates(subset="TM")
            # 生成完整的时间序列（以小时为步长）
            full_time = pd.date_range(
                start=q_df_main["TM"].min(),
                end=q_df_main["TM"].max(),
                freq="h"  # 用小写"h"，避免FutureWarning
            )
            # 以完整时间为主，重新对齐原始数据
            q_df_main = q_df_main.set_index("TM").reindex(full_time).reset_index()
            q_df_main.rename(columns={"index": "TM"}, inplace=True)
            # 计算每小时流量深度（mm/h），写入新列 streamflow_obs_mm
            area_km2 = BASIN_AREAS.get(basin_code)
            if area_km2:
                q_df_main["streamflow_obs_mm"] = q_df_main["Q"] / (area_km2 * 1e6) * 3600 * 1000
            else:
                q_df_main["streamflow_obs_mm"] = None
            # 增加洪水事件标记列 flood_event，默认空值（NaN）
            import numpy as np
            q_df_main["flood_event"] = np.nan
            # 查找该站点的所有洪水事件
            events = flood_event_df[flood_event_df["station_code"] == basin_code]
            for _, event in events.iterrows():
                start = pd.to_datetime(event["FloodEvent_Start"])
                end = pd.to_datetime(event["FloodEvent_End"])
                mask = (q_df_main["TM"] >= start) & (q_df_main["TM"] <= end)
                q_df_main.loc[mask, "flood_event"] = 1
            # 保存为 CSV
            q_df_main.rename(columns={"TM": "time", "Q": "streamflow_obs_m3s"}, inplace=True)
            output_path = os.path.join(output_folder, f"Anhui_{basin_code}_Q_Anhui.csv")
            q_df_main.to_csv(output_path, index=False)
            print(f"已生成: {output_path}")

if __name__ == "__main__":
    main()