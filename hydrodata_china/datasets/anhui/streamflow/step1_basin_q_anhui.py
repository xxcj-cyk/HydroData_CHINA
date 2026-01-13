"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-08-21 23:00:36
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 18:52:21
"""

import os
import pandas as pd
import re
import geopandas as gpd


# 流量数据文件夹路径
BASIN_AREAS = {
    "50501200": 182.15,
    "50701100": 270.2,
    "62549024": 989,
    "62700110": 471.74,
    "62700700": 127.24,
    "62802400": 421.68,
    "62802700": 540.87,
    "62803300": 151.6,
    "62906900": 260.63,
    "62907100": 10.79,
    "62907600": 9.42,
    "62907601": 26.99,
    "62909400": 497.64,
    "62911200": 661.01,
    "70112150": 5.08,
    "70114100": 98.83
}

# 流量数据文件夹路径
BASIN_SHP = r"E:\GIS_Data\AnHui\Basin\Anhui_Basins_16.shp"
q_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Q_Station_21"
output_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui16_1H_Q"

os.makedirs(output_folder, exist_ok=True)

def load_target_basin_codes():
    """Load target basin codes from shapefile (16 basins)."""
    print("---Loading target basin codes from shapefile---")
    basins_gdf = gpd.read_file(BASIN_SHP)
    basin_ids_full = basins_gdf['Basin_ID'].tolist()
    target_basin_codes = set([bid.replace('Anhui_', '') for bid in basin_ids_full])
    print(f"Found {len(target_basin_codes)} target basins")
    return target_basin_codes

def get_basin_code(filename):
    # 从文件名中提取流域编码
    match = re.search(r"ST_RIVER_(\d+)_R.*\.xlsx", filename)
    return match.group(1) if match else None

def main():
    # Load target basin codes (16 basins)
    target_basin_codes = load_target_basin_codes()
    
    # Filter BASIN_AREAS to only include target basins
    filtered_basin_areas = {code: area for code, area in BASIN_AREAS.items() 
                           if code in target_basin_codes}
    print(f"Filtered BASIN_AREAS from {len(BASIN_AREAS)} to {len(filtered_basin_areas)} basins")
    
    processed_count = 0
    skipped_count = 0
    
    for q_file in os.listdir(q_folder):
        if q_file.endswith(".xlsx"):
            basin_code = get_basin_code(q_file)
            if not basin_code:
                continue
            
            # Only process if basin_code is in target basins
            if basin_code not in target_basin_codes:
                skipped_count += 1
                print(f"Skipping {q_file} (basin {basin_code} not in target 16 basins)")
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
            area_km2 = filtered_basin_areas.get(basin_code)
            if area_km2:
                q_df_main["streamflow_obs_mm"] = q_df_main["Q"] / (area_km2 * 1e6) * 3600 * 1000
            else:
                q_df_main["streamflow_obs_mm"] = None
            # 保存为 CSV
            q_df_main.rename(columns={"TM": "time", "Q": "streamflow_obs_m3s"}, inplace=True)
            output_path = os.path.join(output_folder, f"Anhui_{basin_code}_Q_Anhui.csv")
            q_df_main.to_csv(output_path, index=False)
            print(f"已生成: {output_path}")
            processed_count += 1
    
    print(f"\n处理完成: 处理了 {processed_count} 个文件，跳过了 {skipped_count} 个文件")

if __name__ == "__main__":
    main()