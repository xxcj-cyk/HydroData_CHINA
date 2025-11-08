"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-08-18 11:25:20
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-21 22:57:59
"""

import os
import pandas as pd

# 文件夹路径
q_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Q_Station_21"
rain_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Pmean_Basin_21"
output_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Merged"

os.makedirs(output_folder, exist_ok=True)

def get_basin_code(filename, pattern):
    # 从文件名中提取流域编码
    import re
    match = re.search(pattern, filename)
    return match.group(1) if match else None

def main():
    # 遍历雨量文件夹
    for rain_file in os.listdir(rain_folder):
        if rain_file.endswith(".csv"):
            basin_code = get_basin_code(rain_file, r"basin_Anhui_(\d+)_rainfall\.csv")
            if not basin_code:
                continue
            # 匹配Q文件，只根据流域编码
            q_file_found = None
            for q_file in os.listdir(q_folder):
                if basin_code in q_file and q_file.endswith(".xlsx"):
                    q_file_found = q_file
                    break
            if not q_file_found:
                print(f"未找到流量文件，流域编码: {basin_code}")
                continue
            q_filepath = os.path.join(q_folder, q_file_found)
            rain_filepath = os.path.join(rain_folder, rain_file)

            # 读取数据
            q_df = pd.read_excel(q_filepath)
            rain_df = pd.read_csv(rain_filepath)

            q_df_main = q_df[["TM", "Q"]].copy()

            # 转换TM列为datetime类型
            q_df_main["TM"] = pd.to_datetime(q_df_main["TM"])
            rain_df["TM"] = pd.to_datetime(rain_df["TM"])

            # 合并，按TM匹配（以雨量文件为主表）
            merged_df = pd.merge(
                rain_df,
                q_df_main,
                on="TM",
                how="left"
            )

            # 调整Q为第二列
            cols = merged_df.columns.tolist()
            if "Q" in cols:
                cols.remove("Q")
                cols.insert(1, "Q")
                merged_df = merged_df[cols]

            # 保存
            output_path = os.path.join(output_folder, f"merged_{basin_code}.csv")
            merged_df.to_csv(output_path, index=False)
            print(f"已生成: {output_path}")

if __name__ == "__main__":
    main()
