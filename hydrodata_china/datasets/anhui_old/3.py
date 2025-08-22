"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-08-19 13:26:14
@Last Modified by:		Yikai CHAI
@Last Modified time:	2025-08-20 18:10:09
"""

import os
import pandas as pd

# 定义文件路径
merged_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Merged"
summary_file = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21\FloodEvent_797.xlsx"
output_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21"

# 确保输出文件夹存在
os.makedirs(output_folder, exist_ok=True)

print("正在加载洪水事件汇总表...")
try:
    df_summary = pd.read_excel(summary_file)
    # 将时间列转换为datetime类型以便于筛选
    df_summary['Warmup_Start'] = pd.to_datetime(df_summary['Warmup_Start'])
    df_summary['FloodEvent_End'] = pd.to_datetime(df_summary['FloodEvent_End'])
    print("洪水事件汇总表加载成功。")
except FileNotFoundError:
    print(f"错误: 找不到文件 {summary_file}。请确保文件与脚本在同一目录下。")
    exit()
except Exception as e:
    print(f"加载洪水事件汇总表时发生错误: {e}")
    exit()

print("开始处理Merged文件夹下的文件...")

# 获取Merged文件夹下的所有CSV文件
merged_files = [f for f in os.listdir(merged_folder) if f.startswith('merged_') and f.endswith('.csv')]

if not merged_files:
    print(f"警告: 在 {merged_folder} 文件夹中没有找到'merged_'开头的CSV文件。")
else:
    for filename in merged_files:
        print(f"\n正在处理文件: {filename}")
        
        # 从文件名中提取站点编号
        try:
            station_id = filename.split('_')[1].split('.')[0]
        except IndexError:
            print(f"跳过文件 {filename}，因为文件名格式不正确。")
            continue

        # 查找所有与该站点编号匹配的事件
        matching_events = df_summary[df_summary['FloodEvent_797'].str.contains(f'_{station_id}_', na=False)]
        
        if matching_events.empty:
            print(f"未在汇总表中找到与站点 {station_id} 相关的事件。")
            continue

        # 读取对应的merged文件
        merged_filepath = os.path.join(merged_folder, filename)
        try:
            df_merged = pd.read_csv(merged_filepath, encoding='gbk')
            # 确保数据框中包含'TM'列，并转换为datetime
            if 'TM' in df_merged.columns:
                df_merged['TM'] = pd.to_datetime(df_merged['TM'])
            else:
                print(f"警告: 文件 {filename} 中未找到'TM'列，跳过处理。")
                continue
        except Exception as e:
            print(f"读取文件 {filename} 时发生错误: {e}")
            continue

        # 遍历所有匹配的事件，并生成单独的CSV
        for _, row in matching_events.iterrows():
            event_id = row['FloodEvent_797']
            start_time = row['Warmup_Start']
            end_time = row['FloodEvent_End']

            # 筛选数据
            try:
                filtered_df = df_merged[
                    (df_merged['TM'] >= start_time) & (df_merged['TM'] <= end_time)
                ].copy()
            except Exception as e:
                print(f"筛选事件 {event_id} 的数据时发生错误: {e}")
                continue

            if not filtered_df.empty:
                # 定义输出文件路径
                output_filepath = os.path.join(output_folder, f"{event_id}.csv")
                
                # 保存筛选后的数据
                filtered_df.to_csv(output_filepath, index=False)
                print(f"已为事件 {event_id} 生成文件: {os.path.basename(output_filepath)}")
            else:
                print(f"事件 {event_id} 在merged文件中没有找到匹配的时间段，未生成文件。")

print("\n所有文件处理完毕。")