"""@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-27 17:33:16
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-22 09:47:47

修改版本：删除前30天的数据，保留后续数据
"""

import os
import pandas as pd
import glob
import logging
import re
from pathlib import Path
from datetime import timedelta


# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# 添加流域面积字典 (km²)
BASIN_AREAS = {
    "50406910": 79.03,
    "50501200": 182.15,
    "50701100": 270.2,
    "50913900": 1390.24,
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


def read_flood_data(root_folder, skip_days=30):
    """读取洪水数据文件夹中的所有Excel文件，并删除前skip_days天的数据"""
    all_data = {}
    # 获取所有子文件夹（洪水场次文件夹）
    flood_folders = [f for f in os.listdir(root_folder) if os.path.isdir(os.path.join(root_folder, f))]
    
    for flood_folder in flood_folders:
        folder_path = os.path.join(root_folder, flood_folder)
        all_data[flood_folder] = {}
        
        # 获取文件夹中所有的Excel文件
        excel_files = glob.glob(os.path.join(folder_path, "*.xls")) + glob.glob(os.path.join(folder_path, "*.xlsx"))
        
        for excel_file in excel_files:
            file_name = os.path.basename(excel_file)
            
            # 根据文件扩展名选择适当的引擎
            engine = 'xlrd' if file_name.endswith('.xls') else 'openpyxl'
            df = pd.read_excel(excel_file, engine=engine)
            
            # 重命名列
            column_mapping = {
                '时间': 'time',
                '实测流量': 'streamflow_obs',
                '预报流量': 'streamflow_pred_xaj',
                '面雨量': 'P_Anhui'
            }
            df = df.rename(columns=column_mapping)
            
            # 将time列转换为datetime格式
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                
                # 删除前30天的数据
                if len(df) > 0:
                    start_time = df['time'].min()
                    cutoff_time = start_time + timedelta(days=skip_days)
                    original_rows = len(df)
                    df = df[df['time'] >= cutoff_time].copy()
                    filtered_rows = len(df)
                    
                    logging.info(f"文件 {file_name}: 原始数据 {original_rows} 行，删除前{skip_days}天后剩余 {filtered_rows} 行")
                    
                    # 如果删除后没有数据，跳过该文件
                    if len(df) == 0:
                        logging.warning(f"文件 {file_name} 删除前{skip_days}天后无剩余数据，跳过该文件")
                        continue
            
            # 处理雨量站列名
            for col in df.columns:
                if col not in column_mapping.values():
                    match = re.search(r'.*?([0-9]+)$', col)
                    if match:
                        station_code = match.group(1)
                        df = df.rename(columns={col: f'P_{station_code}'})
            
            # 将所有P_开头的列转换为float64
            for col in df.columns:
                if col.startswith('P_'):
                    df[col] = df[col].astype('float64')
            
            # 提取站点编码以获取流域面积
            station_code_match = re.search(r'_([0-9]+)_', file_name)
            if station_code_match:
                station_code = station_code_match.group(1)
                if station_code in BASIN_AREAS:
                    basin_area = BASIN_AREAS[station_code]
                    # 对于小时数据，需要除以24(转换成小时单位)
                    df['streamflow'] = df['streamflow_obs'] * 86.4 / basin_area / 24
            
            all_data[flood_folder][file_name] = df
    
    return all_data





def save_as_csv(data_dict, output_dir):
    """将数据保存为CSV格式"""
    os.makedirs(output_dir, exist_ok=True)
    saved_files = 0
    
    for flood_folder, files_data in data_dict.items():
        for file_name, df in files_data.items():
            # 提取站点编码
            station_match = re.search(r'_([0-9]+)_', file_name)
            if not station_match:
                logging.warning(f"无法从文件名 {file_name} 提取站点编码，跳过该文件")
                continue
            station_code = station_match.group(1)
            
            # 提取时间戳
            timestamp_match = re.search(r'_([0-9]+)\.xls', file_name)
            if not timestamp_match:
                logging.warning(f"无法从文件名 {file_name} 提取时间戳，跳过该文件")
                continue
            timestamp = timestamp_match.group(1)
            
            # 创建新的文件名
            new_filename = f"Anhui_{station_code}_{timestamp}_period.csv"
            output_path = os.path.join(output_dir, new_filename)
            
            # 保存为CSV文件
            df.to_csv(output_path, index=False)
            saved_files += 1
            logging.info(f"已保存CSV: {output_path}")
    
    return saved_files


if __name__ == "__main__":
    # 设置根文件夹路径
    root_folder = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21"
    output_dir_csv = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_FloodEvent_Period"
    
    # 读取所有数据（删除前30天）
    logging.info("开始读取洪水数据（将删除前30天数据）...")
    flood_data = read_flood_data(root_folder, skip_days=30)
    
    # 输出基本统计信息
    flood_count = len(flood_data)
    total_files = sum(len(files) for files in flood_data.values())
    total_rows = sum(sum(df.shape[0] for df in files.values()) for files in flood_data.values())
    
    logging.info("\n数据读取完成。基本统计信息:")
    logging.info(f"共读取了 {flood_count} 个洪水场次")
    logging.info(f"共读取了 {total_files} 个Excel文件")
    logging.info(f"共读取了 {total_rows} 行数据（已删除前30天）")
    
    # 输出各场次统计信息
    logging.info("\n各场次统计信息:")
    for flood_name, files_data in flood_data.items():
        files_count = len(files_data)
        rows_count = sum(df.shape[0] for df in files_data.values())
        logging.info(f"{flood_name}: 包含{files_count}个文件，共{rows_count}行数据")
    
    # 保存为CSV格式
    logging.info("\n开始保存为CSV格式...")
    saved_files_csv = save_as_csv(flood_data, output_dir_csv)
    logging.info(f"数据处理完成，共保存了 {saved_files_csv} 个CSV文件")