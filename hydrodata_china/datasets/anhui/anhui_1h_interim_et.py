"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-09 11:06:49
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-10 15:30:25
"""

import os
import pandas as pd
import numpy as np
import datetime
import calendar
import logging
import xarray as xr  # 添加xarray库导入

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def read_basin_evaporation_mapping(mapping_file_path):
    """
    读取流域ID和对应蒸发站的映射关系
    Args:
        mapping_file_path: 包含映射关系的xlsx文件路径  
    Returns:
        字典，键为流域ID，值为对应蒸发站名称
    """
    # 读取sheet2
    mapping_df = pd.read_excel(mapping_file_path, sheet_name=1)
    # 创建映射字典
    basin_to_station = dict(zip(mapping_df["流域ID"], mapping_df["对应蒸发站"]))
    logging.info(f"成功读取映射关系，共{len(basin_to_station)}个流域")
    return basin_to_station


def read_evaporation_data(evaporation_file_path, station_names):
    """
    读取蒸发数据文件中的各个蒸发站数据
    Args:
        evaporation_file_path: 包含蒸发数据的xlsx文件路径
        station_names: 需要读取的蒸发站名称列表  
    Returns:
        字典，键为蒸发站名称，值为包含该站月平均蒸发数据的DataFrame和多年平均值
    """
    station_data = {}
    # 获取文件中的所有sheet名称
    xl = pd.ExcelFile(evaporation_file_path)
    available_sheets = xl.sheet_names
    logging.info(f"开始读取蒸发数据，需要处理{len(station_names)}个蒸发站")
    # 遍历需要的蒸发站
    for station in station_names:
        if station in available_sheets:
            # 读取对应sheet的数据
            df = pd.read_excel(evaporation_file_path, sheet_name=station)
            # 处理表格格式：设置列名
            if len(df.columns) >= 13:  # 确保有足够的列（年份+12个月）
                # 重命名列
                new_columns = ['年'] + [f'{i}月' for i in range(1, 13)]
                df.columns = new_columns[:len(df.columns)]
                
                # 保存多年平均值（最后一行）
                multi_year_avg = None
                if len(df) > 0:
                    multi_year_avg = df.iloc[-1].copy()
                    # 删除最后一行（多年平均值）用于常规处理
                    df = df.iloc[:-1].copy()
                
                # 确保年份列是数值类型
                df['年'] = pd.to_numeric(df['年'], errors='coerce')
                # 过滤掉无效年份的行
                df = df.dropna(subset=['年'])
                station_data[station] = {'data': df, 'multi_year_avg': multi_year_avg}
                logging.info(f"成功读取蒸发站'{station}'的数据，共{len(df)}年")
            else:
                logging.warning(f"蒸发站'{station}'的数据格式不正确，列数不足")
        else:
            logging.warning(f"蒸发站'{station}'在文件中不存在")
    return station_data


def convert_monthly_to_hourly(monthly_data, start_year, end_year):
    """
    将月平均蒸发数据转换为小时尺度数据
    Args:
        monthly_data: 包含月平均蒸发数据的字典，包含'data'和'multi_year_avg'两个键
        start_year: 起始年份
        end_year: 结束年份
    Returns:
        包含小时尺度蒸发数据的DataFrame
    """
    # 创建一个空的DataFrame来存储小时数据
    hourly_data = []
    monthly_df = monthly_data['data']
    multi_year_avg = monthly_data['multi_year_avg']
    
    logging.info(f"开始将月平均数据转换为小时尺度数据，处理成{start_year}至{end_year}年的数据")
    # 获取年份列和月份列
    year_col = '年'
    month_cols = [f'{i}月' for i in range(1, 13)]  # 1-12月的列名
    # 遍历年份范围
    for year in range(start_year, end_year + 1):
        # 查找该年份的数据
        year_data = monthly_df[monthly_df[year_col] == year]
        if len(year_data) == 0:
            # 如果没有该年份的数据，则使用多年平均值
            if multi_year_avg is not None:
                start_date = datetime.datetime(year, 1, 1, 0, 0, 0)
                end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='H')
                
                # 使用多年平均值计算每月的小时数据
                month_hourly_data = []
                for month_idx, month_col in enumerate(month_cols, 1):
                    # 获取该月的天数
                    days_in_month = calendar.monthrange(year, month_idx)[1]
                    # 获取该月的多年平均蒸发量
                    monthly_evap = multi_year_avg[month_col] if month_col in multi_year_avg.index else np.nan
                    
                    # 如果月平均值为NaN，则该月所有小时都设为NaN
                    if pd.isna(monthly_evap):
                        daily_evap = np.nan
                        hourly_evap = np.nan
                    else:
                        # 将月平均值转换为日平均值（假设每月的日平均值相同）
                        daily_evap = monthly_evap / days_in_month
                        # 将日平均值转换为小时平均值（假设每天的小时平均值相同）
                        hourly_evap = daily_evap / 24
                    
                    # 创建该月的小时时间序列
                    start_date = datetime.datetime(year, month_idx, 1, 0, 0, 0)
                    if month_idx < 12:
                        end_date = datetime.datetime(year, month_idx + 1, 1, 0, 0, 0)
                    else:
                        end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                    dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='H')
                    month_hourly = pd.DataFrame({'time': dates, 'evaporation': hourly_evap})
                    month_hourly_data.append(month_hourly)
                
                # 合并该年所有月的小时数据
                year_hourly = pd.concat(month_hourly_data, ignore_index=True)
                hourly_data.append(year_hourly)
                logging.warning(f"{year}年的数据不存在，已使用多年平均值计算")
            else:
                # 如果没有多年平均值，则设为NaN
                start_date = datetime.datetime(year, 1, 1, 0, 0, 0)
                end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='H')
                year_hourly = pd.DataFrame({'time': dates, 'evaporation': np.nan})
                hourly_data.append(year_hourly)
                logging.warning(f"{year}年的数据不存在，且无多年平均值，已设置为NaN")
        else:
            # 获取该年份的月平均蒸发数据
            row = year_data.iloc[0]
            # 遍历每个月
            for month_idx, month_col in enumerate(month_cols, 1):
                # 获取该月的天数
                days_in_month = calendar.monthrange(year, month_idx)[1]
                # 获取该月的月平均蒸发量
                monthly_evap = row[month_col] if month_col in row.index else np.nan
                # 如果月平均值为NaN，则该月所有小时都设为NaN
                if pd.isna(monthly_evap):
                    daily_evap = np.nan
                    hourly_evap = np.nan
                else:
                    # 将月平均值转换为日平均值（假设每月的日平均值相同）
                    daily_evap = monthly_evap / days_in_month
                    # 将日平均值转换为小时平均值（假设每天的小时平均值相同）
                    hourly_evap = daily_evap / 24
                # 创建该月的小时时间序列
                start_date = datetime.datetime(year, month_idx, 1, 0, 0, 0)
                if month_idx < 12:
                    end_date = datetime.datetime(year, month_idx + 1, 1, 0, 0, 0)
                else:
                    end_date = datetime.datetime(year + 1, 1, 1, 0, 0, 0)
                dates = pd.date_range(start=start_date, end=end_date - datetime.timedelta(seconds=1), freq='H')
                month_hourly = pd.DataFrame({'time': dates, 'evaporation': hourly_evap})
                hourly_data.append(month_hourly)
    # 合并所有小时数据
    hourly_df = pd.concat(hourly_data, ignore_index=True)
    logging.info(f"小时尺度数据转换完成，共生成{len(hourly_df)}条记录")
    return hourly_df


def process_evaporation_data(basin_station_mapping, station_data, output_dir, start_year, end_year):
    """
    处理蒸发数据并按流域生成NC文件
    Args:
        basin_station_mapping: 流域ID到蒸发站的映射字典
        station_data: 蒸发站数据字典
        output_dir: 输出目录
        start_year: 起始年份
        end_year: 结束年份
    """
    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"开始处理蒸发数据，共{len(basin_station_mapping)}个流域")
    # 遍历每个流域
    processed_count = 0
    for basin_id, station_name in basin_station_mapping.items():
        if station_name not in station_data:
            logging.warning(f"流域{basin_id}对应的蒸发站'{station_name}'数据不可用，跳过处理")
            continue
        # 获取该蒸发站的月平均数据
        monthly_data = station_data[station_name]  # 现在是一个包含'data'和'multi_year_avg'的字典
        # 转换为小时尺度
        hourly_df = convert_monthly_to_hourly(monthly_data, start_year, end_year)
        # 将DataFrame转换为xarray Dataset
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
        # 保存为NC文件
        output_file = os.path.join(output_dir, f"{basin_id}_ET.nc")
        ds.to_netcdf(output_file)
        processed_count += 1
        logging.info(f"已保存流域{basin_id}的蒸发数据到{output_file}，共{len(hourly_df)}条记录")
    logging.info(f"处理完成，共处理了{processed_count}个流域的数据")

if __name__ == "__main__":
    # 设置文件路径
    mapping_file_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\ET_Station_21\流域蒸发站对应表.xlsx"
    evaporation_file_path = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\ET_Station_21\多年平均月蒸散发.xlsx"
    output_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_ET"
    # 设置年份范围
    start_year = 1959
    end_year = 2022
    logging.info("开始处理安徽流域蒸发数据")
    # 读取流域和蒸发站的映射关系
    basin_station_mapping = read_basin_evaporation_mapping(mapping_file_path)
    # 获取所有需要的蒸发站名称
    station_names = set(basin_station_mapping.values())
    # 读取蒸发站数据
    station_data = read_evaporation_data(evaporation_file_path, station_names)
    # 处理数据并生成NC文件
    process_evaporation_data(basin_station_mapping, station_data, output_dir, start_year, end_year)
    logging.info("安徽流域蒸发数据处理完成")