import os
import pandas as pd
import glob
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_csv_files(input_dir, output_dir):
    """
    读取按年份存储的CSV文件，并将它们重新组织为按流域ID存储的文件
    
    参数:
    input_dir (str): 包含按年份存储的CSV文件的目录路径
    output_dir (str): 保存按流域ID组织的输出文件的目录路径
    """
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    # 获取所有CSV文件
    csv_files = glob.glob(os.path.join(input_dir, '*.csv'))
    logging.info(f'找到 {len(csv_files)} 个CSV文件')
    if not csv_files:
        logging.error(f'在 {input_dir} 中没有找到CSV文件')
        return
    # 创建一个字典来存储每个流域的数据
    basin_data = {}
    # 处理每个CSV文件
    for csv_file in csv_files:
        # 读取CSV文件
        df = pd.read_csv(csv_file)
        # 将时间列转换为datetime格式
        df['time_start'] = pd.to_datetime(df['time_start'])
        # 将温度从开尔文转换为摄氏度
        df['temperature_2m'] = df['temperature_2m'] - 273.15
        # 按流域ID分组
        for basin_id, group in df.groupby('basin_id'):
            if basin_id not in basin_data:
                basin_data[basin_id] = []
            basin_data[basin_id].append(group)
        logging.info(f'已处理文件 {csv_file}')
    # 合并并保存每个流域的数据
    for basin_id, data_frames in basin_data.items():
        # 合并该流域的所有数据
        basin_df = pd.concat(data_frames)
        # 按时间排序
        basin_df = basin_df.sort_values('time_start')
        # 删除可能的重复数据
        original_len = len(basin_df)
        basin_df = basin_df.drop_duplicates()
        if len(basin_df) < original_len:
            logging.info(f'删除了流域 {basin_id} 的重复数据，共 {original_len - len(basin_df)} 条')
        # 保存到CSV文件
        output_file = os.path.join(output_dir, f'basin_{basin_id}.csv')
        basin_df.to_csv(output_file, index=False)
        logging.info(f'已保存流域 {basin_id} 的数据到 {output_file}，共 {len(basin_df)} 条记录')
    logging.info(f'处理完成，共处理了 {len(basin_data)} 个流域的数据')

if __name__ == '__main__':
    # 设置输入和输出目录
    # 请根据实际情况修改这些路径
    input_directory = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_ERA5-Land_21"
    output_directory = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_PET_1H"
    
    # 处理CSV文件
    process_csv_files(input_directory, output_directory)