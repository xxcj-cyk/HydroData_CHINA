"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-04-26 17:20:12
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-04-26 17:26:18
"""

import os
import pandas as pd
from pathlib import Path

def rename_flow_column(directory_path):
    """
    遍历指定目录下的所有CSV文件，将列名'flow(m^3/s)'重命名为'streamflow'
    
    参数:
    directory_path (str): 包含CSV文件的目录路径
    """
    # 获取目录下所有CSV文件
    csv_files = list(Path(directory_path).glob('**/*.csv'))
    
    if not csv_files:
        print(f"在 {directory_path} 中没有找到CSV文件")
        return
    
    processed_count = 0
    skipped_count = 0
    
    for file_path in csv_files:
        try:
            # 读取CSV文件
            df = pd.read_csv(file_path)
            
            # 检查是否存在'flow(m^3/s)'列
            if 'flow(m^3/s)' in df.columns:
                # 重命名列
                df = df.rename(columns={'flow(m^3/s)': 'streamflow'})
                
                # 保存修改后的文件
                df.to_csv(file_path, index=False)
                print(f"已处理: {file_path}")
                processed_count += 1
            else:
                print(f"跳过: {file_path} (没有找到'flow(m^3/s)'列)")
                skipped_count += 1
        except Exception as e:
            print(f"处理 {file_path} 时出错: {str(e)}")
            skipped_count += 1
    
    print(f"\n处理完成: {processed_count} 个文件已修改，{skipped_count} 个文件已跳过")

if __name__ == "__main__":
    # 指定要处理的目录路径
    # 默认使用当前项目的根目录，您可以根据需要修改
    directory_to_process = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_China\Sanxia_Project\Streamflow"
    
    print(f"开始处理目录: {directory_to_process}")
    rename_flow_column(directory_to_process)