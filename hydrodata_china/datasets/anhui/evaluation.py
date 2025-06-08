"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-28 11:20:45
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-05-29 12:19:44
"""

import xarray as xr
import numpy as np
import pandas as pd
import os
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns

def calculate_nse(observed, simulated):
    """
    计算纳什效率系数(Nash-Sutcliffe Efficiency, NSE)
    """
    mask = ~np.isnan(observed) & ~np.isnan(simulated)
    observed = observed[mask]
    simulated = simulated[mask]
    
    if len(observed) == 0:
        return np.nan
    
    numerator = np.sum((observed - simulated) ** 2)
    denominator = np.sum((observed - np.mean(observed)) ** 2)
    nse = 1 - (numerator / denominator)
    
    return nse

def process_nc_files(folder_path):
    """
    处理文件夹中的所有NC文件，计算NSE并生成结果表格
    """
    # 获取所有NC文件
    nc_files = [f for f in os.listdir(folder_path) if f.endswith('.nc')]
    
    if not nc_files:
        print(f"在文件夹 {folder_path} 中未找到NC文件")
        return None, None
    
    # 存储结果的列表
    results = []
    basin_data = {}  # 用于存储每个流域的所有NSE值
    
    print(f"开始处理 {len(nc_files)} 个NC文件...")
    
    for nc_file in tqdm(nc_files):
        try:
            # 解析文件名
            file_id = nc_file[:-3]  # 去掉.nc后缀
            parts = file_id.split('_')
            basin_id = '_'.join(parts[:2])  # 流域ID
            
            # 读取NC文件
            file_path = os.path.join(folder_path, nc_file)
            with xr.open_dataset(file_path) as ds:
                observed = ds['streamflow_obs'].values
                predicted = ds['streamflow_pred_xaj'].values
            
            # 计算NSE
            nse = calculate_nse(observed, predicted)
            
            # 存储结果
            results.append({
                'flood_event_id': file_id,
                'basin_id': basin_id,
                'NSE_Validation': nse,
            })
            
            # 为计算流域平均值存储数据
            if basin_id not in basin_data:
                basin_data[basin_id] = []
            basin_data[basin_id].append(nse)
            
        except Exception as e:
            print(f"处理文件 {nc_file} 时出错: {str(e)}")
            continue
    
    # 创建DataFrame
    individual_df = pd.DataFrame(results)
    individual_df = individual_df[['flood_event_id', 'basin_id', 'NSE_Validation']]
    
    # 创建流域平均NSE的DataFrame
    basin_avg = {basin: np.nanmean(nses) for basin, nses in basin_data.items()}
    basin_avg_df = pd.DataFrame({
        'basin_id': list(basin_avg.keys()),
        'NSE_avarage': list(basin_avg.values()),
        'flood_event_amount': [len(basin_data[basin]) for basin in basin_avg.keys()]
    })
    
    return individual_df, basin_avg_df

def plot_basin_nse_boxplot(individual_df, output_path=None):
    """
    为每个流域的NSE值绘制箱型图
    
    参数:
    individual_df: 包含每个洪水场次NSE值的DataFrame
    output_path: 保存图像的路径，如果为None则显示图像而不保存
    """
    if individual_df is None or len(individual_df) == 0:
        print("没有数据可供绘图")
        return
    
    # 设置图形样式
    sns.set(style="whitegrid")
    plt.figure(figsize=(15, 8))
    
    # 绘制箱型图
    ax = sns.boxplot(x='basin_id', y='NSE_Validation', data=individual_df)
    
    # 设置图形标题和标签
    plt.title('各流域NSE值分布箱型图', fontsize=16)
    plt.xlabel('流域ID', fontsize=14)
    plt.ylabel('NSE值', fontsize=14)
    plt.xticks(rotation=45, ha='right')
    
    # 添加水平线表示NSE=0.5和NSE=0.7的阈值
    plt.axhline(y=0.5, color='r', linestyle='--', alpha=0.7, label='NSE=0.5')
    plt.axhline(y=0.7, color='g', linestyle='--', alpha=0.7, label='NSE=0.7')
    plt.legend()
    
    # 设置y轴范围为0到1
    plt.ylim(0, 1)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存或显示图像
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"箱型图已保存到: {output_path}")
    else:
        plt.show()

def plot_nse_cdf(individual_df, output_path=None):
    """
    绘制NSE值的累积分布函数(CDF)图
    
    参数:
    individual_df: 包含每个洪水场次NSE值的DataFrame
    output_path: 保存图像的路径，如果为None则显示图像而不保存
    """
    if individual_df is None or len(individual_df) == 0:
        print("没有数据可供绘图")
        return
    
    # 设置图形样式
    sns.set(style="whitegrid")
    plt.figure(figsize=(12, 8))
    
    # 过滤出NSE值在0-1之间的数据
    filtered_df = individual_df[(individual_df['NSE_Validation'] >= 0) & 
                              (individual_df['NSE_Validation'] <= 1)].copy()
    
    # 计算每个NSE值对应的累积概率
    nse_values = np.sort(filtered_df['NSE_Validation'].values)
    y = np.arange(1, len(nse_values)+1) / len(nse_values)
    
    # 绘制CDF曲线
    plt.plot(nse_values, y, marker='.', linestyle='-', alpha=0.7, linewidth=2)
    
    # 添加标记线
    plt.axvline(x=0.5, color='r', linestyle='--', alpha=0.7, label='NSE=0.5')
    plt.axvline(x=0.7, color='g', linestyle='--', alpha=0.7, label='NSE=0.7')
    
    # 添加百分比文本
    percent_above_05 = (filtered_df['NSE_Validation'] > 0.5).mean() * 100
    percent_above_07 = (filtered_df['NSE_Validation'] > 0.7).mean() * 100
    
    plt.text(0.52, 0.1, f'NSE>0.5: {percent_above_05:.1f}%', fontsize=12)
    plt.text(0.72, 0.1, f'NSE>0.7: {percent_above_07:.1f}%', fontsize=12)
    
    # 设置图形标题和标签
    plt.title('NSE值累积分布函数(CDF)', fontsize=16)
    plt.xlabel('NSE值', fontsize=14)
    plt.ylabel('累积概率', fontsize=14)
    
    # 设置坐标轴范围
    plt.xlim(0, 1)
    plt.ylim(0, 1)
    
    # 添加网格和图例
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=12)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存或显示图像
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"CDF图已保存到: {output_path}")
    else:
        plt.show()

# 使用示例
if __name__ == "__main__":
    # 替换为你的NC文件夹路径
    folder_path = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    
    # 处理文件并获取结果
    individual_df, basin_avg_df = process_nc_files(folder_path)
    
    if individual_df is not None and basin_avg_df is not None:
        # 保存单个场次洪水结果到CSV
        individual_output = os.path.join(folder_path, "flood_event_nse.csv")
        individual_df.to_csv(individual_output, index=False, encoding='utf-8-sig')
        print(f"单个场次洪水结果已保存到: {individual_output}")
        
        # 保存流域平均结果到CSV
        basin_avg_output = os.path.join(folder_path, "basin_avg_nse.csv")
        basin_avg_df.to_csv(basin_avg_output, index=False, encoding='utf-8-sig')
        print(f"流域平均结果已保存到: {basin_avg_output}")
        
        # 显示部分结果
        print("\n单个场次洪水结果预览:")
        print(individual_df.head())
        print("\n流域平均结果预览:")
        print(basin_avg_df.head())
        
        # 绘制箱型图并保存
        boxplot_output = os.path.join(folder_path, "basin_nse_boxplot.png")
        plot_basin_nse_boxplot(individual_df, boxplot_output)
        
        # 绘制CDF图并保存
        cdf_output = os.path.join(folder_path, "nse_cdf.png")
        plot_nse_cdf(individual_df, cdf_output)