"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-27 17:35:39
@Last Modified by:  Yikai CHAI
@Last Modified time:2025-06-27 17:39:27
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

# 设置中文字体支持
try:
    plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
    plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号
except:
    print("警告: 无法设置中文字体，图表中文可能无法正确显示")

def plot_overall_metrics(df, output_dir):
    """
    绘制总体NSE和PFE的箱型图
    """
    # 创建两个子图
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))
    
    # NSE箱型图
    sns.boxplot(y=df['nse'], ax=axes[0])
    axes[0].set_title('总体NSE分布', fontsize=16)
    axes[0].set_ylabel('NSE', fontsize=14)
    axes[0].grid(True, linestyle='--', alpha=0.7)
    axes[0].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # 设置NSE的y轴范围为0到1
    axes[0].set_ylim(0, 1)
    
    # PFE箱型图
    sns.boxplot(y=df['pfe'], ax=axes[1])
    axes[1].set_title('总体PFE分布', fontsize=16)
    axes[1].set_ylabel('PFE (%)', fontsize=14)
    axes[1].grid(True, linestyle='--', alpha=0.7)
    axes[1].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # 设置PFE的y轴范围为-100到100
    axes[1].set_ylim(-100, 100)
    
    # 保存图片
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'overall_metrics_boxplot.png')
    plt.savefig(output_file, dpi=300)
    print(f"总体指标箱型图已保存至: {output_file}")
    plt.close()

def plot_basin_metrics(df, output_dir):
    """
    绘制各流域NSE和PFE的箱型图
    """
    # 创建两个子图
    fig, axes = plt.subplots(2, 1, figsize=(14, 12))
    
    # NSE箱型图 (按流域分组)
    sns.boxplot(x='basin_short_id', y='nse', data=df, ax=axes[0])
    axes[0].set_title('各流域NSE分布', fontsize=16)
    axes[0].set_xlabel('流域ID', fontsize=14)
    axes[0].set_ylabel('NSE', fontsize=14)
    axes[0].grid(True, linestyle='--', alpha=0.7)
    axes[0].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # 设置NSE的y轴范围为0到1
    axes[0].set_ylim(0, 1)
    # 旋转x轴标签以防重叠
    axes[0].tick_params(axis='x', rotation=90)
    
    # PFE箱型图 (按流域分组)
    sns.boxplot(x='basin_short_id', y='pfe', data=df, ax=axes[1])
    axes[1].set_title('各流域PFE分布', fontsize=16)
    axes[1].set_xlabel('流域ID', fontsize=14)
    axes[1].set_ylabel('PFE (%)', fontsize=14)
    axes[1].grid(True, linestyle='--', alpha=0.7)
    axes[1].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # 设置PFE的y轴范围为-100到100
    axes[1].set_ylim(-100, 100)
    # 旋转x轴标签以防重叠
    axes[1].tick_params(axis='x', rotation=90)
    
    # 保存图片
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'basin_metrics_boxplot.png')
    plt.savefig(output_file, dpi=300)
    print(f"各流域指标箱型图已保存至: {output_file}")
    plt.close()

def plot_basin_metrics_facet(df, output_dir):
    """
    使用FacetGrid绘制各流域NSE和PFE的箱型图
    """
    # 提取不同的流域ID
    basin_ids = df['basin_short_id'].unique()
    
    # 创建长格式的数据用于facet绘图
    nse_data = []
    pfe_data = []
    
    for basin_id in basin_ids:
        basin_df = df[df['basin_short_id'] == basin_id]
        nse_data.append({
            'basin_id': basin_id,
            'metric': 'NSE',
            'values': basin_df['nse'].tolist()
        })
        pfe_data.append({
            'basin_id': basin_id,
            'metric': 'PFE',
            'values': basin_df['pfe'].tolist()
        })
    
    # 转换为DataFrame
    nse_df = pd.DataFrame([(d['basin_id'], val) for d in nse_data for val in d['values']], 
                          columns=['basin_id', 'NSE'])
    pfe_df = pd.DataFrame([(d['basin_id'], val) for d in pfe_data for val in d['values']], 
                          columns=['basin_id', 'PFE'])
    
    # 设置画布
    plt.figure(figsize=(16, 12))
    
    # 绘制NSE和PFE的箱型图
    plt.subplot(2, 1, 1)
    sns.boxplot(x='basin_id', y='NSE', data=nse_df)
    plt.title('各流域NSE分布', fontsize=16)
    plt.xlabel('流域ID', fontsize=14)
    plt.ylabel('NSE', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
    plt.ylim(0, 1)  # 设置NSE的y轴范围为0到1
    plt.xticks(rotation=90)
    
    plt.subplot(2, 1, 2)
    sns.boxplot(x='basin_id', y='PFE', data=pfe_df)
    plt.title('各流域PFE分布', fontsize=16)
    plt.xlabel('流域ID', fontsize=14)
    plt.ylabel('PFE (%)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
    plt.ylim(-100, 100)  # 设置PFE的y轴范围为-100到100
    plt.xticks(rotation=90)
    
    # 保存图片
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'basin_metrics_facet.png')
    plt.savefig(output_file, dpi=300)
    print(f"各流域指标FacetGrid图已保存至: {output_file}")
    plt.close()

def main():
    # 设置文件路径
    csv_file = r"e:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_7\evaluation_results.csv"
    output_dir = os.path.dirname(csv_file)
    
    # 确保输出目录存在
    os.makedirs(output_dir, exist_ok=True)
    
    # 读取CSV文件
    df = pd.read_csv(csv_file)
    print(f"成功读取数据，共 {len(df)} 条记录")
    
    # 从完整的basin_id中提取实际的流域编号
    df['basin_short_id'] = df['basin_id'].str.extract(r'Anhui_(\d+)_')
    
    # 提取不同的流域
    unique_basins = df['basin_short_id'].unique()
    print(f"数据中包含 {len(unique_basins)} 个不同的流域: {unique_basins}")
    
    # 绘制总体NSE和PFE的箱型图
    plot_overall_metrics(df, output_dir)
    
    # 绘制各流域NSE和PFE的箱型图
    plot_basin_metrics(df, output_dir)
    
    # 使用FacetGrid绘制各流域NSE和PFE的箱型图
    plot_basin_metrics_facet(df, output_dir)
    
    # 计算各流域的指标统计信息
    basin_stats = df.groupby('basin_short_id').agg({
        'nse': ['mean', 'std', 'min', 'max', 'median'],
        'pfe': ['mean', 'std', 'min', 'max', 'median']
    })
    
    # 保存统计结果到CSV
    stats_file = os.path.join(output_dir, 'basin_metrics_stats.csv')
    basin_stats.to_csv(stats_file)
    print(f"各流域指标统计信息已保存至: {stats_file}")

if __name__ == "__main__":
    main()
