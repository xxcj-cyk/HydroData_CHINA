"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-11 11:26:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-30 16:58:49
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import re
import settings.Critical_Evaluation as crit

plt.rcParams['font.family'] = 'Arial'
sns.set(font='Arial')

BASIN_ID_TO_LABEL = {
    "50406910": "A01",
    "50501200": "A02",
    "50701100": "A03",
    "50913900": "A04",
    "51004350": "A05",
    "62549024": "A06",
    "62700110": "A07",
    "62700700": "A08",
    "62802400": "A09",
    "62802700": "A10",
    "62803300": "A11",
    "62902000": "A12",
    "62906900": "A13",
    "62907100": "A14",
    "62907600": "A15",
    "62907601": "A16",
    "62909400": "A17",
    "62911200": "A18",
    "62916110": "A19",
    "70112150": "A20",
    "70114100": "A21",
}

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

def evaluate_metrics(obs, pred, basin_id=None):
    """
    评估所有指标，返回字典。RMSE乘以面积（km2）。
    """
    obs = np.array(obs)
    pred = np.array(pred)
    mask = ~np.isnan(obs) & ~np.isnan(pred)
    obs = obs[mask]
    pred = pred[mask]
    if len(obs) < 10:
        return {k: np.nan for k in ['nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']}
    metrics = {
        'nse': crit.nse(obs, pred),
        'kge': crit.kge(obs, pred),
        'corr': crit.corr(obs, pred),
        'rmse': crit.rmse(obs, pred),
        'pfe': crit.pfe(obs, pred),
        'pte': crit.peak_time_error(obs, pred)
    }
    # 修正RMSE
    if basin_id is not None and basin_id in BASIN_AREAS:
        metrics['rmse'] = metrics['rmse'] * BASIN_AREAS[basin_id]
    return metrics

def process_csv_files(csv_dir):
    """
    处理所有CSV文件，计算所有指标
    """
    results = []
    csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
    pattern = r'Anhui_(\d+)_.*\.csv'
    for file in csv_files:
        match = re.match(pattern, file)
        if match:
            basin_id = match.group(1)
            file_path = os.path.join(csv_dir, file)
            df = pd.read_csv(file_path)
            if 'streamflow_obs' in df.columns and 'streamflow_pred' in df.columns:
                metrics = evaluate_metrics(df['streamflow_obs'], df['streamflow_pred'], basin_id=basin_id)
                metrics['basin'] = os.path.splitext(file)[0]
                metrics['basin_id'] = basin_id  # 保留用于后续映射
                results.append(metrics)
            else:
                print(f"警告: {file} 缺少必要的列 'streamflow_obs' 或 'streamflow_pred'。")
    columns = ['basin', 'basin_id', 'nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']
    return pd.DataFrame(results)[columns]

def plot_metric_boxplot(df, metric, output_dir=None):
    """
    绘制指定指标的箱型图，并设置纵坐标范围，X轴标记为A01~A21，并在每个箱子上标注中位数
    """
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    df = df.copy()
    df['basin_label'] = df['basin_id'].map(BASIN_ID_TO_LABEL)  # 这里不会报错
    basin_order = [f"A{str(i).zfill(2)}" for i in range(1, 22)]
    ax = sns.boxplot(x='basin_label', y=metric, data=df, palette="husl", order=basin_order)
    plt.ylabel(f'{metric.upper()}', fontsize=20)
    plt.xlabel('Basin', fontsize=20)
    plt.grid(True, linestyle='--', alpha=0.7)
    ax.set_xticklabels(ax.get_xticklabels(), fontsize=18)
    ylims = {
        'nse': (0, 1),
        'kge': (0, 1),
        'corr': (0.7, 1),
        'rmse': (0, 800),
        'pfe': (-80, 80),
        'pte': (-8, 8)
    }
    if metric in ylims:
        plt.ylim(ylims[metric])
        ax.set_yticklabels(ax.get_yticklabels(), fontsize=18)  # 结合设置

    # 计算中位数并标注
    medians = df.groupby('basin_label')[metric].median()
    ymin, ymax = ylims.get(metric, (None, None))
    for i, label in enumerate(basin_order):
        if label in medians:
            median_val = medians[label]
            # 只在中位数在ylims范围内时标注
            if ymin is not None and ymax is not None and (median_val < ymin or median_val > ymax):
                continue
            # 根据指标类型设置格式
            if metric in ['rmse', 'pte', 'pfe']:
                median_str = f"{int(round(median_val))}"
            else:
                median_str = f"{median_val:.2f}"
            # 在箱子上方稍微偏上一点标注
            ax.text(i, median_val + (ymax - ymin) * 0.001, 
                    median_str, 
                    ha='center', va='bottom', fontsize=14, color='black')

    plt.tight_layout()
    if output_dir:
        output_path = os.path.join(output_dir, f'{metric}_boxplot.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"{metric.upper()}箱型图已保存至: {output_path}")
    # plt.show()

def main():
    csv_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Period\Anhui_dPL\nc2csv_period"
    output_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Visualization\Sec1_ModelPerf\Period\Anhui_dPL\dPL_Local"
    os.makedirs(output_dir, exist_ok=True)
    df = process_csv_files(csv_dir)
    # 打印统计信息
    print("\n统计信息:")
    for basin_id, group in df.groupby('basin_id'):
        print(
            f"流域 {basin_id}: "
            f"NSE均值={group['nse'].mean():.3f}, "
            f"KGE均值={group['kge'].mean():.3f}, "
            f"Corr均值={group['corr'].mean():.3f}, "
            f"RMSE均值={group['rmse'].mean():.1f}, "
            f"PFE均值={group['pfe'].mean():.1f}, "
            f"PTE均值={group['pte'].mean():.1f}, "
            f"样本数={len(group)}"
        )
    # 绘制各指标箱型图
    for metric in ['nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']:
        plot_metric_boxplot(df, metric, output_dir)
    # 绘制所有流域整体NSE和PFE箱型图
    for metric in ['nse', 'pfe']:
        plt.figure(figsize=(6, 8))
        sns.set_style("whitegrid")
        ax = sns.boxplot(y=df[metric], color="skyblue")
        plt.ylabel(f'{metric.upper()}', fontsize=20)
        plt.xlabel('All Basins', fontsize=20)
        plt.grid(True, linestyle='--', alpha=0.7)
        ylims = {
            'nse': (0, 1),
            'pfe': (-80, 80)
        }
        if metric in ylims:
            plt.ylim(ylims[metric])
            ax.set_yticklabels(ax.get_yticklabels(), fontsize=18)
        # 标注中位数
        median_val = df[metric].median()
        ymin, ymax = ylims.get(metric, (None, None))
        if ymin is not None and ymax is not None and (median_val >= ymin and median_val <= ymax):
            if metric == 'pfe':
                median_str = f"{int(round(median_val))}"
            else:
                median_str = f"{median_val:.3f}"
            ax.text(0, median_val + (ymax - ymin) * 0.01, median_str, 
                    ha='center', va='bottom', fontsize=16, color='black')
        plt.tight_layout()
        output_path = os.path.join(output_dir, f'AllBasins_{metric}_boxplot.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"所有流域整体{metric.upper()}箱型图已保存至: {output_path}")
        # plt.show()
    # 只导出需要的字段
    export_columns = ['basin', 'nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']
    base_name = "_".join(os.path.normpath(csv_dir).split(os.sep)[-2:]) + "_Evaluation.csv"
    results_path = os.path.join(output_dir, base_name)
    df[export_columns].to_csv(results_path, index=False)
    print(f"所有评估结果已保存至: {results_path}")

if __name__ == "__main__":
    main()
