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
    Evaluate all metrics and return a dictionary. RMSE is multiplied by area (km2).
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
    # Adjust RMSE
    if basin_id is not None and basin_id in BASIN_AREAS:
        metrics['rmse'] = metrics['rmse'] * BASIN_AREAS[basin_id]
    return metrics

def process_csv_files(csv_dir):
    """
    Process all CSV files and calculate all metrics
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
                metrics['basin_id'] = basin_id  # Keep for subsequent mapping
                results.append(metrics)
            else:
                print(f"Warning: {file} is missing required columns 'streamflow_obs' or 'streamflow_pred'.")
    columns = ['basin', 'basin_id', 'nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']
    return pd.DataFrame(results)[columns]

def plot_metric_boxplot(df, metric, output_dir=None):
    """
    Plot boxplot for the specified metric, set y-axis range, mark X-axis as A01~A21, and annotate median on each box
    """
    plt.figure(figsize=(12, 8))
    sns.set_style("whitegrid")
    df = df.copy()
    df['basin_label'] = df['basin_id'].map(BASIN_ID_TO_LABEL)  # This won't raise an error
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
        ax.set_yticklabels(ax.get_yticklabels(), fontsize=18)  # Combined with settings

    # Calculate and annotate median
    medians = df.groupby('basin_label')[metric].median()
    ymin, ymax = ylims.get(metric, (None, None))
    for i, label in enumerate(basin_order):
        if label in medians:
            median_val = medians[label]
            # Only annotate when median is within ylims range
            if ymin is not None and ymax is not None and (median_val < ymin or median_val > ymax):
                continue
            # Set format based on metric type
            if metric in ['rmse', 'pte', 'pfe']:
                median_str = f"{int(round(median_val))}"
            else:
                median_str = f"{median_val:.2f}"
            # Annotate slightly above the box
            ax.text(i, median_val + (ymax - ymin) * 0.001, 
                    median_str, 
                    ha='center', va='bottom', fontsize=14, color='black')

    plt.tight_layout()
    if output_dir:
        output_path = os.path.join(output_dir, f'{metric}_boxplot.png')
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        print(f"{metric.upper()} boxplot saved to: {output_path}")
    # plt.show()

def main():
    csv_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Period\Anhui_dPL\nc2csv_period"
    output_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Visualization\Sec1_ModelPerf\Period\Anhui_dPL\dPL_Local"
    os.makedirs(output_dir, exist_ok=True)
    df = process_csv_files(csv_dir)
    # Print statistics
    print("\nStatistics:")
    for basin_id, group in df.groupby('basin_id'):
        print(
            f"Basin {basin_id}: "
            f"NSE_mean={group['nse'].mean():.3f}, "
            f"KGE_mean={group['kge'].mean():.3f}, "
            f"Corr_mean={group['corr'].mean():.3f}, "
            f"RMSE_mean={group['rmse'].mean():.1f}, "
            f"PFE_mean={group['pfe'].mean():.1f}, "
            f"PTE_mean={group['pte'].mean():.1f}, "
            f"sample_count={len(group)}"
        )
    # Plot boxplots for each metric
    for metric in ['nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']:
        plot_metric_boxplot(df, metric, output_dir)
    # Plot overall NSE and PFE boxplots for all basins
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
        # Annotate median
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
        print(f"Overall {metric.upper()} boxplot for all basins saved to: {output_path}")
        # plt.show()
    # Export only required fields
    export_columns = ['basin', 'nse', 'kge', 'corr', 'rmse', 'pfe', 'pte']
    base_name = "_".join(os.path.normpath(csv_dir).split(os.sep)[-2:]) + "_Evaluation.csv"
    results_path = os.path.join(output_dir, base_name)
    df[export_columns].to_csv(results_path, index=False)
    print(f"All evaluation results saved to: {results_path}")

if __name__ == "__main__":
    main()
