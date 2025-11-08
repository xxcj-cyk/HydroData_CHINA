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

# Set Chinese font support
try:
    plt.rcParams['font.sans-serif'] = ['SimHei']  # For displaying Chinese labels
    plt.rcParams['axes.unicode_minus'] = False  # For displaying minus signs
except:
    print("Warning: Unable to set Chinese font, Chinese text in charts may not display correctly")

def plot_overall_metrics(df, output_dir):
    """
    Plot boxplots for overall NSE and PFE
    """
    # Create two subplots
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))
    
    # NSE boxplot
    sns.boxplot(y=df['nse'], ax=axes[0])
    axes[0].set_title('Overall NSE Distribution', fontsize=16)
    axes[0].set_ylabel('NSE', fontsize=14)
    axes[0].grid(True, linestyle='--', alpha=0.7)
    axes[0].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # Set NSE y-axis range from 0 to 1
    axes[0].set_ylim(0, 1)
    
    # PFE boxplot
    sns.boxplot(y=df['pfe'], ax=axes[1])
    axes[1].set_title('Overall PFE Distribution', fontsize=16)
    axes[1].set_ylabel('PFE (%)', fontsize=14)
    axes[1].grid(True, linestyle='--', alpha=0.7)
    axes[1].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # Set PFE y-axis range from -100 to 100
    axes[1].set_ylim(-100, 100)
    
    # Save figure
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'overall_metrics_boxplot.png')
    plt.savefig(output_file, dpi=300)
    print(f"Overall metrics boxplot saved to: {output_file}")
    plt.close()

def plot_basin_metrics(df, output_dir):
    """
    Plot boxplots for NSE and PFE by basin
    """
    # Create two subplots
    fig, axes = plt.subplots(2, 1, figsize=(14, 12))
    
    # NSE boxplot (grouped by basin)
    sns.boxplot(x='basin_short_id', y='nse', data=df, ax=axes[0])
    axes[0].set_title('NSE Distribution by Basin', fontsize=16)
    axes[0].set_xlabel('Basin ID', fontsize=14)
    axes[0].set_ylabel('NSE', fontsize=14)
    axes[0].grid(True, linestyle='--', alpha=0.7)
    axes[0].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # Set NSE y-axis range from 0 to 1
    axes[0].set_ylim(0, 1)
    # Rotate x-axis labels to prevent overlap
    axes[0].tick_params(axis='x', rotation=90)
    
    # PFE boxplot (grouped by basin)
    sns.boxplot(x='basin_short_id', y='pfe', data=df, ax=axes[1])
    axes[1].set_title('PFE Distribution by Basin', fontsize=16)
    axes[1].set_xlabel('Basin ID', fontsize=14)
    axes[1].set_ylabel('PFE (%)', fontsize=14)
    axes[1].grid(True, linestyle='--', alpha=0.7)
    axes[1].axhline(y=0, color='r', linestyle='-', alpha=0.3)
    # Set PFE y-axis range from -100 to 100
    axes[1].set_ylim(-100, 100)
    # Rotate x-axis labels to prevent overlap
    axes[1].tick_params(axis='x', rotation=90)
    
    # Save figure
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'basin_metrics_boxplot.png')
    plt.savefig(output_file, dpi=300)
    print(f"Basin metrics boxplot saved to: {output_file}")
    plt.close()

def plot_basin_metrics_facet(df, output_dir):
    """
    Plot boxplots for NSE and PFE by basin using FacetGrid
    """
    # Extract different basin IDs
    basin_ids = df['basin_short_id'].unique()
    
    # Create long-format data for facet plotting
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
    
    # Convert to DataFrame
    nse_df = pd.DataFrame([(d['basin_id'], val) for d in nse_data for val in d['values']], 
                          columns=['basin_id', 'NSE'])
    pfe_df = pd.DataFrame([(d['basin_id'], val) for d in pfe_data for val in d['values']], 
                          columns=['basin_id', 'PFE'])
    
    # Set canvas
    plt.figure(figsize=(16, 12))
    
    # Plot NSE and PFE boxplots
    plt.subplot(2, 1, 1)
    sns.boxplot(x='basin_id', y='NSE', data=nse_df)
    plt.title('NSE Distribution by Basin', fontsize=16)
    plt.xlabel('Basin ID', fontsize=14)
    plt.ylabel('NSE', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
    plt.ylim(0, 1)  # Set NSE y-axis range from 0 to 1
    plt.xticks(rotation=90)
    
    plt.subplot(2, 1, 2)
    sns.boxplot(x='basin_id', y='PFE', data=pfe_df)
    plt.title('PFE Distribution by Basin', fontsize=16)
    plt.xlabel('Basin ID', fontsize=14)
    plt.ylabel('PFE (%)', fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
    plt.ylim(-100, 100)  # Set PFE y-axis range from -100 to 100
    plt.xticks(rotation=90)
    
    # Save figure
    plt.tight_layout()
    output_file = os.path.join(output_dir, 'basin_metrics_facet.png')
    plt.savefig(output_file, dpi=300)
    print(f"Basin metrics FacetGrid plot saved to: {output_file}")
    plt.close()

def main():
    # Set file path
    csv_file = r"e:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_7\evaluation_results.csv"
    output_dir = os.path.dirname(csv_file)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Read CSV file
    df = pd.read_csv(csv_file)
    print(f"Successfully read data, total {len(df)} records")
    
    # Extract actual basin number from full basin_id
    df['basin_short_id'] = df['basin_id'].str.extract(r'Anhui_(\d+)_')
    
    # Extract different basins
    unique_basins = df['basin_short_id'].unique()
    print(f"Data contains {len(unique_basins)} different basins: {unique_basins}")
    
    # Plot overall NSE and PFE boxplots
    plot_overall_metrics(df, output_dir)
    
    # Plot NSE and PFE boxplots by basin
    plot_basin_metrics(df, output_dir)
    
    # Plot NSE and PFE boxplots by basin using FacetGrid
    plot_basin_metrics_facet(df, output_dir)
    
    # Calculate metric statistics by basin
    basin_stats = df.groupby('basin_short_id').agg({
        'nse': ['mean', 'std', 'min', 'max', 'median'],
        'pfe': ['mean', 'std', 'min', 'max', 'median']
    })
    
    # Save statistics to CSV
    stats_file = os.path.join(output_dir, 'basin_metrics_stats.csv')
    basin_stats.to_csv(stats_file)
    print(f"Basin metrics statistics saved to: {stats_file}")

if __name__ == "__main__":
    main()
