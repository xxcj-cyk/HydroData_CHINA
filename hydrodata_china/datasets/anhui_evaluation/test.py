"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-27 17:29:56
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-07-05 11:40:02
"""

import os
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path

def nash_sutcliffe(obs, sim):
    """计算纳什效率系数 (Nash-Sutcliffe Efficiency)"""
    # 去除缺失值
    idx = ~(np.isnan(obs) | np.isnan(sim))
    obs = obs[idx]
    sim = sim[idx]
    
    # 计算NSE
    if len(obs) == 0:
        return np.nan
    return 1 - np.sum((obs - sim) ** 2) / np.sum((obs - np.mean(obs)) ** 2)

def peak_flow_error(obs, sim):
    """计算峰值流量误差 (Peak Flow Error)"""
    # 去除缺失值
    idx = ~(np.isnan(obs) | np.isnan(sim))
    obs = obs[idx]
    sim = sim[idx]
    
    if len(obs) == 0:
        return np.nan
    
    # 计算峰值流量误差 (PFE) = (峰值预测 - 峰值观测) / 峰值观测
    peak_obs = np.max(obs)
    peak_sim = np.max(sim)
    
    if peak_obs == 0:
        return np.nan
    
    return (peak_sim - peak_obs) / peak_obs * 100  # 转为百分比

def evaluate_nc_files(directory):
    """评估目录中的NC文件并计算指标"""
    # 获取目录中的所有NC文件
    directory = Path(directory)
    nc_files = list(directory.glob("*.nc"))
    
    results = []
    
    for file_path in nc_files:
        try:
            # 打开NC文件
            ds = xr.open_dataset(file_path)
            
            # 检查是否包含所需变量
            if 'streamflow_obs' not in ds or 'streamflow_pred_xaj' not in ds:
                print(f"文件 {file_path.name} 缺少必要的变量")
                continue
                
            # 获取数据
            obs = ds.streamflow_obs.values
            pred = ds.streamflow_pred_xaj.values
            
            # 计算指标
            nse = nash_sutcliffe(obs, pred)
            pfe = peak_flow_error(obs, pred)
            
            # 获取流域信息（如果有）
            basin_id = file_path.stem
            if 'basin' in ds.dims:
                basin_id = str(ds.basin.values)
            
            # 添加结果
            results.append({
                'file': file_path.name,
                'basin_id': basin_id,
                'nse': nse,
                'pfe': pfe
            })
            
            ds.close()
            
        except Exception as e:
            print(f"处理文件 {file_path.name} 时出错: {e}")
    
    return results

def main():
    # 设置数据目录
    data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_new"
    
    # 评估文件
    results = evaluate_nc_files(data_dir)
    
    # 创建DataFrame并保存为CSV
    if results:
        df = pd.DataFrame(results)
        output_path = os.path.join(data_dir, "evaluation_results.csv")
        df.to_csv(output_path, index=False)
        print(f"评估结果已保存至: {output_path}")
    else:
        print("没有找到有效的评估结果")

if __name__ == "__main__":
    main()