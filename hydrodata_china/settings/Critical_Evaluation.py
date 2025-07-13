"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-22 09:59:12
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-22 10:28:34
"""

import numpy as np

def nse(obs, sim):
    """
    计算 Nash-Sutcliffe 效率系数（Nash-Sutcliffe Efficiency, NSE）
    参数:
        obs: 观测值数组
        sim: 模拟值数组
    返回:
        NSE 指标，越接近 1 表示模拟效果越好
    """
    obs, sim = np.array(obs), np.array(sim)
    denominator = np.sum((obs - np.mean(obs)) ** 2)
    if denominator == 0:
        return -np.inf  # 避免除以零
    return 1 - np.sum((obs - sim) ** 2) / denominator

def kge(obs, sim):
    """
    计算 Kling-Gupta 效率系数（Kling-Gupta Efficiency, KGE）
    参数:
        obs: 观测值数组
        sim: 模拟值数组
    返回:
        KGE 指标，综合相关性、变异性和均值偏差
    """
    obs, sim = np.array(obs), np.array(sim)
    if np.std(obs) == 0 or np.mean(obs) == 0:
        return -np.inf
    r = np.corrcoef(obs, sim)[0, 1]
    if np.isnan(r):
        r = 0
    alpha = np.std(sim) / np.std(obs)
    beta = np.mean(sim) / np.mean(obs)
    return 1 - np.sqrt((r - 1) ** 2 + (alpha - 1) ** 2 + (beta - 1) ** 2)

def corr(obs, sim):
    """
    计算皮尔逊相关系数（Pearson Correlation Coefficient, Corr）
    参数:
        obs: 观测值数组
        sim: 模拟值数组
    返回:
        相关系数，衡量线性相关程度
    """
    obs, sim = np.array(obs), np.array(sim)
    if np.std(obs) == 0 or np.std(sim) == 0:
        return 0  # 常数序列无相关性
    return np.corrcoef(obs, sim)[0, 1]

def rmse(obs, sim):
    """
    计算均方根误差（Root Mean Square Error, RMSE）
    参数:
        obs: 观测值数组
        sim: 模拟值数组
    返回:
        RMSE，衡量模拟值与观测值的偏差
    """
    obs = np.array(obs)
    sim = np.array(sim)
    return np.sqrt(np.mean((obs - sim) ** 2))

def pfe(obs, sim):
    """
    计算洪峰流量误差（Peak Flow Error, PFE）
    参数:
        obs: 观测值数组
        sim: 模拟值数组
    返回:
        洪峰流量相对误差（%）
    """
    peak_obs = np.max(obs)
    peak_sim = np.max(sim)
    if peak_obs == 0:
        return np.inf if peak_sim != 0 else 0  # 避免除以零
    return (peak_sim - peak_obs) / peak_obs * 100

def peak_time_error(obs, sim):
    """
    计算峰值时间误差（Peak Time Error, PTE）
    参数:
        obs: 观测值数组
        sim: 模拟值数组
    返回:
        峰值出现时间的索引差（模拟-观测）
    """
    obs = np.array(obs)
    sim = np.array(sim)
    peak_time_obs = np.argmax(obs)
    peak_time_sim = np.argmax(sim)
    return peak_time_sim - peak_time_obs