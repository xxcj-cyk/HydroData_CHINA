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
    Calculate Nash-Sutcliffe Efficiency coefficient (NSE)
    Parameters:
        obs: Array of observed values
        sim: Array of simulated values
    Returns:
        NSE metric, values closer to 1 indicate better simulation performance
    """
    obs, sim = np.array(obs), np.array(sim)
    denominator = np.sum((obs - np.mean(obs)) ** 2)
    if denominator == 0:
        return -np.inf  # Avoid division by zero
    return 1 - np.sum((obs - sim) ** 2) / denominator

def kge(obs, sim):
    """
    Calculate Kling-Gupta Efficiency coefficient (KGE)
    Parameters:
        obs: Array of observed values
        sim: Array of simulated values
    Returns:
        KGE metric, which integrates correlation, variability, and mean bias
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
    Calculate Pearson Correlation Coefficient (Corr)
    Parameters:
        obs: Array of observed values
        sim: Array of simulated values
    Returns:
        Correlation coefficient, measuring the degree of linear correlation
    """
    obs, sim = np.array(obs), np.array(sim)
    if np.std(obs) == 0 or np.std(sim) == 0:
        return 0  # Constant sequences have no correlation
    return np.corrcoef(obs, sim)[0, 1]

def rmse(obs, sim):
    """
    Calculate Root Mean Square Error (RMSE)
    Parameters:
        obs: Array of observed values
        sim: Array of simulated values
    Returns:
        RMSE, measuring the deviation between simulated and observed values
    """
    obs = np.array(obs)
    sim = np.array(sim)
    return np.sqrt(np.mean((obs - sim) ** 2))

def pfe(obs, sim):
    """
    Calculate Peak Flow Error (PFE)
    Parameters:
        obs: Array of observed values
        sim: Array of simulated values
    Returns:
        Relative error of peak flow (%)
    """
    peak_obs = np.max(obs)
    peak_sim = np.max(sim)
    if peak_obs == 0:
        return np.inf if peak_sim != 0 else 0  # Avoid division by zero
    return (peak_sim - peak_obs) / peak_obs * 100

def peak_time_error(obs, sim):
    """
    Calculate Peak Time Error (PTE)
    Parameters:
        obs: Array of observed values
        sim: Array of simulated values
    Returns:
        Index difference of peak time occurrence (simulated - observed)
    """
    obs = np.array(obs)
    sim = np.array(sim)
    peak_time_obs = np.argmax(obs)
    peak_time_sim = np.argmax(sim)
    return peak_time_sim - peak_time_obs