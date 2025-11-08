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
    """Calculate Nash-Sutcliffe Efficiency coefficient"""
    # Remove missing values
    idx = ~(np.isnan(obs) | np.isnan(sim))
    obs = obs[idx]
    sim = sim[idx]
    
    # Calculate NSE
    if len(obs) == 0:
        return np.nan
    return 1 - np.sum((obs - sim) ** 2) / np.sum((obs - np.mean(obs)) ** 2)

def peak_flow_error(obs, sim):
    """Calculate Peak Flow Error"""
    # Remove missing values
    idx = ~(np.isnan(obs) | np.isnan(sim))
    obs = obs[idx]
    sim = sim[idx]
    
    if len(obs) == 0:
        return np.nan
    
    # Calculate Peak Flow Error (PFE) = (peak_predicted - peak_observed) / peak_observed
    peak_obs = np.max(obs)
    peak_sim = np.max(sim)
    
    if peak_obs == 0:
        return np.nan
    
    return (peak_sim - peak_obs) / peak_obs * 100  # Convert to percentage

def evaluate_nc_files(directory):
    """Evaluate NC files in directory and calculate metrics"""
    # Get all NC files in directory
    directory = Path(directory)
    nc_files = list(directory.glob("*.nc"))
    
    results = []
    
    for file_path in nc_files:
        try:
            # Open NC file
            ds = xr.open_dataset(file_path)
            
            # Check if required variables are present
            if 'streamflow_obs' not in ds or 'streamflow_pred_xaj' not in ds:
                print(f"File {file_path.name} is missing required variables")
                continue
                
            # Get data
            obs = ds.streamflow_obs.values
            pred = ds.streamflow_pred_xaj.values
            
            # Calculate metrics
            nse = nash_sutcliffe(obs, pred)
            pfe = peak_flow_error(obs, pred)
            
            # Get basin information (if available)
            basin_id = file_path.stem
            if 'basin' in ds.dims:
                basin_id = str(ds.basin.values)
            
            # Add results
            results.append({
                'file': file_path.name,
                'basin_id': basin_id,
                'nse': nse,
                'pfe': pfe
            })
            
            ds.close()
            
        except Exception as e:
            print(f"Error processing file {file_path.name}: {e}")
    
    return results

def main():
    # Set data directory
    data_dir = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_new"
    
    # Evaluate files
    results = evaluate_nc_files(data_dir)
    
    # Create DataFrame and save as CSV
    if results:
        df = pd.DataFrame(results)
        output_path = os.path.join(data_dir, "evaluation_results.csv")
        df.to_csv(output_path, index=False)
        print(f"Evaluation results saved to: {output_path}")
    else:
        print("No valid evaluation results found")

if __name__ == "__main__":
    main()