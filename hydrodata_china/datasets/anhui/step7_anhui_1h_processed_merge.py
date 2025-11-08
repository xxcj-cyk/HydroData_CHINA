"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 17:31:00
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-19 11:18:44
"""

import os
import glob
import re
import xarray as xr
import logging
from collections import defaultdict
import warnings


warnings.filterwarnings("ignore", message="invalid value encountered in cast")


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def extract_basin_id(filename):
    """
    Extract basin ID from filename
    
    Parameters:
        filename: Filename
        
    Returns:
        basin_id: Basin ID
    """
    # Extract basin ID from filename (assuming filename format is Anhui_XXXXXXXX_YYYYMMDD.nc)
    match = re.search(r'Anhui_([0-9]+)_', os.path.basename(filename))
    if match:
        return match.group(1)
    return None


def reorder_dataset(ds):
    """
    Reorder Dataset dimensions and data variables, and convert time_true data type from datetime to string
    
    Parameters:
        ds: Input xarray.Dataset
        
    Returns:
        reordered_ds: Reordered xarray.Dataset with time_true as string type
    """
    # Define desired variable order
    desired_var_order = [
        'P_Anhui',
        'P_50406910',
        'P_50436450',
        'P_50436550',
        'P_50436650',
        'P_ERA5-Land',
        'PET_Anhui',
        'PET_ERA5-Land',
        'ET_ERA5-Land',
        'T_ERA5-Land',
        'streamflow_obs',
        'streamflow_pred_xaj',
        'streamflow',     
        'time_true'
    ]
    # Create new Dataset, preserving original coordinates
    reordered_ds = xr.Dataset(coords={'basin': ds.basin, 'time': ds.time})
    # Add variables in desired order
    for var_name in desired_var_order:
        if var_name in ds:
            if var_name == 'time_true':
                # Convert time_true to string type
                # First convert datetime to string, then create as new variable
                time_str_values = ds[var_name].dt.strftime('%Y-%m-%d %H:%M:%S').values
                reordered_ds[var_name] = xr.DataArray(
                    time_str_values,
                    dims=ds[var_name].dims,
                    coords=ds[var_name].coords
                )
                # Set attribute indicating this is string representation of time
                reordered_ds[var_name].attrs['description'] = 'String representation of datetime'
            else:
                reordered_ds[var_name] = ds[var_name]
    # Preserve original attributes
    reordered_ds.attrs.update(ds.attrs)
    return reordered_ds


def merge_nc_files_by_basin(input_folder, output_folder):
    """
    Merge nc files by basin ID
    
    Parameters:
        input_folder: Path to input folder containing all nc files
        output_folder: Path to output folder for saving merged nc files
    """
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    # Get all nc files
    nc_files = glob.glob(os.path.join(input_folder, "*.nc"))
    if not nc_files:
        logging.error(f"Error: No nc files found in {input_folder}")
        return [], []
    logging.info(f"Found {len(nc_files)} nc files to process")
    # Group by basin ID
    basin_files = defaultdict(list)
    for file_path in nc_files:
        basin_id = extract_basin_id(file_path)
        if basin_id:
            basin_files[basin_id].append(file_path)
    logging.info(f"Found {len(basin_files)} different basins")
    # Record successful and failed basins
    success_basins = []
    failed_basins = []
    # Process files for each basin
    for basin_id, files in basin_files.items():
        logging.info(f"Processing basin {basin_id}, total {len(files)} files")
        # Read all nc files for this basin
        datasets = []
        for file_path in files:
            ds = xr.open_dataset(file_path)
            # Extract event ID as new dimension
            event_id = os.path.basename(file_path).split('.')[0]  # Remove .nc extension
            ds = ds.expand_dims({"basin": [event_id]})
            datasets.append(ds)
        if not datasets:
            logging.warning(f"Basin {basin_id} has no valid datasets, skipping")
            failed_basins.append(basin_id)
            continue
        # Merge datasets
        merged_ds = xr.concat(datasets, dim="basin")
        # Add basin ID as global attribute
        merged_ds.attrs["basin_id"] = basin_id
        merged_ds.attrs["flood_event_count"] = len(files)
        # Add variable unit information
        if 'streamflow' in merged_ds:
            merged_ds['streamflow'].attrs['units'] = 'mm/h'
        if 'streamflow_obs' in merged_ds:
            merged_ds['streamflow_obs'].attrs['units'] = 'm3/s'
        if 'streamflow_pred_xaj' in merged_ds:
            merged_ds['streamflow_pred_xaj'].attrs['units'] = 'm3/s'
        if 'P_Anhui' in merged_ds:
            merged_ds['P_Anhui'].attrs['units'] = 'mm/h'
        if 'evaporation' in merged_ds:
            merged_ds = merged_ds.rename({'evaporation': 'PET_Anhui'})
            merged_ds['PET_Anhui'].attrs['units'] = 'mm/h'
        if 'total_precipitation_hourly' in merged_ds:
            merged_ds = merged_ds.rename({'total_precipitation_hourly': 'P_ERA5-Land'})
            merged_ds['P_ERA5-Land'].attrs['units'] = 'mm/h'
        if 'potential_evaporation_hourly' in merged_ds:
            merged_ds = merged_ds.rename({'potential_evaporation_hourly': 'PET_ERA5-Land'})
            merged_ds['PET_ERA5-Land'].attrs['units'] = 'mm/h'
        if 'total_evaporation_hourly' in merged_ds:
            merged_ds = merged_ds.rename({'total_evaporation_hourly': 'ET_ERA5-Land'})
            merged_ds['ET_ERA5-Land'].attrs['units'] = 'mm/h'
        if 'temperature_2m' in merged_ds:
            merged_ds = merged_ds.rename({'temperature_2m': 'T_ERA5-Land'})
            merged_ds['T_ERA5-Land'].attrs['units'] = '°C'
        # Get first and last event IDs
        event_ids = [os.path.basename(file_path).split('.')[0] for file_path in files]
        if event_ids:
            # Sort event IDs by timestamp (assuming format is Anhui_XXXXXXXX_YYYYMMDD)
            event_ids.sort(key=lambda x: x.split('_')[-1] if len(x.split('_')) >= 3 else '')
            first_event_id = event_ids[0]
            last_event_id = event_ids[-1]
            # Create new filename format: timeseries_1h_batch_first_event_id_last_event_id
            output_filename = f"timeseries_1h_batch_{first_event_id}_{last_event_id}.nc"
        else:
            # If no valid event IDs, use basin ID as fallback
            output_filename = f"{basin_id}.nc"
        # Reorder dimensions and variables
        merged_ds = reorder_dataset(merged_ds)
        # Save merged nc file
        output_file = os.path.join(output_folder, output_filename)
        merged_ds.to_netcdf(output_file)
        logging.info(f"Merged {len(files)} files for basin {basin_id} into {output_file}")
        success_basins.append(basin_id)
    # Output processing summary
    logging.info(f"Number of successfully processed basins: {len(success_basins)}")
    if success_basins:
        logging.info(f"Successfully processed basin IDs: {', '.join(success_basins)}")
    logging.info(f"Number of failed basins: {len(failed_basins)}")
    if failed_basins:
        logging.info(f"Failed basin IDs: {', '.join(failed_basins)}")
    return success_basins, failed_basins    


if __name__ == "__main__":
    input_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H"
    # Execute merge operation
    success_basins, failed_basins = merge_nc_files_by_basin(input_folder, output_folder)
    logging.info("Processing completed!")