"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 10:52:13
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-19 11:18:48
"""

import os
import glob
import numpy as np
import xarray as xr
import warnings
import csv


warnings.filterwarnings("ignore", message="Converting non-nanosecond precision datetime")
warnings.filterwarnings("ignore", message="invalid value encountered in cast")


def identify_train_val_sets(folder_path, train_ratio=0.8, min_validation_samples=2):
    """
    Identify training and validation sets for each basin
    
    Divide data for each basin into training and validation sets according to the specified ratio.
    By default, time-ordered division is used, with earlier events used for training and newer events for validation.
    
    Parameters:
        folder_path (str): Path to folder containing original nc files
        train_ratio (float): Ratio of training set to total data (default 0.8, i.e., 4:1)
        min_validation_samples (int): Minimum number of samples in validation set (default 2)
    
    Returns:
        tuple: Contains two dictionaries:
            - train_sets (dict): Keys are basin IDs, values are lists of training event IDs
            - val_sets (dict): Keys are basin IDs, values are lists of validation event IDs
    """
    # Get all nc files
    nc_files = glob.glob(os.path.join(folder_path, "*.nc"))
    if not nc_files:
        print(f"Error: No nc files found in {folder_path}")
        return {}, {}
    # Extract basin information from filenames
    basin_files = {}
    for nc_file in nc_files:
        filename = os.path.basename(nc_file)
        # Extract basin ID from filename
        parts = filename.split('_')
        if len(parts) >= 2:
            # Extract basin ID (e.g., 50406910)
            basin_id = parts[1]
            # Verify if basin_id is a valid basin identifier
            if basin_id.isdigit():
                if basin_id not in basin_files:
                    basin_files[basin_id] = []
                basin_files[basin_id].append(nc_file)
            else:
                print(f"Warning: Basin ID {basin_id} extracted from filename {filename} is not a valid numeric identifier")
        else:
            print(f"Warning: Filename {filename} format does not meet expectations, cannot extract basin ID")
    # Print found basin information
    print(f"\nFound {len(basin_files)} basins:")
    for basin_id, files in basin_files.items():
        print(f"  - Basin {basin_id}: {len(files)} files")
    # Divide training and validation sets for each basin
    train_sets = {}
    val_sets = {}
    for basin_id, files in basin_files.items():
        if not files:
            continue 
        # Sort files by time (assuming filenames contain date information)
        files.sort(key=lambda x: os.path.basename(x).split('_')[2].split('.')[0])
        # Calculate training and validation set counts
        total_count = len(files)
        val_count = max(min_validation_samples, int(total_count * (1 - train_ratio)))
        train_count = total_count - val_count
        # Ensure there is enough data for division
        if train_count <= 0:
            print(f"Warning: Basin {basin_id} has insufficient events ({total_count}) for division, at least {min_validation_samples+1} events are needed")
            continue 
        # Divide training and validation sets (newer events are validation set)
        train_files = files[:train_count]
        val_files = files[train_count:]
        # Extract filenames (without path and extension) as event identifiers
        train_sets[basin_id] = [os.path.basename(f).split('.')[0] for f in train_files]
        val_sets[basin_id] = [os.path.basename(f).split('.')[0] for f in val_files]
        print(f"\nBasin {basin_id} division results:")
        print(f"  - Total events: {total_count}")
        print(f"  - Training set: {train_count} events")
        print(f"  - Validation set: {val_count} events")
    return train_sets, val_sets


def export_sets_to_csv(train_sets, val_sets, output_folder):
    """
    Export training and validation set IDs to CSV files
    
    Parameters:
        train_sets (dict): Training set dictionary, keys are basin IDs, values are lists of training event IDs
        val_sets (dict): Validation set dictionary, keys are basin IDs, values are lists of validation event IDs
        output_folder (str): Folder path for output CSV files
    
    Returns:
        tuple: Contains two strings, paths to training and validation set CSV files
    """
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    
    # Define output file paths
    train_csv_path = os.path.join(output_folder, "train_sets.csv")
    val_csv_path = os.path.join(output_folder, "validation_sets.csv")
    
    # Export training set IDs to CSV
    with open(train_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['basin'])  # Write header
        for basin_id, events in train_sets.items():
            for event_id in events:
                # Check if event_id already contains Anhui_ prefix
                if event_id.startswith(f"Anhui_{basin_id}"):
                    formatted_id = event_id
                else:
                    # Format as Anhui_basin_id_event_id
                    formatted_id = f"Anhui_{basin_id}_{event_id}"
                writer.writerow([formatted_id])
    
    # Export validation set IDs to CSV
    with open(val_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['basin'])  # Write header
        for basin_id, events in val_sets.items():
            for event_id in events:
                # Check if event_id already contains Anhui_ prefix
                if event_id.startswith(f"Anhui_{basin_id}"):
                    formatted_id = event_id
                else:
                    # Format as Anhui_basin_id_event_id
                    formatted_id = f"Anhui_{basin_id}_{event_id}"
                writer.writerow([formatted_id])
    
    print(f"\nTraining set IDs exported to: {train_csv_path}")
    print(f"Validation set IDs exported to: {val_csv_path}")
    
    return train_csv_path, val_csv_path


def process_nc_files(input_folder, output_folder):
    """
    Process NC files: Copy original time dimension content to time_true variable, and uniformly output 744 time steps
    Also supplement training set with August data and validation set with July data to form complete July-August dataset
    
    Parameters:
        input_folder (str): Path to folder containing original nc files
        output_folder (str): Path to folder for output nc files
    
    Returns:
        None
    """
    # Ensure output folder exists
    os.makedirs(output_folder, exist_ok=True)
    # Get all nc files
    nc_files = glob.glob(os.path.join(input_folder, "*.nc"))
    if not nc_files:
        print(f"Error: No nc files found in {input_folder}")
        return
    print(f"Found {len(nc_files)} nc files to process")
    processed_count = 0
    error_count = 0
    error_files = []  # Record files that failed to process
    # Get training and validation sets
    train_sets, val_sets = identify_train_val_sets(input_folder)
    # Merge all training and validation set events from all basins into two lists
    all_train_events = []
    all_val_events = []
    for basin_id in train_sets:
        all_train_events.extend(train_sets[basin_id])
    for basin_id in val_sets:
        all_val_events.extend(val_sets[basin_id])
    for nc_file in nc_files:
        filename = os.path.basename(nc_file)
        event_id = filename.split('.')[0]  # Get filename without extension as event ID
        # Read original nc file
        ds = xr.open_dataset(nc_file)
        # Get original time values and data
        original_times = ds.time.values
        original_length = len(original_times)
        # Save original time to time_true variable
        ds['time_true'] = xr.Variable('time', original_times)
        # Uniform time step length to 744
        target_length = 744
        # Check if time dimensions of all variables are consistent
        time_dims = {}
        for var_name, var in ds.data_vars.items():
            if 'time' in var.dims:
                var_time_len = var.sizes['time']
                if var_time_len not in time_dims:
                    time_dims[var_time_len] = []
                time_dims[var_time_len].append(var_name)
        # If there are multiple different time dimension lengths, unify them first
        if len(time_dims) > 1:
            print(f"  ⚠ Found multiple different time dimension lengths: {time_dims}")
            # Find the shortest time dimension length
            min_time_len = min(time_dims.keys())
            # Truncate all variables to the shortest length
            for var_name in ds.data_vars:
                if 'time' in ds[var_name].dims and ds[var_name].sizes['time'] > min_time_len:
                    ds[var_name] = ds[var_name].isel(time=slice(0, min_time_len))
            # Update time dimension
            ds = ds.isel(time=slice(0, min_time_len))
            # Update original length
            original_length = min_time_len
            original_times = ds.time.values
            print(f"  ℹ Truncated all variables to the same time length: {min_time_len}")
        if original_length > target_length:
            # If original time step length is greater than target length, truncate last 744 time steps from the end
            start_idx = original_length - target_length
            ds = ds.isel(time=slice(start_idx, original_length))
        elif original_length < target_length:
            # If original time step length is less than target length, padding is needed
            # Calculate number of time steps to pad
            padding_length = target_length - original_length
            print(f"  ℹ Original time steps {original_length} is less than target time steps {target_length}, need to pad {padding_length} time steps")
            # Create padding data
            # Assume time interval is 1 hour
            if len(original_times) > 0:
                # Check time interval
                if original_length > 1:
                    # Ensure time is datetime64 type
                    if isinstance(original_times[0], str):
                        original_times = np.array([np.datetime64(t) for t in original_times])
                    # Calculate average time interval
                    time_diffs = np.diff(original_times)
                    avg_time_diff = np.mean(time_diffs)
                    time_diff = avg_time_diff
                else:
                    # If there is only one time point, default to 1 hour interval
                    time_diff = np.timedelta64(1, 'h')
                # Create new time array (prepend to original time)
                new_times = np.array([original_times[0] - (i+1) * time_diff for i in range(padding_length)])
                new_times = np.flip(new_times)  # Reverse array to keep chronological order
                padded_times = np.concatenate([new_times, original_times])
                # Create padding data for all variables (use values from first time step, not NaN)
                padded_ds = xr.Dataset()
                padded_ds['time'] = ('time', padded_times)
                # Create padding data for each data variable
                for var_name, var in ds.data_vars.items():
                    if 'time' in var.dims:
                        # Get variable dimensions
                        dims = var.dims
                        shape = list(var.shape)
                        time_dim_idx = dims.index('time')
                        # Create padding array
                        pad_shape = shape.copy()
                        pad_shape[time_dim_idx] = padding_length
                        # Use values from first time step for padding, not NaN
                        if np.issubdtype(var.dtype, np.number):
                            # Get values from first time step
                            first_values = var.isel(time=0).values
                            # Create padding array, fill with values from first time step
                            pad_data = np.zeros(pad_shape, dtype=var.dtype)
                            # For multi-dimensional arrays, need to repeat values from first time step in time dimension
                            if len(pad_shape) > 1:
                                # Copy values from first time step for each time point
                                for i in range(padding_length):
                                    # Select correct indexing method to set values
                                    idx = [slice(None)] * len(dims)
                                    idx[time_dim_idx] = i
                                    pad_data[tuple(idx)] = first_values
                            else:
                                # For one-dimensional arrays, directly fill with first value
                                pad_data.fill(var.values[0])
                        else:
                            # Non-numeric variables, try to use first value
                            first_value = var.isel(time=0).values
                            pad_data = np.full(pad_shape, first_value, dtype=var.dtype)
                        # Merge original data and padding data
                        padded_data = np.concatenate([pad_data, var.values], axis=time_dim_idx)
                        padded_ds[var_name] = (dims, padded_data)
                    else:
                        # For variables that don't depend on time dimension, directly copy
                        padded_ds[var_name] = var
                # Replace original dataset
                ds = padded_ds
                print(f"  ℹ Successfully padded time steps, new time step count: {len(ds.time)}, padding values used values from first time step")
            else:
                raise ValueError("Original time array is empty, cannot perform padding")
        # Set different time ranges based on whether event is in training or validation set
        if event_id in all_train_events:
            # Training set time range: 2024-07-01 00:00:00 ~ 2024-07-31 23:00:00
            start_time = np.datetime64('2024-07-01T00:00:00')
            # Calculate hourly time points
            new_times = np.array([start_time + np.timedelta64(i, 'h') for i in range(target_length)])
            # Don't overwrite time_true, only modify time
            ds['time'] = xr.Variable('time', new_times)
            # Delete this line: ds['time_true'] = xr.Variable('time', new_times)
            # Supplement training set with August data
            aug_start_time = np.datetime64('2024-08-01T00:00:00')
            aug_hours = 31 * 24  # Total hours in August
            aug_times = np.array([aug_start_time + np.timedelta64(i, 'h') for i in range(aug_hours)])
            # Create extended dataset
            aug_ds = xr.Dataset()
            aug_ds['time'] = ('time', aug_times)
            # Set time_true of extended dataset to August time variable
            aug_ds['time_true'] = ('time', aug_times)
            # Get streamflow value at 2024-07-31 23:00:00
            last_july_time = np.datetime64('2024-07-31T23:00:00')
            last_july_idx = np.where(new_times == last_july_time)[0][0]
            # Create extended data for each variable
            for var_name, var in ds.data_vars.items():
                if 'time' in var.dims:
                    # Get variable dimensions
                    dims = var.dims
                    shape = list(var.shape)
                    time_dim_idx = dims.index('time') 
                    # Create extended array
                    aug_shape = shape.copy()
                    aug_shape[time_dim_idx] = aug_hours
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step6_anhui_1h_processed.py
                    # For streamflow variables, use value from 2024-07-31 23:00:00
                    if var_name == 'streamflow' or var_name == 'streamflow_obs':
                        # Get value from 2024-07-31 23:00:00
                        last_july_value = var.isel(time=last_july_idx).values
                        # Create padding array, fill with value from 2024-07-31 23:00:00
                        aug_data = np.zeros(aug_shape, dtype=var.dtype)
                        # For multi-dimensional arrays, need to repeat this value in time dimension
                        if len(aug_shape) > 1:
                            for i in range(aug_hours):
                                idx = [slice(None)] * len(dims)
                                idx[time_dim_idx] = i
                                aug_data[tuple(idx)] = last_july_value
                        else:
                            # For one-dimensional arrays, directly fill with this value
                            aug_data.fill(last_july_value)
                    elif var_name == 'P_Anhui' or var_name in ['evaporation', 'temperature_2m', 'potential_evaporation_hourly', 'total_evaporation_hourly', 'total_precipitation_hourly']:
                        # P_Anhui variable set to 0
                        aug_data = np.zeros(aug_shape, dtype=np.float64)
                    else:
                        # Other variables set to NaN
                        aug_data = np.full(aug_shape, np.nan, dtype=var.dtype)
=======
                    aug_data = np.full(aug_shape, np.nan, dtype=var.dtype)
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step5_anhui_1h_processed.py
                    aug_ds[var_name] = (dims, aug_data)
                else:
                    # For variables that don't depend on time dimension, directly copy
                    aug_ds[var_name] = var
        elif event_id in all_val_events:
            # Validation set time range: 2024-08-01 00:00:00 ~ 2024-08-31 23:00:00
            start_time = np.datetime64('2024-08-01T00:00:00')
            # Calculate hourly time points
            new_times = np.array([start_time + np.timedelta64(i, 'h') for i in range(target_length)])
            # Don't overwrite time_true, only modify time
            ds['time'] = xr.Variable('time', new_times)
            # Supplement validation set with July data
            jul_start_time = np.datetime64('2024-07-01T00:00:00')
            jul_hours = 31 * 24  # Total hours in July
            jul_times = np.array([jul_start_time + np.timedelta64(i, 'h') for i in range(jul_hours)])
            # Create extended dataset
            jul_ds = xr.Dataset()
            jul_ds['time'] = ('time', jul_times)
            # Set time_true of extended dataset to July time variable
            jul_ds['time_true'] = ('time', jul_times)
            # Get streamflow value at 2024-08-01 00:00:00
            first_aug_time = np.datetime64('2024-08-01T00:00:00')
            first_aug_idx = np.where(new_times == first_aug_time)[0][0]
            # Create extended data for each variable
            for var_name, var in ds.data_vars.items():
                if 'time' in var.dims:
                    # Get variable dimensions
                    dims = var.dims
                    shape = list(var.shape)
                    time_dim_idx = dims.index('time')
                    # Create extended array
                    jul_shape = shape.copy()
                    jul_shape[time_dim_idx] = jul_hours
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step6_anhui_1h_processed.py
                    # For streamflow variables, use value from 2024-08-01 00:00:00
                    if var_name == 'streamflow' or var_name == 'streamflow_obs':
                        # Get value from 2024-08-01 00:00:00
                        first_aug_value = var.isel(time=first_aug_idx).values
                        # Create padding array, fill with value from 2024-08-01 00:00:00
                        jul_data = np.zeros(jul_shape, dtype=var.dtype)
                        # For multi-dimensional arrays, need to repeat this value in time dimension
                        if len(jul_shape) > 1:
                            for i in range(jul_hours):
                                idx = [slice(None)] * len(dims)
                                idx[time_dim_idx] = i
                                jul_data[tuple(idx)] = first_aug_value
                        else:
                            # For one-dimensional arrays, directly fill with this value
                            jul_data.fill(first_aug_value)
                    elif var_name == 'P_Anhui' or var_name in ['evaporation', 'temperature_2m', 'potential_evaporation_hourly', 'total_evaporation_hourly', 'total_precipitation_hourly']:
                        # These variables all set to 0
                        jul_data = np.zeros(jul_shape, dtype=np.float64)
                    else:
                        # Other variables set to NaN
                        jul_data = np.full(jul_shape, np.nan, dtype=var.dtype)
=======
                    jul_data = np.full(jul_shape, np.nan, dtype=var.dtype)
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step5_anhui_1h_processed.py
                    jul_ds[var_name] = (dims, jul_data)
                else:
                    # For variables that don't depend on time dimension, directly copy
                    jul_ds[var_name] = var 
        else:
            # If not in training or validation set, keep original time
            new_times = ds.time.values
            # Set time_true to original time
            ds['time_true'] = xr.Variable('time', new_times)
            print(f"  ℹ Event {event_id} is not in training or validation set, keeping original time")
        # Ensure time dimension lengths of all variables are consistent
        for var_name, var in list(ds.data_vars.items()):
            if 'time' in var.dims and var.sizes['time'] != target_length:
                print(f"  ⚠ Variable {var_name} time dimension length ({var.sizes['time']}) does not match target length ({target_length}), will adjust")
                if var.sizes['time'] < target_length:
                    # If variable time dimension is less than target length, need to pad
                    padding_length = target_length - var.sizes['time']
                    # Get variable dimensions
                    dims = var.dims
                    shape = list(var.shape)
                    time_dim_idx = dims.index('time')
                    # Create padding array
                    pad_shape = shape.copy()
                    pad_shape[time_dim_idx] = padding_length
                    # Use values from first time step for padding
                    if np.issubdtype(var.dtype, np.number):
                        # Get values from first time step
                        first_values = var.isel(time=0).values
                        # Create padding array
                        pad_data = np.zeros(pad_shape, dtype=var.dtype)
                        # For multi-dimensional arrays, need to repeat values from first time step in time dimension
                        if len(pad_shape) > 1:
                            # Copy values from first time step for each time point
                            for i in range(padding_length):
                                # Select correct indexing method to set values
                                idx = [slice(None)] * len(dims)
                                idx[time_dim_idx] = i
                                pad_data[tuple(idx)] = first_values
                        else:
                            # For one-dimensional arrays, directly fill with first value
                            pad_data.fill(var.values[0])
                    else:
                        # Non-numeric variables, try to use first value
                        first_value = var.isel(time=0).values
                        pad_data = np.full(pad_shape, first_value, dtype=var.dtype)
                    # Merge original data and padding data
                    padded_data = np.concatenate([pad_data, var.values], axis=time_dim_idx)
                    ds[var_name] = (dims, padded_data)
                else:
                    # If variable time dimension is greater than target length, truncate from the end
                    start_idx = var.sizes['time'] - target_length
                    ds[var_name] = var.isel(time=slice(start_idx, var.sizes['time']))
        # Update time dimension
        ds['time'] = ('time', new_times)
        # Merge extended dataset (if exists)
        if event_id in all_train_events and 'aug_ds' in locals():
            # Merge training set and August extended data
            ds = xr.concat([ds, aug_ds], dim='time')
        elif event_id in all_val_events and 'jul_ds' in locals():
            # Merge validation set and July extended data
            ds = xr.concat([jul_ds, ds], dim='time')
        # Save modified file
        output_file = os.path.join(output_folder, filename)
        ds.to_netcdf(output_file)
        processed_count += 1
    print("\nProcessing statistics:")
    print(f"  - Successfully processed: {processed_count} files")
    print(f"  - Failed to process: {error_count} files")
    print(f"  - Total files: {len(nc_files)} files")
    # If there are files that failed to process, print detailed information
    if error_files:
        print("\nFiles that failed to process:")
        for i, error_file in enumerate(error_files):
            print(f"  {i+1}. {error_file['filename']}: {error_file['error']}")


if __name__ == "__main__":
    # Specify folder paths
    data_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood"
    # 1. Run division function
    print("=" * 50)
    print("Starting to divide training and validation sets...")
    print("=" * 50)
    train_sets, val_sets = identify_train_val_sets(data_folder)
    # Print overall statistics
    print("\n" + "=" * 20 + " Overall Statistics " + "=" * 20)
    print(f"Total basins: {len(train_sets)}")
    print(f"Total training events: {sum(len(samples) for samples in train_sets.values())}")
    print(f"Total validation events: {sum(len(samples) for samples in val_sets.values())}")
    # 2. Export training and validation set IDs to CSV
    print("\n" + "=" * 50)
    print("Exporting training and validation set IDs to CSV files...")
    print("=" * 50)
    export_sets_to_csv(train_sets, val_sets, output_folder)
    
    # 3. Process files
    print("\n" + "=" * 50)
    print("Starting to process NC files...")
    print("=" * 50)
    process_nc_files(data_folder, output_folder)