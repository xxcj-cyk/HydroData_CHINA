"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 00:10:23
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-11 11:20:07
"""

import os
import glob
import pandas as pd
import re
import xarray as xr


def list_nc_files(directory_path):
    """
    Read all nc files in specified directory and record their names (without extension)
    
    Parameters:
        directory_path: Directory path containing nc files
        
    Returns:
        nc_files: List containing all nc file names (without extension)
    """
    # Ensure path exists
    if not os.path.exists(directory_path):
        raise FileNotFoundError(f"Directory {directory_path} does not exist")
    # Use glob to get all nc files
    nc_files_pattern = os.path.join(directory_path, "*.nc")
    nc_files = glob.glob(nc_files_pattern)
    # Extract filenames (without path and extension)
    nc_filenames = [os.path.splitext(os.path.basename(file))[0] for file in nc_files]
    print(f"Found {len(nc_filenames)} nc files in {directory_path}")
    return nc_filenames


def extract_basin_id_from_filename(filename):
    """
    Extract basin ID from filename
    For example: extract anhui_62935423 from Anhui_62935423_2017092321
    
    Parameters:
        filename: nc filename (without extension)
        
    Returns:
        basin_id: Basin ID in lowercase form
    """
    # Use regex to extract basin ID
    match = re.match(r'(?i)(Anhui_\d+)_\d+', filename)
    if match:
        # Convert to lowercase to match format in attributes.csv
        return match.group(1).lower()
    return None


def match_attributes_to_basins(nc_filenames, attributes_file):
    """
    Match basin filenames with attribute data
    
    Parameters:
        nc_filenames: List of nc filenames
        attributes_file: Path to CSV file containing basin attributes
        
    Returns:
        matched_data: DataFrame containing filenames and their corresponding attributes
    """
    # Read attribute data
    attributes_df = pd.read_csv(attributes_file)
    print(f"Successfully read attribute file, contains {len(attributes_df)} records")
    # Create result list
    result_data = []
    # Match each filename with attributes
    for filename in nc_filenames:
        basin_id = extract_basin_id_from_filename(filename)
        if basin_id:
            # Find matching basin ID in attribute data
            basin_attributes = attributes_df[attributes_df['basin_id'] == basin_id]
            if not basin_attributes.empty:
                # Create a row of data, including filename and attributes
                row_data = {'basin': filename}
                for col in attributes_df.columns:
                    if col != 'basin_id':
                        row_data[col] = basin_attributes[col].values[0]
                result_data.append(row_data)
            else:
                print(f"Warning: Unable to find attributes matching {basin_id}")
        else:
            print(f"Warning: Unable to extract basin ID from {filename}")
    # Create result DataFrame
    if result_data:
        return pd.DataFrame(result_data)
    else:
        print("Warning: No data matched")
        return None


def create_basin_attributes_nc(matched_data, output_nc_file):
    """
    Save basin attribute data as NetCDF format
    
    Parameters:
        matched_data: DataFrame containing basin filenames and attributes
        output_nc_file: Output NetCDF file path
    """
    if matched_data is None or matched_data.empty:
        print("Error: No data available to save")
        return False
    # Create dataset
    ds = xr.Dataset()
    # Add basic coordinates
    basins = matched_data['basin'].values
    ds.coords['basin'] = basins
    # Add attribute variables
    for col in matched_data.columns:
        if col != 'basin':
            # Ensure data is numeric
            values = matched_data[col].astype(float).values
            ds[col] = xr.DataArray(values, coords=[ds.basin], dims=['basin'])
            # Add attribute description
            ds[col].attrs['long_name'] = col
            ds[col].attrs['units'] = '-'  # Can be modified based on actual situation
    # Add global attributes
    ds.attrs['title'] = 'Anhui Basin Attributes'
    ds.attrs['description'] = 'Static attributes for Anhui basins'
    ds.attrs['created_by'] = 'Yikai CHAI'
    ds.attrs['creation_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
    # Save as NetCDF file
    ds.to_netcdf(output_nc_file)
    print(f"Basin attribute data successfully saved as NetCDF file: {output_nc_file}")
    return True


if __name__ == "__main__":
    # Set directory paths
    nc_directory_path = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    attributes_file = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Attribute_21\attributes.csv"
    # Get nc file list (without extension)
    nc_files = list_nc_files(nc_directory_path)
    # Match files with attributes
    matched_data = match_attributes_to_basins(nc_files, attributes_file)
    if matched_data is not None:
        # Output result preview
        print("\nMatch result preview:")
        print(matched_data.head())
<<<<<<< HEAD:hydrodata_china/datasets/anhui/step1_anhui_attributes.py
        # Save result to NetCDF file
        output_nc = os.path.join(os.path.dirname(nc_directory_path), "attributes.nc")
=======
        # 保存结果到NetCDF文件
        output_nc = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H\attributes.nc"
>>>>>>> d5c8209abb8225c3386d1982a1cd0152a073a0d5:hydrodata_china/datasets/anhui_old/step7_anhui_attributes.py
        create_basin_attributes_nc(matched_data, output_nc)
    else:
        print("Failed to generate match results")

