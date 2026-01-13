"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-05-29 00:10:23
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 20:58:08
"""

import pandas as pd
import xarray as xr

# File paths
FLOOD_EVENT_XLSX = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Flood_Event_21\FloodEvent16_612.xlsx"
ATTRIBUTES_CSV = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\Attributes_21\attributes.csv"
OUTPUT_NC = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui16_1H\attributes.nc"
OUTPUT_CSV = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui16_1H\attributes.csv"

# Read flood events
flood_df = pd.read_excel(FLOOD_EVENT_XLSX)
flood_events = flood_df['FloodEvent_612'].dropna().astype(str)

# Read basin attributes
attributes_df = pd.read_csv(ATTRIBUTES_CSV)

# Build basin_code to attribute mapping
basin_attr_map = {}
for _, row in attributes_df.iterrows():
    basin_id = row['basin_id']
    basin_code = basin_id.replace('anhui_', '')
    # 字段名替换
    row = row.rename({'area': 'Area', 'pre_mm_syr': 'p_mean'})
    basin_attr_map[basin_code] = row.drop('basin_id')

# Assign attributes to each flood event
records = []
for event in flood_events:
    basin_code = event.split('_')[0]
    if basin_code in basin_attr_map:
        record = {'FloodEvent_612': event}
        record.update(basin_attr_map[basin_code].to_dict())
        records.append(record)
    else:
        print(f"Warning: No attributes found for {event}")

result_df = pd.DataFrame(records)
result_df.rename(columns={'FloodEvent_612': 'basin'}, inplace=True)
result_df['basin'] = result_df['basin'].apply(lambda x: 'Anhui_' + x)

# basin列移到第一列
cols = ['basin'] + [col for col in result_df.columns if col != 'basin']
result_df = result_df[cols]

# Save as CSV
result_df.to_csv(OUTPUT_CSV, index=False)
print(f"CSV saved: {OUTPUT_CSV}")

# Save as NetCDF
ds = xr.Dataset()
ds.coords['basin'] = result_df['basin'].values
for col in result_df.columns:
    if col != 'basin':
        values = pd.to_numeric(result_df[col], errors='coerce').values
        ds[col] = xr.DataArray(values, coords=[ds['basin']], dims=['basin'])
        ds[col].attrs['long_name'] = col
        ds[col].attrs['units'] = '-'
ds.attrs['title'] = 'Anhui FloodEvent Attributes'
ds.attrs['description'] = '197 attributes for each FloodEvent'
ds.attrs['created_by'] = 'Yikai CHAI'
ds.to_netcdf(OUTPUT_NC)
print(f"NetCDF saved: {OUTPUT_NC}")

