"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-04-26 11:11:01
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-04-27 11:01:14
"""

import numpy as np
import pandas as pd
import xarray as xr
import os
from pathlib import Path
from tqdm import tqdm
import collections
from hydrodatautils.foundation import hydro_time
from hydrodata_china.settings.datasets_dir import DATASETS_DIR


# Some files of Sanxia_1D are named with spaces and need to be manually filled with underscores
class Sanxia_1D:
    def __init__(self, dataset_path=None):
        # Set the path of the dataset, cache, and result
        self.dataset_path = Path(dataset_path or DATASETS_DIR["Sanxia_1D"]["ROOT_DIR"])
        self.cache_path = Path(DATASETS_DIR["Sanxia_1D"]["CACHE_DIR"])
        self.result_path = Path(DATASETS_DIR["Sanxia_1D"]["EXPORT_DIR"])
        # Set the path of the Basins (Shapefile),
        self.data_path = self.get_data_path()
        self.sites = self.read_sites()

    # ========================== Dataset Path ==========================
    # Define paths for dataset components, including basin shapefiles, attributes,
    # forcing data, and target data. This ensures all required files are accessible.
    def get_data_path(self) -> collections.OrderedDict:
        sanxia_1d = self.dataset_path
        # Basins (Shapefile)
        basin_dir = sanxia_1d.joinpath(
            "Basin",
            "basin_test_113.shp",
        )
        # Attributes
        attribute_dir = sanxia_1d
        # Forcing
        forcing_dir = sanxia_1d.joinpath(
            "Forcing",
        )
        # Target
        target_dir = sanxia_1d.joinpath(
            "Streamflow",
        )
        return collections.OrderedDict(
            SANXIA_1D_DIR=sanxia_1d,
            SANXIA_1D_BASINS_DIR=basin_dir,
            SANXIA_1D_ATTRIBUTE_DIR=attribute_dir,
            SANXIA_1D_FORCING_DIR=forcing_dir,
            SANXIA_1D_TARGET_DIR=target_dir,
        )

    # ========================== Gauge sites ID ==========================
    # Retrieve the gauge site IDs from the dataset, which are necessary for
    # matching and processing data files for each catchment.
    def read_sites(self) -> pd.DataFrame:
        site_file = self.data_path["SANXIA_1D_ATTRIBUTE_DIR"].joinpath("attributes.csv")
        site_info = pd.read_csv(site_file)
        return site_info.iloc[:, [0]]

    # ========================== Paths of Attributes, Forcings, and Targets ==========================
    # Provide functions to retrieve file paths for attribute, forcing, and target data.
    # These paths point to specific CSV files required for data processing.
    def get_attributes(self) -> list[Path]:
        attributes_file = self.data_path["SANXIA_1D_ATTRIBUTE_DIR"].joinpath(
            "attributes.csv"
        )
        return attributes_file

    def get_forcings(self) -> dict:
        forcing_dir = self.data_path["SANXIA_1D_FORCING_DIR"]
        forcing_files = {
            'ERA5-land': [],
            'MSWEP': [],
            'Sanxia': []
        }
        # Get ERA5land files
        era5_dir = forcing_dir.joinpath("ERA5-land")
        if era5_dir.exists():
            forcing_files['ERA5-land'] = [
                era5_dir.joinpath(file)
                for file in os.listdir(era5_dir)
                if file.endswith(".csv") and file.startswith("era5land_china113_mean_")
            ]
        # Get MSWEP files
        mswep_dir = forcing_dir.joinpath("MSWEP")
        if mswep_dir.exists():
            for year_dir in sorted(os.listdir(mswep_dir)):
                year_path = mswep_dir.joinpath(year_dir)
                if year_path.is_dir() and year_dir.isdigit():
                    for file in os.listdir(year_path):
                        if file.endswith(".csv") and file.startswith("sanxia_"):
                            forcing_files['MSWEP'].append(year_path.joinpath(file))
        # Get Sanxia files
        sanxia_dir = forcing_dir.joinpath("Sanxia")
        if sanxia_dir.exists():
            forcing_files['Sanxia'] = [
                sanxia_dir.joinpath(file)
                for file in os.listdir(sanxia_dir)
                if file.endswith(".csv") and file.startswith("sanxia_") and "_average_rainfall" in file
            ]
        return forcing_files

    def get_targets(self) -> np.ndarray:
        targets_dir = self.data_path["SANXIA_1D_TARGET_DIR"]
        targets_files = [
            targets_dir.joinpath(file)
            for file in os.listdir(targets_dir)
            if file.endswith(".csv") and "sanxia_" in file
        ]
        return targets_files

    # ========================== Variables of Attributes, Forcings, and Targets ==========================
    # Define and retrieve variable names for attributes, forcing, and target data
    # based on predefined configurations. These variables specify the dataset columns to process.
    def read_attributes(self) -> np.ndarray:
        attributes_file = self.data_path["SANXIA_1D_ATTRIBUTE_DIR"].joinpath(
            "attributes.csv"
        )
        attributes = pd.read_csv(attributes_file)
        return np.array(attributes.columns[1:])

    def read_forcings(self) -> dict:
        """Return a dictionary of variables for each forcing type"""
        # 获取 ERA5land 文件的列名（排除前两列）
        forcing_dir = self.data_path["SANXIA_1D_FORCING_DIR"]
        era5land_dir = forcing_dir.joinpath("ERA5-land")
        era5land_files = [f for f in era5land_dir.glob("*.csv") if "era5land_china113_mean_" in f.name]
        era5_df = pd.read_csv(era5land_files[0])
        era5_vars = era5_df.columns[2:].tolist()
        return {
            'ERA5land': era5_vars,
            'MSWEP': ['precipitation'],  # MSWEP 固定变量
            'Sanxia': ['mean_rainfall']  # Sanxia 固定变量
        }

    def read_targets(self) -> np.ndarray:
        target_dir = self.data_path["SANXIA_1D_TARGET_DIR"]
        targets_file = next(
            (
                target_dir.joinpath(file)
                for file in os.listdir(target_dir)
                if file.endswith(".csv")
            ),
            None,
        )
        targets = pd.read_csv(targets_file)
        return np.array(targets.columns[1:])

    # ========================== Temporary Data Aggregation ==========================
    # Aggregate attribute, forcing, and target data into a unified format for further processing.
    # This version does not apply additional transformations or normalization.
    def cache_attributes_xrdataset(self):
        # Step 1: Read site information and attribute list
        site_info = self.read_sites()
        attribute_files = self.get_attributes()
        attributes = self.read_attributes()
        # Step 2: Read and filter attribute data
        data = pd.read_csv(attribute_files)
        data = data[data.iloc[:, 0].isin(site_info.iloc[:, 0].values)]
        data = data[
            [data.columns[0]] + [col for col in data.columns if col in attributes]
        ]
        # Step 3: Rename and format basin
        data.rename(columns={data.columns[0]: "basin"}, inplace=True)
        # Step 4: Convert to xarray.Dataset
        ds = xr.Dataset.from_dataframe(data.set_index("basin"))
        # Step 5: Save as NetCDF file
        ds.to_netcdf(self.cache_path.joinpath("sanxia_attributes.nc"))
        print(
            f"Cached attributes saved to {self.cache_path.joinpath('sanxia_attributes.nc')}"
        )

    def cache_forcing_xrdataset(self):
        """Process and save all forcing data to separate netCDF files"""
        # Get common data
        site_info = self.read_sites()
        forcings_files = self.get_forcings()
        forcings_vars = self.read_forcings()
        site_ids = site_info.iloc[:, 0].tolist()
        t_range = ["1993-01-01", "2025-01-01"]
        t_range_hour = ["1993-01-01 00:00", "2025-01-01 23:00"]

        # Process ERA5land
        era5_output = self.cache_path.joinpath("era5land_forcing.nc")
        era5_data = []
        for f in forcings_files['ERA5-land']:
            df = pd.read_csv(f)
            df['time'] = pd.to_datetime(df['time_start'])
            era5_data.append(df[df['time'].isin(hydro_time.t_range_days(t_range))])
        era5_df = pd.concat(era5_data)
        
        ds_era5 = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": era5_df['time'].unique(),
            }
        )
        for var in forcings_vars['ERA5land']:
            ds_era5[var] = xr.DataArray(
                era5_df.pivot(index='basin_id', columns='time', values=var).values,
                dims=["basin", "time"],
                coords={"basin": ds_era5.basin, "time": ds_era5.time},
            )
        ds_era5.to_netcdf(era5_output)

        # Process MSWEP
        mswep_output = self.cache_path.joinpath("mswep_forcing.nc")
        mswep_data = []
        for f in forcings_files['MSWEP']:
            df = pd.read_csv(f)
            df['time'] = pd.to_datetime(df['tm'])
            mswep_data.append(df[df['time'].isin(hydro_time.t_range_days(t_range))])
        mswep_df = pd.concat(mswep_data)
        
        ds_mswep = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": mswep_df['time'].unique(),
            }
        )
        ds_mswep['precipitation'] = xr.DataArray(
            mswep_df.pivot(index='basin', columns='time', values='precipitation').values,
            dims=["basin", "time"],
            coords={"basin": ds_mswep.basin, "time": ds_mswep.time},
        )
        ds_mswep.to_netcdf(mswep_output)

        # Process Sanxia
        sanxia_output = self.cache_path.joinpath("sanxia_forcing.nc")
        sanxia_data = []
        for f in forcings_files['Sanxia']:
            df = pd.read_csv(f)
            df['time'] = pd.to_datetime(df['TM'])
            sanxia_data.append(df[df['time'].isin(hydro_time.t_range_hours(t_range_hour))])
        sanxia_df = pd.concat(sanxia_data)
        
        ds_sanxia = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": sanxia_df['time'].unique(),
            }
        )
        ds_sanxia['mean_rainfall'] = xr.DataArray(
            sanxia_df.pivot(index='BASIN_ID', columns='time', values='mean_rainfall').values,
            dims=["basin", "time"],
            coords={"basin": ds_sanxia.basin, "time": ds_sanxia.time},
        )
        ds_sanxia.to_netcdf(sanxia_output)

    def cache_target_xrdataset(self):
        # Step 1: Prepare data
        site_info = self.read_sites()
        targets_files = self.get_targets()
        targets = self.read_targets()
        site_ids = site_info.iloc[:, 0].tolist()
        t_range = ["2020-01-01 00:00", "2025-01-01 23:00"]
        output_file = self.cache_path.joinpath("sanxia_target.nc")
        # Step 2: Create time index template
        sample_site = site_ids[0]
        matched_file = next(
            (file for file in targets_files if sample_site in file.name),
            None,
        )
        sample_df = pd.read_csv(matched_file)
        sample_df["time"] = pd.to_datetime(
            sample_df["time"], format="%Y-%m-%d %H:%M:%S"
        )
        sample_df["time"] = sample_df["time"].dt.floor("min")
        time_index = sample_df[
            sample_df["time"].isin(hydro_time.t_range_hours(t_range))
        ]["time"]
        # Step 3: Create empty xarray dataset
        ds = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": time_index,
            }
        )
        for var in targets:
            ds[var] = xr.DataArray(
                np.zeros((len(site_ids), len(time_index))),
                dims=["basin", "time"],
                coords={"basin": ds.basin, "time": ds.time},
            )
        # Step 4: Fill data
        for i, site_id in enumerate(tqdm(site_ids, desc="Processing target data")):
            # Find corresponding file
            matched_file = next(
                (file for file in targets_files if site_id in file.name),
                None,
            )
            df = pd.read_csv(matched_file)
            df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d %H:%M:%S")
            df = df[df["time"].isin(hydro_time.t_range_hours(t_range))]
            for var in targets:
                ds[var].loc[site_id, :] = df[var].values
        # Step 5: Save dataset
        ds.to_netcdf(output_file)
        print(f"Target data saved to {output_file}")

    # ========================== Standardize Data for Model Loading ==========================
    # Merge forcing and target data into a single dataset in a standardized format (NetCDF),
    # ensuring compatibility with hydrological models.
    def cache_xrdataset(self):
        self.normalize_attributes()
        self.normalize_timeseries()
        self.split_timeseries_into_batches()

    def normalize_attributes(self):
        input_file = self.cache_path.joinpath("sanxia_attributes.nc")
        output_file = self.result_path.joinpath("attributes.nc")
        with xr.open_dataset(input_file) as ds:
            ds = ds.rename({"area": "Area", "pre_mm_syr": "p_mean"})
            # Convert categorical variables to numeric
            ds.to_netcdf(output_file)
            print(f"Converted file saved to {output_file}.")

    def normalize_timeseries(self):
        # 定义输出文件路径
        output_file = self.cache_path.joinpath("sanxia_timeseries_norm.nc")
        
        # 读取目标数据集 (流量数据)
        target_file = self.cache_path.joinpath("sanxia_target.nc")
        with xr.open_dataset(target_file) as ds_target:
            # 将小时尺度聚合为日尺度
            ds_target_daily = ds_target.resample(time="1D").mean()                
            ds_target_daily['streamflow'].attrs['units'] = 'mm/d'
        
        # 读取三峡降雨数据并聚合为日尺度
        sanxia_file = self.cache_path.joinpath("sanxia_forcing.nc")
        with xr.open_dataset(sanxia_file) as ds_sanxia:
            # 将小时尺度聚合为日尺度（使用sum而不是mean，因为是降水量）
            ds_sanxia_daily = ds_sanxia.resample(time="1D").sum()
            ds_sanxia_daily = ds_sanxia_daily.rename({'mean_rainfall': 'precipitation_sanxia'})
            ds_sanxia_daily['precipitation_sanxia'].attrs['units'] = 'mm/d'
        
        # 读取MSWEP降水数据（已经是日尺度）
        mswep_file = self.cache_path.joinpath("mswep_forcing.nc")
        with xr.open_dataset(mswep_file) as ds_mswep:
            ds_mswep = ds_mswep.rename({'precipitation': 'precipitation_mswep'})
            ds_mswep['precipitation_mswep'].attrs['units'] = 'mm/d'
        
        # 读取ERA5-Land数据集
        era5_file = self.cache_path.joinpath("era5land_forcing.nc")
        with xr.open_dataset(era5_file) as ds_era5:
            era5_units = {
                'dewpoint_temperature_2m': 'K',
                'temperature_2m': 'K',
                'temperature_2m_min': 'K',
                'temperature_2m_max': 'K',
                'snow_depth_water_equivalent': 'm',
                'snowfall_sum': 'm',
                'snowmelt_sum': 'm',
                'total_precipitation_sum': 'm',
                'potential_evaporation_sum': 'm',
                'total_evaporation_sum': 'm',
                'surface_net_solar_radiation_sum': 'J/m²',
                'surface_net_thermal_radiation_sum': 'J/m²',
                'surface_solar_radiation_downwards_sum': 'J/m²',
                'surface_thermal_radiation_downwards_sum': 'J/m²',
            }
            # 为所有存在的变量添加单位属性
            for var, unit in era5_units.items():
                if var in ds_era5:
                    ds_era5[var].attrs['units'] = unit            
        
        # 确定各数据集的时间范围，找出共同的时间范围
        # 提取原始时间戳值而不是 DataArray 对象
        time_min = max(
            ds_target_daily.time.min().values,
            ds_sanxia_daily.time.min().values, 
            ds_mswep.time.min().values,
            ds_era5.time.min().values
        )
        time_max = min(
            ds_target_daily.time.max().values,
            ds_sanxia_daily.time.max().values, 
            ds_mswep.time.max().values,
            ds_era5.time.max().values
        )
        
        # 创建空的数据集（使用目标数据集的流域作为模板）
        ds_merged = xr.Dataset(
            coords={
                "basin": ds_target_daily.basin,
                "time": pd.date_range(
                    start=time_min,
                    end=time_max,
                    freq="D"
                )
            }
        )
        
        # 合并所有数据变量到新的数据集中
        # 流量数据
        ds_merged['streamflow'] = ds_target_daily['streamflow'].sel(
            time=slice(time_min, time_max)
        )
        
        # 三峡降雨数据
        ds_merged['precipitation_sanxia'] = ds_sanxia_daily['precipitation_sanxia'].sel(
            time=slice(time_min, time_max)
        )
        
        # MSWEP降水数据
        ds_merged['precipitation_mswep'] = ds_mswep['precipitation_mswep'].sel(
            time=slice(time_min, time_max)
        )
        
        # ERA5-Land数据 - 遍历所有变量
        for var in ds_era5.data_vars:
            ds_merged[var] = ds_era5[var].sel(
                time=slice(time_min, time_max)
            )
        
        # 保存标准化的数据集
        ds_merged.to_netcdf(output_file)
        print(f"标准化的时间序列数据已保存到 {output_file}")

    def split_timeseries_into_batches(self):
        input_file = self.cache_path.joinpath("sanxia_timeseries_norm.nc")
        ds = xr.open_dataset(input_file)
        basins = ds.coords["basin"].values
        batch_size = 100
        for batch_start in range(0, len(basins), batch_size):
            batch_end = min(batch_start + batch_size, len(basins))
            batch_basins = basins[batch_start:batch_end]
            ds_batch = ds.sel(basin=batch_basins)
            start_basin = batch_basins[0]
            end_basin = batch_basins[-1]
            output_file = self.result_path.joinpath(
                f"timeseries_1D_batch_{start_basin}_{end_basin}.nc"
            )
            ds_batch.to_netcdf(output_file)
        print("Final dataset split into batches and saved as NetCDF files.")
