"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-04-28 22:23:57
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-04-28 23:47:05
"""


import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from tqdm import tqdm
import collections
from hydrodatautils.foundation import hydro_time
from hydrodata_china.settings.datasets_dir import DATASETS_DIR


class Sanxia_1D:
    def __init__(self, dataset_path=None):
        """Initialize the Sanxia 1D dataset processor"""
        self.dataset_path = Path(dataset_path or DATASETS_DIR["Sanxia_1D"]["ROOT_DIR"])
        self.cache_path = Path(DATASETS_DIR["Sanxia_1D"]["CACHE_DIR"])
        self.result_path = Path(DATASETS_DIR["Sanxia_1D"]["EXPORT_DIR"])
        self.data_path = self._get_data_paths()
        self.sites = pd.read_csv(self.data_path["attributes"]).iloc[:, [0]]


    def _get_data_paths(self):
        """Get all required dataset paths in an organized dictionary"""
        base = self.dataset_path
        return collections.OrderedDict(
            basins=base.joinpath("Basin", "basin_test_113.shp"),
            attributes=base.joinpath("attributes.csv"),
            forcing=collections.OrderedDict(
                ERA5_land=base.joinpath("Forcing", "ERA5-land"),
                MSWEP=base.joinpath("Forcing", "MSWEP"),
                Sanxia=base.joinpath("Forcing", "Sanxia")
            ),
            target=base.joinpath("Streamflow")
        )


    def cache_attributes(self):
        """Cache basin attributes as NetCDF file"""
        attrs = pd.read_csv(self.data_path["attributes"])
        attrs = attrs[attrs.iloc[:, 0].isin(self.sites.iloc[:, 0])]
        attrs.rename(columns={attrs.columns[0]: "basin"}, inplace=True)
        ds = xr.Dataset.from_dataframe(attrs.set_index("basin"))
        ds.to_netcdf(self.cache_path.joinpath("sanxia_attributes.nc"))
        print(f"Attributes cached to {self.cache_path/'sanxia_attributes.nc'}")


    def cache_forcings(self):
        """Cache all forcing data types (ERA5-land, MSWEP, Sanxia)"""
        site_ids = self.sites.iloc[:, 0].tolist()
        t_range_days = ["1993-01-01", "2025-01-01"]
        t_range_hours = ["1993-01-01 00:00", "2025-01-01 23:00"]
        # Process ERA5-land data
        era5_files = [
            f for f in self.data_path["forcing"]["ERA5_land"].glob("*.csv") 
            if "era5land_china113_mean_" in f.name
        ]
        era5_dfs = []
        for f in tqdm(era5_files, desc="Processing ERA5-land data"):
            df = pd.read_csv(f)
            df['time'] = pd.to_datetime(df['time_start'])
            era5_dfs.append(df[df['time'].isin(hydro_time.t_range_days(t_range_days))])
        era5_df = pd.concat(era5_dfs)
        # Create empty dataset
        ds_era5 = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": era5_df['time'].unique()
            }
        )
        # Add variables
        for var in era5_df.columns[2:]:  # Skip time and basin_id columns
            if var not in ['time', 'time_start', 'basin_id']:
                ds_era5[var] = xr.DataArray(
                    era5_df.pivot(index="basin_id", columns="time", values=var).values,
                    dims=["basin", "time"],
                    coords={"basin": ds_era5.basin, "time": ds_era5.time}
                )
        ds_era5.to_netcdf(self.cache_path.joinpath("era5land_forcing.nc"))
        print(f"ERA5-land forcing data cached to {self.cache_path/'era5land_forcing.nc'}")
        # Process MSWEP data
        mswep_files = []
        for year_dir in sorted(self.data_path["forcing"]["MSWEP"].iterdir()):
            if year_dir.is_dir() and year_dir.name.isdigit():
                mswep_files.extend(
                    f for f in year_dir.glob("*.csv") 
                    if f.name.startswith("sanxia_") and f.name.endswith(".csv")
                )
        mswep_dfs = []
        for f in tqdm(mswep_files, desc="Processing MSWEP data"):
            df = pd.read_csv(f)
            df['time'] = pd.to_datetime(df['tm'])
            mswep_dfs.append(df[df['time'].isin(hydro_time.t_range_days(t_range_days))])
        mswep_df = pd.concat(mswep_dfs)
        # Create empty dataset
        ds_mswep = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": mswep_df['time'].unique()
            }
        )
        # Add variables
        ds_mswep["precipitation"] = xr.DataArray(
            mswep_df.pivot(index="basin", columns="time", values="precipitation").values,
            dims=["basin", "time"],
            coords={"basin": ds_mswep.basin, "time": ds_mswep.time}
        )
        ds_mswep.to_netcdf(self.cache_path.joinpath("mswep_forcing.nc"))
        print(f"MSWEP forcing data cached to {self.cache_path/'mswep_forcing.nc'}")
        # Process Sanxia data
        sanxia_files = [
            f for f in self.data_path["forcing"]["Sanxia"].glob("*.csv") 
            if f.name.startswith("sanxia_") and "_average_rainfall" in f.name
        ] 
        sanxia_dfs = []
        for f in tqdm(sanxia_files, desc="Processing Sanxia data"):
            df = pd.read_csv(f)
            df['time'] = pd.to_datetime(df['TM'])
            sanxia_dfs.append(df[df['time'].isin(hydro_time.t_range_hours(t_range_hours))])
        sanxia_df = pd.concat(sanxia_dfs)
        # Create empty dataset
        ds_sanxia = xr.Dataset(
            coords={
                "basin": site_ids,
                "time": sanxia_df['time'].unique()
            }
        )
        # Add variables
        ds_sanxia["mean_rainfall"] = xr.DataArray(
            sanxia_df.pivot(index="BASIN_ID", columns="time", values="mean_rainfall").values,
            dims=["basin", "time"],
            coords={"basin": ds_sanxia.basin, "time": ds_sanxia.time}
        )
        ds_sanxia.to_netcdf(self.cache_path.joinpath("sanxia_forcing.nc"))
        print(f"Sanxia forcing data cached to {self.cache_path/'sanxia_forcing.nc'}")


    def cache_targets(self):
        """Cache streamflow target data"""
        site_ids = self.sites.iloc[:, 0].tolist()
        t_range = ["2020-01-01 00:00", "2025-01-01 23:00"]
        target_files = [
            f for f in self.data_path["target"].glob("*.csv") 
            if "sanxia_" in f.name
        ]
        # Create template from first file
        sample_file = next(f for f in target_files if site_ids[0] in f.name)
        sample_df = pd.read_csv(sample_file)
        sample_df["time"] = pd.to_datetime(sample_df["time"])
        time_index = sample_df[
            sample_df["time"].isin(hydro_time.t_range_hours(t_range))
        ]["time"]
        # Create empty dataset
        ds = xr.Dataset(
            coords={"basin": site_ids, "time": time_index},
            data_vars={"streamflow": (["basin", "time"], 
                                    np.zeros((len(site_ids), len(time_index))))}
        )
        # Fill with actual data
        for i, site_id in enumerate(tqdm(site_ids, desc="Processing targets")):
            file = next(f for f in target_files if site_id in f.name)
            df = pd.read_csv(file)
            df["time"] = pd.to_datetime(df["time"])
            df = df[df["time"].isin(hydro_time.t_range_hours(t_range))]
            ds["streamflow"].loc[site_id, :] = df["streamflow"].values
        ds.to_netcdf(self.cache_path.joinpath("sanxia_target.nc"))
        print(f"Target data cached to {self.cache_path/'sanxia_target.nc'}")


    def normalize_attributes(self):
        """Normalize and save attribute data with standard names"""
        with xr.open_dataset(self.cache_path.joinpath("sanxia_attributes.nc")) as ds:
            ds = ds.rename({"area": "Area", "pre_mm_syr": "p_mean"})
            ds.to_netcdf(self.result_path.joinpath("attributes.nc"))
            print(f"Normalized attributes saved to {self.result_path/'attributes.nc'}")


    def normalize_timeseries(self):
        """Normalize and merge all timeseries data"""
        # Open all datasets
        with xr.open_dataset(self.cache_path.joinpath("sanxia_target.nc")) as ds_target, \
             xr.open_dataset(self.cache_path.joinpath("sanxia_attributes.nc")) as ds_attr, \
             xr.open_dataset(self.cache_path.joinpath("sanxia_forcing.nc")) as ds_sanxia, \
             xr.open_dataset(self.cache_path.joinpath("mswep_forcing.nc")) as ds_mswep, \
             xr.open_dataset(self.cache_path.joinpath("era5land_forcing.nc")) as ds_era5:
            # Convert streamflow from m³/s to mm/day
            basin_area = ds_attr['area']  # in km²
            conversion_factor = 3.6 / basin_area  # m³/s to mm/day
            ds_target['streamflow'] = ds_target['streamflow'] * conversion_factor
            ds_target['streamflow'].attrs['units'] = 'mm/day'
            # Resample to daily
            ds_target_daily = ds_target.resample(time="1D").sum()
            ds_sanxia_daily = ds_sanxia.resample(time="1D").sum()
            # Rename variables
            ds_sanxia_daily = ds_sanxia_daily.rename({'mean_rainfall': 'precipitation_sanxia'})
            ds_mswep = ds_mswep.rename({'precipitation': 'precipitation_mswep'})
            # add units
            ds_target_daily["streamflow"].attrs["units"] = "mm/d"
            ds_sanxia_daily["precipitation_sanxia"].attrs["units"] = "mm/d"
            ds_mswep["precipitation_mswep"].attrs["units"] = "mm/d"
            era5_units = {
                "dewpoint_temperature_2m": "K",
                "temperature_2m": "K",
                "temperature_2m_min": "K",
                "temperature_2m_max": "K",
                "snow_depth_water_equivalent": "m",
                "snowfall_sum": "m",
                "snowmelt_sum": "m",
                "total_precipitation_sum": "m",
                "potential_evaporation_sum": "m",
                "total_evaporation_sum": "m",
                "surface_net_solar_radiation_sum": "J/m²",
                "surface_net_thermal_radiation_sum": "J/m²",
                "surface_solar_radiation_downwards_sum": "J/m²",
                "surface_thermal_radiation_downwards_sum": "J/m²",
            }
            for var, unit in era5_units.items():
                if var in ds_era5:
                    ds_era5[var].attrs["units"] = unit
            # Transpose dimensions 
            ds_target_daily = ds_target_daily.transpose("basin", "time")
            ds_sanxia_daily = ds_sanxia_daily.transpose("basin", "time")
            ds_mswep = ds_mswep.transpose("basin", "time")
            ds_era5 = ds_era5.transpose("basin", "time")
            # Determine common time period
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
            # Merge all datasets
            merged = xr.merge([
                ds_target_daily[['streamflow']].sel(time=slice(time_min, time_max)),
                ds_sanxia_daily[['precipitation_sanxia']].sel(time=slice(time_min, time_max)),
                ds_mswep[['precipitation_mswep']].sel(time=slice(time_min, time_max)),
                ds_era5.sel(time=slice(time_min, time_max))
            ])
            # Save merged dataset
            merged.to_netcdf(self.cache_path.joinpath("sanxia_timeseries_norm.nc"))
            print(f"Normalized timeseries saved to {self.cache_path/'sanxia_timeseries_norm.nc'}")


    def split_into_batches(self, batch_size=100):
        """Split normalized data into smaller batches"""
        with xr.open_dataset(self.cache_path.joinpath("sanxia_timeseries_norm.nc")) as ds:
            basins = ds.basin.values
            for i in range(0, len(basins), batch_size):
                batch = ds.isel(basin=slice(i, i + batch_size))
                start_id, end_id = basins[i], basins[min(i + batch_size - 1, len(basins) - 1)]
                batch.to_netcdf(
                    self.result_path.joinpath(f"timeseries_1D_batch_{start_id}_{end_id}.nc")
                )
        print(f"Dataset split into {len(range(0, len(basins), batch_size))} batches")


    def process_full_dataset(self):
        """Complete processing pipeline"""
        print("Starting Sanxia 1D dataset processing...")
        self.cache_attributes()
        self.cache_forcings()
        self.cache_targets()
        self.normalize_attributes()
        self.normalize_timeseries()
        self.split_into_batches()
        print("Dataset processing completed successfully!")