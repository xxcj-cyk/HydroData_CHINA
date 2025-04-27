"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-03-30 10:44:43
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-04-23 09:03:14
"""

import xarray as xr
import os
import re
from hydrodata_camels.settings.datasets_dir import DATASETS_DIR


class ReadDataset_CAMELS:
    def __init__(self, dataset_type=None, source_name=None, source_path=None, time_unit=["1D"]):
        self.dataset_type = dataset_type
        self.source_name = source_name
        self.source_path = source_path
        self.time_unit = time_unit
        
    def read_ts_xrdataset(
        self,
        gage_id_lst: list = None,
        t_range: list = None,
        var_lst: list = None,
        **kwargs,
    ) -> dict:
        time_units = kwargs.get("time_units", self.time_unit)
        if var_lst is None:
            return None
        # Initialize a dictionary to hold datasets for each time unit
        datasets_by_time_unit = {}
        # Collect batch files specific to the current time unit
        for time_unit in time_units:
            batch_files = [
                os.path.join(DATASETS_DIR[self.source_name]["EXPORT_DIR"], f)
                for f in os.listdir(DATASETS_DIR[self.source_name]["EXPORT_DIR"])
                if re.match(
                    rf"^timeseries_{time_unit}_batch_[A-Za-z0-9_]+_[A-Za-z0-9_]+\.nc$",
                    f,
                )
            ]
            selected_datasets = []
            for batch_file in batch_files:
                ds = xr.open_dataset(batch_file)
                all_vars = ds.data_vars
                # Check if all variables exist in the dataset
                if any(var not in ds.variables for var in var_lst):
                    raise ValueError(f"var_lst must all be in {all_vars}")
                if valid_gage_ids := [
                    gid for gid in gage_id_lst if gid in ds["basin"].values
                ]:
                    ds_selected = ds[var_lst].sel(
                        basin=valid_gage_ids, time=slice(t_range[0], t_range[1])
                    )
                    selected_datasets.append(ds_selected)
                ds.close()  # Close the dataset to free memory
            # If any datasets were selected, concatenate them along the 'basin' dimension
            if selected_datasets:
                datasets_by_time_unit[time_unit] = xr.concat(
                    selected_datasets, dim="basin"
                ).sortby("basin")
            else:
                datasets_by_time_unit[time_unit] = xr.Dataset()
        return datasets_by_time_unit

    def read_attr_xrdataset(self, gage_id_lst=None, var_lst=None):
        if var_lst is None or len(var_lst) == 0:
            return None
        attr = xr.open_dataset(
            os.path.join(
                DATASETS_DIR[self.source_name]["EXPORT_DIR"], f"attributes.nc"
            )
        )
        return attr[var_lst].sel(basin=gage_id_lst)

    def read_area(self, gage_id_lst=None):
        return self.read_attr_xrdataset(gage_id_lst, ["Area"])  # area

    def read_mean_prcp(self, gage_id_lst=None, unit="mm/d"):
        pre_mm_syr = self.read_attr_xrdataset(gage_id_lst, ["p_mean"])  # pre_mm_syr
        da = pre_mm_syr["p_mean"]  # pre_mm_syr
        # Convert the unit to the specified unit, p_mean means yearly precipitation
        if unit in ["mm/d", "mm/day"]:
            converted_data = da / 365
        elif unit in ["mm/h", "mm/hour"]:
            converted_data = da / 8760
        elif unit in ["mm/3h", "mm/3hour"]:
            converted_data = da / (8760 / 3)
        elif unit in ["mm/8d", "mm/8day"]:
            converted_data = da / (365 / 8)
        else:
            raise ValueError(
                "unit must be one of ['mm/d', 'mm/day', 'mm/h', 'mm/hour', 'mm/3h', 'mm/3hour', 'mm/8d', 'mm/8day']"
            )
        # Set the units attribute
        converted_data.attrs["units"] = unit
        # Assign the modified DataArray back to the Dataset
        pre_mm_syr["p_mean"] = converted_data  # pre_mm_syr
        return pre_mm_syr
