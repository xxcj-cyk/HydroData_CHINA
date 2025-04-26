# -*- coding: utf-8 -*-
# @Author: Yikai CHAI
# @Date:   2025-03-23 10:11:16
# @Last Modified by:   Yikai CHAI
# @Last Modified time: 2025-04-23 15:43:12
from pathlib import Path
from hydrodatautils.foundation import hydro_dirction

DATASETS_DIR = {
    "CAMELS_US_HydroATLAS": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_HydroATLAS")),
    },
    "CAMELS_US_MSWEP": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_MSWEP")),
    },
    "CAMELS_US_ERA5Land": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_ERA5Land")),
    },
    "CAMELS_US_HydroATLAS_ERA5Land": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_HydroATLAS_ERA5Land")),
    },
    "CAMELS_US_HydroATLAS_MSWEP_ERA5Land": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_HydroATLAS_MSWEP_ERA5Land")),
    },
    "CAMELS_US_HydroATLAS_MSWEP_ERA5Land-har": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_HydroATLAS_MSWEP_ERA5Land-har")),
    },
    "CAMELS_US_Budyko": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_Budyko")),
    },
    "Changdian_HydroATLAS_ERA5Land": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="Changdian_HydroATLAS_ERA5Land")),
    },
    "Changdian_HydroATLAS_MSWEP_ERA5Land": {
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_TL", dataset_name="Changdian_HydroATLAS_MSWEP_ERA5Land")),
    },
}

DATA_DIR = {
    "hydro_ouyang": {
        "CACHE_DIR": Path(hydro_dirction.get_cache_dir(dataset_type="Dataset_TL", dataset_name="hydro_ouyang")),
    },
    "MSWEP_700_1993-2024": {
        "CACHE_DIR": Path(hydro_dirction.get_cache_dir(dataset_type="Dataset_TL", dataset_name="MSWEP_700_1993-2024")),
    },
    "CAMELS_US_ERA5-Land": {
        "ORIGIN_DIR": Path(hydro_dirction.get_origin_dir(dataset_type="Dataset_CAMELS_Extend", dataset_name="CAMELS_US_ERA5-Land")),
        "CACHE_DIR": Path(hydro_dirction.get_cache_dir(dataset_type="Dataset_TL", dataset_name="CAMELS_US_ERA5-Land")),
    },
}
