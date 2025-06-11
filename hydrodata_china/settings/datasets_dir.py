"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-04-26 11:06:32
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-11 11:26:00
"""


from pathlib import Path
from hydrodatautils.foundation import hydro_dirction

DATASETS_DIR = {
    "Sanxia_1D": {
        "ROOT_DIR": Path(hydro_dirction.get_origin_dir(dataset_type="Dataset_CHINA", dataset_name="Sanxia_Project")),
        "CACHE_DIR": Path(hydro_dirction.get_cache_dir(dataset_type="Dataset_CHINA", dataset_name="Sanxia_1D")),
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_CHINA", dataset_name="Sanxia_1D")),
    },
    "Sanxia_1H": {
        "ROOT_DIR": Path(hydro_dirction.get_origin_dir(dataset_type="Dataset_CHINA", dataset_name="Sanxia_Project")),
        "CACHE_DIR": Path(hydro_dirction.get_cache_dir(dataset_type="Dataset_CHINA", dataset_name="Sanxia_1H")),
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_CHINA", dataset_name="Sanxia_1H")),
    },
    "Anhui_1H": {
        "ROOT_DIR": Path(hydro_dirction.get_origin_dir(dataset_type="Dataset_CHINA", dataset_name="Anhui_Project")),
        "CACHE_DIR": Path(hydro_dirction.get_cache_dir(dataset_type="Dataset_CHINA", dataset_name="Anhui_1H")),
        "EXPORT_DIR": Path(hydro_dirction.get_export_dir(dataset_type="Dataset_CHINA", dataset_name="Anhui_1H")),
    },
}
