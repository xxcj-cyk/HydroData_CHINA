"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-23 15:04:25
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-06-30 16:32:52
"""

import os
import xarray as xr

base_dir = r"E:\Takusan_no_Code\Paper\Paper2_Anhui_FloodEvent\Result\Sec1_ModelPerf\Month\Anhui_EnLoss-LSTM"

# 不带_train后缀的目录
basin_dirs = [
    "anhui_50406910_28",
    "anhui_50501200_36",
    "anhui_50701100_44",
    "anhui_50913900_37",
    "anhui_51004350_21",
    "anhui_62549024_80",
    "anhui_62700110_27",
    "anhui_62700700_41",
    "anhui_62802400_19",
    "anhui_62802700_62",
    "anhui_62803300_87",
    "anhui_62902000_48",
    "anhui_62906900_40",
    "anhui_62907100_26",
    "anhui_62907600_16",
    "anhui_62907601_14",
    "anhui_62909400_62",
    "anhui_62911200_43",
    "anhui_62916110_21",
    "anhui_70112150_10",
    "anhui_70114100_35"
]

# 带_train后缀的目录
basin_train_dirs = [f"{d}_train" for d in basin_dirs]

# 文件类型与对应目录
file_dir_map = {
    "epoch_best_flow_obs.nc": basin_dirs,
    "epoch_best_flow_pred.nc": basin_dirs,
    "epoch_best_model.pth_flow_obs.nc": basin_train_dirs,
    "epoch_best_model.pth_flow_pred.nc": basin_train_dirs
}

# 合并并保存
for fname, dirs in file_dir_map.items():
    files = []
    for d in dirs:
        fpath = os.path.join(base_dir, d, fname)
        if os.path.exists(fpath):
            files.append(fpath)
    if files:
        datasets = [xr.open_dataset(f) for f in files]
        ds = xr.concat(datasets, dim="basin")
        out_path = os.path.join(base_dir, fname)
        ds.to_netcdf(out_path)
        print(f"已保存: {out_path}")
    else:
        print(f"{fname} 没有找到文件。")