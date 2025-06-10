"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-05-29 10:52:13
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-05-29 15:21:36
"""

import os
import glob
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime


def identify_train_val_sets(folder_path, train_ratio=0.8, min_validation_samples=5):
    """
    识别每个流域的训练集和验证集场次
    
    根据指定的比例将每个流域的数据划分为训练集和验证集。
    默认采用时间顺序划分，较早的场次用于训练，较新的场次用于验证。
    
    参数:
        folder_path (str): 原始nc文件所在文件夹路径
        train_ratio (float): 训练集占总数据的比例 (默认0.8，即4:1)
        min_validation_samples (int): 验证集最少包含的样本数 (默认5个)
    
    返回:
        tuple: 包含两个字典:
            - train_sets (dict): 键为流域ID，值为训练集场次列表
            - val_sets (dict): 键为流域ID，值为验证集场次列表
    """
    # 获取所有nc文件
    nc_files = glob.glob(os.path.join(folder_path, "*.nc"))
    
    if not nc_files:
        print(f"错误: 在 {folder_path} 中未找到任何nc文件")
        return {}, {}
    
    # 通过文件名提取流域信息
    basin_files = {}
    
    for nc_file in nc_files:
        filename = os.path.basename(nc_file)
        # 从文件名中提取流域ID
        parts = filename.split('_')
        if len(parts) >= 2:
            # 提取流域ID (如 50406910)
            basin_id = parts[1]
            
            # 验证 basin_id 是否是有效的流域标识
            if basin_id.isdigit():
                if basin_id not in basin_files:
                    basin_files[basin_id] = []
                basin_files[basin_id].append(nc_file)
            else:
                print(f"警告: 从文件名 {filename} 中提取的流域ID {basin_id} 不是有效的数字标识")
        else:
            print(f"警告: 文件名 {filename} 格式不符合预期，无法提取流域ID")
    
    # 打印找到的流域信息
    print(f"\n找到 {len(basin_files)} 个流域:")
    for basin_id, files in basin_files.items():
        print(f"  - 流域 {basin_id}: {len(files)} 个文件")
    
    # 为每个流域划分训练集和验证集
    train_sets = {}
    val_sets = {}
    
    for basin_id, files in basin_files.items():
        if not files:
            continue
            
        # 按时间排序文件（假设文件名中包含日期信息）
        files.sort(key=lambda x: os.path.basename(x).split('_')[2].split('.')[0])
        
        # 计算训练集和验证集数量
        total_count = len(files)
        val_count = max(min_validation_samples, int(total_count * (1 - train_ratio)))
        train_count = total_count - val_count
        
        # 确保有足够的数据进行划分
        if train_count <= 0:
            print(f"警告: 流域 {basin_id} 的场次数 ({total_count}) 不足以进行划分，至少需要 {min_validation_samples+1} 个场次")
            continue
            
        # 划分训练集和验证集（较新的场次为验证集）
        train_files = files[:train_count]
        val_files = files[train_count:]
        
        # 提取文件名（不含路径和扩展名）作为场次标识
        train_sets[basin_id] = [os.path.basename(f).split('.')[0] for f in train_files]
        val_sets[basin_id] = [os.path.basename(f).split('.')[0] for f in val_files]
        
        print(f"\n流域 {basin_id} 划分结果:")
        print(f"  - 总场次数: {total_count}")
        print(f"  - 训练集: {train_count} 场次")
        print(f"  - 验证集: {val_count} 场次")
    
    return train_sets, val_sets


def process_nc_files(input_folder, output_folder):
    """
    处理NC文件：将原始time维度的内容复制到time_true变量中，并统一输出744个时段
    
    参数:
        input_folder (str): 原始nc文件所在文件夹路径
        output_folder (str): 输出nc文件的文件夹路径
    
    返回:
        None
    """
    # 确保输出文件夹存在
    os.makedirs(output_folder, exist_ok=True)
    
    # 获取所有nc文件
    nc_files = glob.glob(os.path.join(input_folder, "*.nc"))
    
    if not nc_files:
        print(f"错误: 在 {input_folder} 中未找到任何nc文件")
        return
    
    print(f"共找到 {len(nc_files)} 个nc文件待处理")
    processed_count = 0
    error_count = 0
    error_files = []  # 记录处理失败的文件
    
    # 获取训练集和验证集
    train_sets, val_sets = identify_train_val_sets(input_folder)
    
    # 将所有流域的训练集和验证集场次合并为两个列表
    all_train_events = []
    all_val_events = []
    for basin_id in train_sets:
        all_train_events.extend(train_sets[basin_id])
    for basin_id in val_sets:
        all_val_events.extend(val_sets[basin_id])
    
    for nc_file in nc_files:
        filename = os.path.basename(nc_file)
        event_id = filename.split('.')[0]  # 获取不带扩展名的文件名作为场次ID
        print(f"正在处理: {filename}")
        # 读取原始nc文件
        ds = xr.open_dataset(nc_file)
        
        # 获取原始时间值和数据
        original_times = ds.time.values
        original_length = len(original_times)
        
        # 统一时段长度为744
        target_length = 744
        
        # 检查所有变量的时间维度是否一致
        time_dims = {}
        for var_name, var in ds.data_vars.items():
            if 'time' in var.dims:
                var_time_len = var.sizes['time']
                if var_time_len not in time_dims:
                    time_dims[var_time_len] = []
                time_dims[var_time_len].append(var_name)
        
        # 如果有多个不同的时间维度长度，先统一它们
        if len(time_dims) > 1:
            print(f"  ⚠ 发现多个不同的时间维度长度: {time_dims}")
            # 找出最短的时间维度长度
            min_time_len = min(time_dims.keys())
            # 截取所有变量到最短的长度
            for var_name in ds.data_vars:
                if 'time' in ds[var_name].dims and ds[var_name].sizes['time'] > min_time_len:
                    ds[var_name] = ds[var_name].isel(time=slice(0, min_time_len))
            # 更新时间维度
            ds = ds.isel(time=slice(0, min_time_len))
            # 更新原始长度
            original_length = min_time_len
            original_times = ds.time.values
            print(f"  ℹ 已将所有变量截取到相同的时间长度: {min_time_len}")
        
        if original_length > target_length:
            # 如果原始时段长度大于目标长度，从后往前截取744个时段
            start_idx = original_length - target_length
            ds = ds.isel(time=slice(start_idx, original_length))
            print(f"  ℹ 原始时段数 {original_length} 大于目标时段数 {target_length}，从后往前截取")
        elif original_length < target_length:
            # 如果原始时段长度小于目标长度，需要填充
            # 计算需要填充的时段数
            padding_length = target_length - original_length
            print(f"  ℹ 原始时段数 {original_length} 小于目标时段数 {target_length}，需要填充 {padding_length} 个时段")
            
            # 创建填充数据
            # 假设时间间隔为1小时
            if len(original_times) > 0:
                # 检查时间间隔
                if original_length > 1:
                    # 确保时间是datetime64类型
                    if isinstance(original_times[0], str):
                        original_times = np.array([np.datetime64(t) for t in original_times])
                    # 计算平均时间间隔
                    time_diffs = np.diff(original_times)
                    avg_time_diff = np.mean(time_diffs)
                    time_diff = avg_time_diff
                else:
                    # 如果只有一个时间点，默认使用1小时间间隔
                    time_diff = np.timedelta64(1, 'h')
                
                print(f"  ℹ 使用时间间隔: {time_diff}")
                
                # 创建新的时间数组（在原始时间前面添加）
                new_times = np.array([original_times[0] - (i+1) * time_diff for i in range(padding_length)])
                new_times = np.flip(new_times)  # 反转数组使其按时间顺序
                padded_times = np.concatenate([new_times, original_times])
                
                # 为所有变量创建填充数据（使用第一个时段的值，而不是NaN）
                padded_ds = xr.Dataset()
                padded_ds['time'] = ('time', padded_times)
                
                # 为每个数据变量创建填充数据
                for var_name, var in ds.data_vars.items():
                    if 'time' in var.dims:
                        # 获取变量的维度
                        dims = var.dims
                        shape = list(var.shape)
                        time_dim_idx = dims.index('time')
                        
                        # 创建填充数组
                        pad_shape = shape.copy()
                        pad_shape[time_dim_idx] = padding_length
                        
                        # 使用第一个时段的值进行填充，而不是NaN
                        if np.issubdtype(var.dtype, np.number):
                            # 获取第一个时段的值
                            first_values = var.isel(time=0).values
                            
                            # 创建填充数组，用第一个时段的值填充
                            pad_data = np.zeros(pad_shape, dtype=var.dtype)
                            
                            # 对于多维数组，需要在时间维度上重复第一个时段的值
                            if len(pad_shape) > 1:
                                # 为每个时间点复制第一个时段的值
                                for i in range(padding_length):
                                    # 选择正确的索引方式来设置值
                                    idx = [slice(None)] * len(dims)
                                    idx[time_dim_idx] = i
                                    pad_data[tuple(idx)] = first_values
                            else:
                                # 对于一维数组，直接填充第一个值
                                pad_data.fill(var.values[0])
                        else:
                            # 非数值型变量，尝试使用第一个值
                            first_value = var.isel(time=0).values
                            pad_data = np.full(pad_shape, first_value, dtype=var.dtype)
                        
                        # 合并原始数据和填充数据
                        padded_data = np.concatenate([pad_data, var.values], axis=time_dim_idx)
                        padded_ds[var_name] = (dims, padded_data)
                    else:
                        # 对于不依赖时间维度的变量，直接复制
                        padded_ds[var_name] = var
                
                # 替换原始数据集
                ds = padded_ds
                print(f"  ℹ 成功填充时段，新的时段数: {len(ds.time)}，填充值使用了第一个时段的值")
            else:
                raise ValueError("原始时间数组为空，无法进行填充")
        
        # 移除这行代码（约在第652-653行）
        # ds['time_true'] = xr.Variable('time', ds.time.values)
        
        # 根据场次是训练集还是验证集，设置不同的时间范围
        if event_id in all_train_events:
            # 训练集时间范围：2024-07-01 00:00:00 ~ 2024-07-31 23:00:00
            start_time = np.datetime64('2024-07-01T00:00:00')
            # 计算每小时的时间点
            new_times = np.array([start_time + np.timedelta64(i, 'h') for i in range(target_length)])
            # 设置time_true为7月的时间变量
            ds['time_true'] = xr.Variable('time', new_times)
            print(f"  ℹ 设置训练集时间范围: 2024-07-01 00:00:00 ~ 2024-07-31 23:00:00")
            
            # 新增：为训练集补充8月份数据
            aug_start_time = np.datetime64('2024-08-01T00:00:00')
            aug_end_time = np.datetime64('2024-08-31T23:00:00')
            aug_hours = 31 * 24  # 8月份总小时数
            aug_times = np.array([aug_start_time + np.timedelta64(i, 'h') for i in range(aug_hours)])
            
            # 创建扩展数据集
            aug_ds = xr.Dataset()
            aug_ds['time'] = ('time', aug_times)
            # 设置扩展数据集的time_true也为8月的时间变量
            aug_ds['time_true'] = ('time', aug_times)
            
            # 获取7月31日23:00:00的streamflow值
            last_july_time = np.datetime64('2024-07-31T23:00:00')
            last_july_idx = np.where(new_times == last_july_time)[0][0]
            
            # 为每个变量创建扩展数据
            for var_name, var in ds.data_vars.items():
                if 'time' in var.dims:
                    # 获取变量的维度
                    dims = var.dims
                    shape = list(var.shape)
                    time_dim_idx = dims.index('time')
                    
                    # 创建扩展数组
                    aug_shape = shape.copy()
                    aug_shape[time_dim_idx] = aug_hours
                    
                    # 对于streamflow变量，使用7月31日23:00:00的值
                    if var_name == 'streamflow' or var_name == 'streamflow_obs':
                        # 获取7月31日23:00:00的值
                        last_july_value = var.isel(time=last_july_idx).values
                        
                        # 创建填充数组，用7月31日23:00:00的值填充
                        aug_data = np.zeros(aug_shape, dtype=var.dtype)
                        
                        # 对于多维数组，需要在时间维度上重复该值
                        if len(aug_shape) > 1:
                            for i in range(aug_hours):
                                idx = [slice(None)] * len(dims)
                                idx[time_dim_idx] = i
                                aug_data[tuple(idx)] = last_july_value
                        else:
                            # 对于一维数组，直接填充该值
                            aug_data.fill(last_july_value)
                    elif var_name == 'P_Anhui':
                        # P_Anhui变量设为0
                        aug_data = np.zeros(aug_shape, dtype=np.float64)
                    else:
                        # 其他变量设为NaN
                        aug_data = np.full(aug_shape, np.nan, dtype=var.dtype)
                    
                    aug_ds[var_name] = (dims, aug_data)
                else:
                    # 对于不依赖时间维度的变量，直接复制
                    aug_ds[var_name] = var
            
            print(f"  ℹ 已为训练集补充2024-08-01 00:00:00~2024-08-31 23:00:00的数据")
            
        elif event_id in all_val_events:
            # 验证集时间范围：2024-08-01 00:00:00 ~ 2024-08-31 23:00:00
            start_time = np.datetime64('2024-08-01T00:00:00')
            # 计算每小时的时间点
            new_times = np.array([start_time + np.timedelta64(i, 'h') for i in range(target_length)])
            # 设置time_true为8月的时间变量
            ds['time_true'] = xr.Variable('time', new_times)
            print(f"  ℹ 设置验证集时间范围: 2024-08-01 00:00:00 ~ 2024-08-31 23:00:00")
            
            # 新增：为验证集补充7月份数据
            jul_start_time = np.datetime64('2024-07-01T00:00:00')
            jul_end_time = np.datetime64('2024-07-31T23:00:00')
            jul_hours = 31 * 24  # 7月份总小时数
            jul_times = np.array([jul_start_time + np.timedelta64(i, 'h') for i in range(jul_hours)])
            
            # 创建扩展数据集
            jul_ds = xr.Dataset()
            jul_ds['time'] = ('time', jul_times)
            # 设置扩展数据集的time_true也为7月的时间变量
            jul_ds['time_true'] = ('time', jul_times)
            
            # 获取8月1日00:00:00的streamflow值
            first_aug_time = np.datetime64('2024-08-01T00:00:00')
            first_aug_idx = np.where(new_times == first_aug_time)[0][0]
            
            # 为每个变量创建扩展数据
            for var_name, var in ds.data_vars.items():
                if 'time' in var.dims:
                    # 获取变量的维度
                    dims = var.dims
                    shape = list(var.shape)
                    time_dim_idx = dims.index('time')
                    
                    # 创建扩展数组
                    jul_shape = shape.copy()
                    jul_shape[time_dim_idx] = jul_hours
                    
                    # 对于streamflow变量，使用8月1日00:00:00的值
                    if var_name == 'streamflow' or var_name == 'streamflow_obs':
                        # 获取8月1日00:00:00的值
                        first_aug_value = var.isel(time=first_aug_idx).values
                        
                        # 创建填充数组，用8月1日00:00:00的值填充
                        jul_data = np.zeros(jul_shape, dtype=var.dtype)
                        
                        # 对于多维数组，需要在时间维度上重复该值
                        if len(jul_shape) > 1:
                            for i in range(jul_hours):
                                idx = [slice(None)] * len(dims)
                                idx[time_dim_idx] = i
                                jul_data[tuple(idx)] = first_aug_value
                        else:
                            # 对于一维数组，直接填充该值
                            jul_data.fill(first_aug_value)
                    elif var_name == 'P_Anhui':
                        # P_Anhui变量设为0
                        jul_data = np.zeros(jul_shape, dtype=np.float64)
                    else:
                        # 其他变量设为NaN
                        jul_data = np.full(jul_shape, np.nan, dtype=var.dtype)
                    
                    jul_ds[var_name] = (dims, jul_data)
                else:
                    # 对于不依赖时间维度的变量，直接复制
                    jul_ds[var_name] = var
            
            print(f"  ℹ 已为验证集补充2024-07-01 00:00:00~2024-07-31 23:00:00的数据")
            
        else:
            # 如果不在训练集或验证集中，保持原始时间
            new_times = ds.time.values
            print(f"  ℹ 场次 {event_id} 不在训练集或验证集中，保持原始时间")
        
        # 确保所有变量的时间维度长度一致
        for var_name, var in list(ds.data_vars.items()):
            if 'time' in var.dims and var.sizes['time'] != target_length:
                print(f"  ⚠ 变量 {var_name} 的时间维度长度 ({var.sizes['time']}) 与目标长度 ({target_length}) 不一致，将进行调整")
                
                if var.sizes['time'] < target_length:
                    # 如果变量的时间维度小于目标长度，需要填充
                    padding_length = target_length - var.sizes['time']
                    
                    # 获取变量的维度
                    dims = var.dims
                    shape = list(var.shape)
                    time_dim_idx = dims.index('time')
                    
                    # 创建填充数组
                    pad_shape = shape.copy()
                    pad_shape[time_dim_idx] = padding_length
                    
                    # 使用第一个时段的值进行填充
                    if np.issubdtype(var.dtype, np.number):
                        # 获取第一个时段的值
                        first_values = var.isel(time=0).values
                        
                        # 创建填充数组
                        pad_data = np.zeros(pad_shape, dtype=var.dtype)
                        
                        # 对于多维数组，需要在时间维度上重复第一个时段的值
                        if len(pad_shape) > 1:
                            # 为每个时间点复制第一个时段的值
                            for i in range(padding_length):
                                # 选择正确的索引方式来设置值
                                idx = [slice(None)] * len(dims)
                                idx[time_dim_idx] = i
                                pad_data[tuple(idx)] = first_values
                        else:
                            # 对于一维数组，直接填充第一个值
                            pad_data.fill(var.values[0])
                    else:
                        # 非数值型变量，尝试使用第一个值
                        first_value = var.isel(time=0).values
                        pad_data = np.full(pad_shape, first_value, dtype=var.dtype)
                    
                    # 合并原始数据和填充数据
                    padded_data = np.concatenate([pad_data, var.values], axis=time_dim_idx)
                    ds[var_name] = (dims, padded_data)
                else:
                    # 如果变量的时间维度大于目标长度，从后往前截取
                    start_idx = var.sizes['time'] - target_length
                    ds[var_name] = var.isel(time=slice(start_idx, var.sizes['time']))
        
        # 更新时间维度
        ds['time'] = ('time', new_times)
        
        # 合并扩展数据集（如果有）
        if event_id in all_train_events and 'aug_ds' in locals():
            # 合并训练集和8月份扩展数据
            ds = xr.concat([ds, aug_ds], dim='time')
            print(f"  ℹ 已合并训练集和8月份扩展数据，总时段数: {len(ds.time)}")
        elif event_id in all_val_events and 'jul_ds' in locals():
            # 合并验证集和7月份扩展数据
            ds = xr.concat([jul_ds, ds], dim='time')
            print(f"  ℹ 已合并验证集和7月份扩展数据，总时段数: {len(ds.time)}")
        
        # 保存修改后的文件
        output_file = os.path.join(output_folder, filename)
        ds.to_netcdf(output_file)
        print(f"  ✓ 已保存到: {output_file}")
        processed_count += 1
    
    print("\n处理完成统计:")
    print(f"  - 成功处理: {processed_count} 个文件")
    print(f"  - 处理失败: {error_count} 个文件")
    print(f"  - 总文件数: {len(nc_files)} 个文件")
    
    # 如果有处理失败的文件，打印详细信息
    if error_files:
        print("\n处理失败的文件详情:")
        for i, error_file in enumerate(error_files):
            print(f"  {i+1}. {error_file['filename']}: {error_file['error']}")


if __name__ == "__main__":
    # 指定文件夹路径
    data_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H"
    
    # 1. 运行划分函数
    print("=" * 50)
    print("开始划分训练集和验证集...")
    print("=" * 50)
    train_sets, val_sets = identify_train_val_sets(data_folder)
    
    # 打印总体统计信息
    print("\n" + "=" * 20 + " 总体统计 " + "=" * 20)
    print(f"总流域数: {len(train_sets)}")
    print(f"总训练场次: {sum(len(samples) for samples in train_sets.values())}")
    print(f"总验证场次: {sum(len(samples) for samples in val_sets.values())}")
    
    # 2. 处理文件
    print("\n" + "=" * 50)
    print("开始处理NC文件...")
    print("=" * 50)
    process_nc_files(data_folder, output_folder)