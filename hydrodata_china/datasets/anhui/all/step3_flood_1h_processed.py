"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-08-22 10:19:44
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-23 22:16:15
"""

import os
import glob
import numpy as np
import csv

def identify_train_val_sets(folder_path, train_ratio=0.8, min_validation_samples=2):
    """
    识别每个流域的训练集和验证集场次
    """
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        print(f"错误: 在 {folder_path} 中未找到任何csv文件")
        return {}, {}
    basin_files = {}
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        parts = filename.split('_')
        if len(parts) >= 2:
            basin_id = parts[1]
            if basin_id.isdigit():
                if basin_id not in basin_files:
                    basin_files[basin_id] = []
                basin_files[basin_id].append(csv_file)
            else:
                print(f"警告: 从文件名 {filename} 中提取的流域ID {basin_id} 不是有效的数字标识")
        else:
            print(f"警告: 文件名 {filename} 格式不符合预期，无法提取流域ID")
    print(f"\n找到 {len(basin_files)} 个流域:")
    for basin_id, files in basin_files.items():
        print(f"  - 流域 {basin_id}: {len(files)} 个文件")
    train_sets = {}
    val_sets = {}
    for basin_id, files in basin_files.items():
        if not files:
            continue
        files.sort(key=lambda x: os.path.basename(x).split('_')[2].split('.')[0])
        total_count = len(files)
        val_count = max(min_validation_samples, int(total_count * (1 - train_ratio)))
        train_count = total_count - val_count
        if train_count <= 0:
            print(f"警告: 流域 {basin_id} 的场次数 ({total_count}) 不足以进行划分，至少需要 {min_validation_samples+1} 个场次")
            continue
        train_files = files[:train_count]
        val_files = files[train_count:]
        train_sets[basin_id] = [os.path.basename(f).split('.')[0] for f in train_files]
        val_sets[basin_id] = [os.path.basename(f).split('.')[0] for f in val_files]
        print(f"\n流域 {basin_id} 划分结果:")
        print(f"  - 总场次数: {total_count}")
        print(f"  - 训练集: {train_count} 场次")
        print(f"  - 验证集: {val_count} 场次")
    return train_sets, val_sets

def export_sets_to_csv(train_sets, val_sets, output_folder):
    """
    将训练集和验证集的ID导出到CSV文件
    """
    os.makedirs(output_folder, exist_ok=True)
    train_csv_path = os.path.join(output_folder, "train_sets.csv")
    val_csv_path = os.path.join(output_folder, "validation_sets.csv")
    with open(train_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['basin'])
        for basin_id, events in train_sets.items():
            for event_id in events:
                if event_id.startswith(f"Anhui_{basin_id}"):
                    formatted_id = event_id
                else:
                    formatted_id = f"Anhui_{basin_id}_{event_id}"
                writer.writerow([formatted_id])
    with open(val_csv_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['basin'])
        for basin_id, events in val_sets.items():
            for event_id in events:
                if event_id.startswith(f"Anhui_{basin_id}"):
                    formatted_id = event_id
                else:
                    formatted_id = f"Anhui_{basin_id}_{event_id}"
                writer.writerow([formatted_id])
    print(f"\n训练集ID已导出到: {train_csv_path}")
    print(f"验证集ID已导出到: {val_csv_path}")
    return train_csv_path, val_csv_path

def read_csv_data(file_path):
    """
    读取csv文件，返回表头和数据（二维列表）
    """
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        header = next(reader)
        data = [row for row in reader]
    return header, data

def write_csv_data(file_path, header, data):
    """
    写入csv文件
    """
    with open(file_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for row in data:
            writer.writerow(row)

def process_csv_files(input_folder, output_folder):
    """
    处理CSV文件：统一输出744个时段，补齐缺失时段，训练集补8月，验证集补7月
    """
    os.makedirs(output_folder, exist_ok=True)
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    if not csv_files:
        print(f"错误: 在 {input_folder} 中未找到任何csv文件")
        return
    print(f"共找到 {len(csv_files)} 个csv文件待处理")
    processed_count = 0
    error_count = 0
    error_files = []
    train_sets, val_sets = identify_train_val_sets(input_folder)
    all_train_events = []
    all_val_events = []
    for basin_id in train_sets:
        all_train_events.extend(train_sets[basin_id])
    for basin_id in val_sets:
        all_val_events.extend(val_sets[basin_id])
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        event_id = filename.split('.')[0]
        try:
            header, data = read_csv_data(csv_file)
            times = [row[1] for row in data]
            original_length = len(times)
            target_length = 744
            # 记录原始时间序列
            original_times = times.copy()
            # 补齐时段
            if original_length > target_length:
                data = data[-target_length:]
                times = times[-target_length:]
                original_times = original_times[-target_length:]
            elif original_length < target_length:
                padding_length = target_length - original_length
                first_row = data[0]
                # 补齐时间（假定时间为字符串，间隔1小时）
                if original_length > 1:
                    t0 = np.datetime64(times[0])
                    t1 = np.datetime64(times[1])
                    time_diff = t1 - t0
                else:
                    time_diff = np.timedelta64(1, 'h')
                new_times = [str(np.datetime64(times[0]) - (i+1)*time_diff) for i in range(padding_length)]
                new_times.reverse()
                pad_rows = [first_row.copy() for _ in range(padding_length)]
                for i, row in enumerate(pad_rows):
                    row[1] = new_times[i]
                data = pad_rows + data
                times = new_times + times
                original_times = new_times + original_times
            # 训练/验证集补充
            new_data = []
            # 添加 time_true 列
            if 'time_true' not in header:
                header.append('time_true')
            if event_id in all_train_events:
                # 训练集时间范围：2024-07-01 ~ 2024-07-31
                start_time = np.datetime64('2024-07-01T00:00:00')
                new_times = [str(start_time + np.timedelta64(i, 'h')).replace('T', ' ') for i in range(target_length)]
                for i, row in enumerate(data):
                    row[1] = new_times[i]
                    # time_true为原始时间（补齐后）
                    if len(row) == len(header)-1:
                        row.append(original_times[i].replace('T', ' '))
                # 补充8月
                aug_hours = 31 * 24
                aug_start_time = np.datetime64('2024-08-01T00:00:00')
                aug_times = [str(aug_start_time + np.timedelta64(i, 'h')).replace('T', ' ') for i in range(aug_hours)]
                aug_rows = [[row[0], t] + ["" for _ in header[2:-1]] + [""] for t in aug_times]
                new_data = data + aug_rows
            elif event_id in all_val_events:
                # 验证集时间范围：2024-08-01 ~ 2024-08-31
                start_time = np.datetime64('2024-08-01T00:00:00')
                new_times = [str(start_time + np.timedelta64(i, 'h')).replace('T', ' ') for i in range(target_length)]
                for i, row in enumerate(data):
                    row[1] = new_times[i]
                    if len(row) == len(header)-1:
                        row.append(original_times[i].replace('T', ' '))
                # 补充7月
                jul_hours = 31 * 24
                jul_start_time = np.datetime64('2024-07-01T00:00:00')
                jul_times = [str(jul_start_time + np.timedelta64(i, 'h')).replace('T', ' ') for i in range(jul_hours)]
                jul_rows = [[row[0], t] + ["" for _ in header[2:-1]] + [""] for t in jul_times]
                new_data = jul_rows + data
            else:
                # 非训练/验证集，保持原始时间
                for i, row in enumerate(data):
                    if len(row) == len(header)-1:
                        row.append(original_times[i].replace('T', ' '))
                new_data = data
            # 输出
            output_file = os.path.join(output_folder, filename)
            write_csv_data(output_file, header, new_data)
            processed_count += 1
        except Exception as e:
            error_count += 1
            error_files.append({'filename': filename, 'error': str(e)})
    print("\n处理完成统计:")
    print(f"  - 成功处理: {processed_count} 个文件")
    print(f"  - 处理失败: {error_count} 个文件")
    print(f"  - 总文件数: {len(csv_files)} 个文件")
    if error_files:
        print("\n处理失败的文件详情:")
        for i, error_file in enumerate(error_files):
            print(f"  {i+1}. {error_file['filename']}: {error_file['error']}")

if __name__ == "__main__":
    data_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Flood_Selected"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui_1H_Flood_Selected"
    print("=" * 50)
    print("开始划分训练集和验证集...")
    print("=" * 50)
    train_sets, val_sets = identify_train_val_sets(data_folder)
    print("\n" + "=" * 20 + " 总体统计 " + "=" * 20)
    print(f"总流域数: {len(train_sets)}")
    print(f"总训练场次: {sum(len(samples) for samples in train_sets.values())}")
    print(f"总验证场次: {sum(len(samples) for samples in val_sets.values())}")
    print("\n" + "=" * 50)
    print("导出训练集和验证集ID到CSV文件...")
    print("=" * 50)
    export_sets_to_csv(train_sets, val_sets, output_folder)
    print("\n" + "=" * 50)
    print("开始处理CSV文件...")
    print("=" * 50)
    process_csv_files(data_folder, output_folder)