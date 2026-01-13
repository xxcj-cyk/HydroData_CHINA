"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-01-XX XX:XX:XX
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-01-XX XX:XX:XX
@Description:        处理空值：根据数据特征采用不同的填充策略
"""

import os
import glob
import pandas as pd
import numpy as np
import logging
from collections import defaultdict

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def identify_train_val_sets(folder_path):
    """
    识别每个流域的训练集和验证集场次（从step3的逻辑）
    返回训练集和验证集的event_id列表
    """
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if not csv_files:
        logging.warning(f"在 {folder_path} 中未找到任何csv文件")
        return set(), set()
    
    basin_files = defaultdict(list)
    for csv_file in csv_files:
        filename = os.path.basename(csv_file)
        parts = filename.split('_')
        if len(parts) >= 2:
            basin_id = parts[1]
            if basin_id.isdigit():
                basin_files[basin_id].append(csv_file)
    
    train_events = set()
    val_events = set()
    
    for basin_id, files in basin_files.items():
        if not files:
            continue
        files.sort(key=lambda x: os.path.basename(x).split('_')[2].split('.')[0] if len(os.path.basename(x).split('_')) >= 3 else '')
        total_count = len(files)
        min_validation_samples = 2
        train_ratio = 0.8
        val_count = max(min_validation_samples, int(total_count * (1 - train_ratio)))
        train_count = total_count - val_count
        
        if train_count <= 0:
            continue
        
        train_files = files[:train_count]
        val_files = files[train_count:]
        
        for f in train_files:
            event_id = os.path.basename(f).split('.')[0]
            train_events.add(event_id)
        for f in val_files:
            event_id = os.path.basename(f).split('.')[0]
            val_events.add(event_id)
    
    return train_events, val_events


def fill_missing_values_in_file(file_path, train_events, val_events, output_folder):
    """
    处理单个文件中的空值
    
    参数:
        file_path: CSV文件路径
        train_events: 训练集event_id集合
        val_events: 验证集event_id集合
        output_folder: 输出文件夹路径
    
    返回:
        dict: 处理结果统计
    """
    filename = os.path.basename(file_path)
    event_id = filename.replace('.csv', '')
    
    try:
        df = pd.read_csv(file_path)
        original_shape = df.shape
        
        # 确保time列是datetime类型
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        else:
            logging.warning(f"文件 {filename} 中没有time列，跳过")
            return None
        
        # 判断是训练集还是验证集
        is_train = event_id in train_events
        is_val = event_id in val_events
        
        if not is_train and not is_val:
            # 根据数据中7月和8月的行数来判断
            month7_count = len(df[df['time'].dt.month == 7])
            month8_count = len(df[df['time'].dt.month == 8])
            if month7_count > month8_count:
                is_train = True
            elif month8_count > month7_count:
                is_val = True
            else:
                logging.warning(f"文件 {filename} 无法判断是训练集还是验证集，跳过")
                return None
        
        # 根据训练/验证集筛选时间（只处理7月或8月的数据）
        if is_train:
            df_filtered = df[df['time'].dt.month == 7].copy()
            period_type = "训练期(7月)"
        else:
            df_filtered = df[df['time'].dt.month == 8].copy()
            period_type = "验证期(8月)"
        
        if len(df_filtered) == 0:
            logging.warning(f"文件 {filename} 在{period_type}中没有数据，跳过")
            return None
        
        # 统计处理前的空值
        stats_before = {}
        target_columns = ['streamflow_obs_mm', 'streamflow_obs_m3s']
        for col in target_columns:
            if col in df_filtered.columns:
                stats_before[col] = {
                    'missing_count': int(df_filtered[col].isna().sum())
                }
        
        # ========== 处理策略 ==========
        
        # 1. streamflow_obs_mm 的处理
        if 'streamflow_obs_mm' in df_filtered.columns:
            col = 'streamflow_obs_mm'
            missing_mask = df_filtered[col].isna()
            
            if missing_mask.sum() > 0:
                # 检查是否全部为空值
                if missing_mask.sum() == len(df_filtered):
                    # 全部为空，用0填充（可能是无流量）
                    df_filtered.loc[missing_mask, col] = 0
                    logging.info(f"  {col}: 全部为空值，用0填充")
                else:
                    # 部分为空值，优先使用线性插值
                    # 如果插值失败（如开头或结尾全是空值），则用0填充
                    df_filtered[col] = df_filtered[col].interpolate(method='linear', limit_direction='both')
                    
                    # 检查是否还有空值（可能是开头或结尾全是空值）
                    remaining_missing = df_filtered[col].isna().sum()
                    if remaining_missing > 0:
                        # 用前向填充和后向填充
                        df_filtered[col] = df_filtered[col].ffill().bfill()
                        
                        # 如果还有空值，用0填充
                        if df_filtered[col].isna().sum() > 0:
                            df_filtered[col] = df_filtered[col].fillna(0)
                            logging.info(f"  {col}: 插值后仍有空值，用0填充")
        
        # 2. streamflow_obs_m3s 的处理（与 streamflow_obs_mm 相同的策略）
        if 'streamflow_obs_m3s' in df_filtered.columns:
            col = 'streamflow_obs_m3s'
            missing_mask = df_filtered[col].isna()
            
            if missing_mask.sum() > 0:
                # 检查是否全部为空值
                if missing_mask.sum() == len(df_filtered):
                    # 全部为空，用0填充（可能是无流量）
                    df_filtered.loc[missing_mask, col] = 0
                    logging.info(f"  {col}: 全部为空值，用0填充")
                else:
                    # 部分为空值，优先使用线性插值
                    # 如果插值失败（如开头或结尾全是空值），则用0填充
                    df_filtered[col] = df_filtered[col].interpolate(method='linear', limit_direction='both')
                    
                    # 检查是否还有空值（可能是开头或结尾全是空值）
                    remaining_missing = df_filtered[col].isna().sum()
                    if remaining_missing > 0:
                        # 用前向填充和后向填充
                        df_filtered[col] = df_filtered[col].ffill().bfill()
                        
                        # 如果还有空值，用0填充
                        if df_filtered[col].isna().sum() > 0:
                            df_filtered[col] = df_filtered[col].fillna(0)
                            logging.info(f"  {col}: 插值后仍有空值，用0填充")
        
        # 统计处理后的空值
        stats_after = {}
        for col in target_columns:
            if col in df_filtered.columns:
                stats_after[col] = {
                    'missing_count': int(df_filtered[col].isna().sum())
                }
        
        # 将处理后的数据合并回原始DataFrame
        # 更新原始df中对应月份的数据
        if is_train:
            month_mask = df['time'].dt.month == 7
        else:
            month_mask = df['time'].dt.month == 8
        
        # 更新对应月份的数据
        for col in df_filtered.columns:
            if col in df.columns:
                df.loc[month_mask, col] = df_filtered[col].values
        
        # 保存处理后的文件
        output_file = os.path.join(output_folder, filename)
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        # 返回统计信息
        result = {
            'filename': filename,
            'event_id': event_id,
            'period_type': period_type,
            'stats_before': stats_before,
            'stats_after': stats_after,
            'filled': any(stats_after[col]['missing_count'] < stats_before[col]['missing_count'] 
                         for col in target_columns if col in stats_before and col in stats_after)
        }
        
        return result
        
    except Exception as e:
        logging.error(f"处理文件 {filename} 时出错: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return None


def fill_missing_values(input_folder, output_folder):
    """
    处理所有CSV文件中的空值
    
    参数:
        input_folder: 输入文件夹路径（step3的输出文件夹）
        output_folder: 输出文件夹路径
    """
    os.makedirs(output_folder, exist_ok=True)
    
    # 首先需要从step3的输入文件夹获取训练/验证集划分信息
    step3_input_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui16_612_1H"
    train_events, val_events = identify_train_val_sets(step3_input_folder)
    
    logging.info(f"识别到 {len(train_events)} 个训练集事件和 {len(val_events)} 个验证集事件")
    
    # 获取所有CSV文件
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    if not csv_files:
        logging.error(f"在 {input_folder} 中未找到任何csv文件")
        return
    
    logging.info(f"共找到 {len(csv_files)} 个csv文件待处理")
    
    # 处理每个文件
    all_results = []
    for csv_file in csv_files:
        result = fill_missing_values_in_file(csv_file, train_events, val_events, output_folder)
        if result:
            all_results.append(result)
    
    # 汇总统计
    logging.info("\n" + "="*80)
    logging.info("空值处理结果汇总")
    logging.info("="*80)
    
    total_files = len(all_results)
    files_filled = sum(1 for r in all_results if r['filled'])
    
    logging.info(f"处理的文件总数: {total_files}")
    logging.info(f"进行了空值填充的文件数: {files_filled}")
    logging.info(f"无需填充的文件数: {total_files - files_filled}")
    
    # 统计各列的处理情况
    target_columns = ['streamflow_obs_mm', 'streamflow_obs_m3s']
    column_summary = {col: {'before_total': 0, 'after_total': 0, 'files_with_missing': 0} 
                      for col in target_columns}
    
    for result in all_results:
        for col in target_columns:
            if col in result['stats_before'] and col in result['stats_after']:
                before = result['stats_before'][col]['missing_count']
                after = result['stats_after'][col]['missing_count']
                column_summary[col]['before_total'] += before
                column_summary[col]['after_total'] += after
                if before > 0:
                    column_summary[col]['files_with_missing'] += 1
    
    logging.info("\n各列处理统计:")
    for col in target_columns:
        if col in column_summary:
            summary = column_summary[col]
            logging.info(f"  {col}:")
            logging.info(f"    处理前总空值数: {summary['before_total']}")
            logging.info(f"    处理后总空值数: {summary['after_total']}")
            logging.info(f"    填充的空值数: {summary['before_total'] - summary['after_total']}")
            logging.info(f"    存在空值的文件数: {summary['files_with_missing']}")
    
    logging.info("\n处理完成！所有文件已保存到: " + output_folder)


if __name__ == "__main__":
    input_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui16_612_1H_Standardized"
    output_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui16_612_1H_Filled"
    
    fill_missing_values(input_folder, output_folder)
    logging.info("空值处理完成！")
