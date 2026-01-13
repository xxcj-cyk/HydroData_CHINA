"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-01-XX XX:XX:XX
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-01-XX XX:XX:XX
"""

import os
import glob
import pandas as pd
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


def extract_event_id_from_filename(filename):
    """
    从文件名中提取event_id
    step3的输出文件名格式: Anhui_XXXX_YYYYMMDD.csv
    """
    filename_no_ext = filename.replace('.csv', '')
    # 直接返回文件名（去掉扩展名）作为event_id
    return filename_no_ext


def check_missing_values_in_file(file_path, train_events, val_events, target_columns):
    """
    检查单个文件中的空值
    
    参数:
        file_path: CSV文件路径
        train_events: 训练集event_id集合
        val_events: 验证集event_id集合
        target_columns: 要检查的列名列表
    
    返回:
        dict: 包含检查结果的字典
    """
    filename = os.path.basename(file_path)
    
    try:
        df = pd.read_csv(file_path)
        
        # 确保time列是datetime类型
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'], errors='coerce')
        else:
            logging.warning(f"文件 {filename} 中没有time列")
            return None
        
        # 从文件名提取event_id
        event_id = extract_event_id_from_filename(filename)
        
        # 判断是训练集还是验证集
        is_train = event_id in train_events
        is_val = event_id in val_events
        
        # 如果无法从文件名判断，则根据数据中的月份来判断
        if not is_train and not is_val:
            # 根据数据中7月和8月的行数来判断
            month7_count = len(df[df['time'].dt.month == 7])
            month8_count = len(df[df['time'].dt.month == 8])
            if month7_count > month8_count:
                is_train = True
                is_val = False
            elif month8_count > month7_count:
                is_train = False
                is_val = True
            else:
                # 如果无法判断，跳过
                logging.warning(f"文件 {filename} 无法判断是训练集还是验证集，跳过")
                return None
        
        if not is_train and not is_val:
            logging.warning(f"文件 {filename} 既不在训练集也不在验证集中，跳过")
            return None
        
        # 根据训练/验证集筛选时间
        if is_train:
            # 训练期只看7月
            df_filtered = df[df['time'].dt.month == 7].copy()
            period_type = "训练期(7月)"
        else:
            # 验证期只看8月
            df_filtered = df[df['time'].dt.month == 8].copy()
            period_type = "验证期(8月)"
        
        if len(df_filtered) == 0:
            logging.warning(f"文件 {filename} 在{period_type}中没有数据")
            return None
        
        # 检查目标列是否存在
        missing_columns = [col for col in target_columns if col not in df_filtered.columns]
        if missing_columns:
            logging.warning(f"文件 {filename} 缺少列: {missing_columns}")
        
        available_columns = [col for col in target_columns if col in df_filtered.columns]
        if not available_columns:
            logging.warning(f"文件 {filename} 中没有可检查的列")
            return None
        
        # 统计空值（全部数据）
        result = {
            'filename': filename,
            'event_id': event_id,
            'period_type': period_type,
            'total_rows': len(df_filtered),
            'columns_checked': available_columns,
            'missing_info': {},
            'flood_event_missing_info': {}
        }
        
        for col in available_columns:
            missing_count = df_filtered[col].isna().sum()
            missing_pct = (missing_count / len(df_filtered)) * 100 if len(df_filtered) > 0 else 0
            result['missing_info'][col] = {
                'missing_count': int(missing_count),
                'missing_percentage': round(missing_pct, 2),
                'has_missing': missing_count > 0
            }
        
        # 检查flood_event=1时的空值
        if 'flood_event' in df_filtered.columns:
            # 创建副本用于转换flood_event列类型（避免影响原始数据）
            df_for_flood = df_filtered.copy()
            # 转换flood_event列为数值类型（处理可能的字符串类型）
            try:
                df_for_flood['flood_event'] = pd.to_numeric(df_for_flood['flood_event'], errors='coerce')
            except:
                pass
            # 筛选flood_event=1的数据
            df_flood_event = df_for_flood[df_for_flood['flood_event'] == 1].copy()
            result['flood_event_rows'] = len(df_flood_event)
            
            if len(df_flood_event) > 0:
                for col in available_columns:
                    flood_missing_count = df_flood_event[col].isna().sum()
                    flood_missing_pct = (flood_missing_count / len(df_flood_event)) * 100 if len(df_flood_event) > 0 else 0
                    result['flood_event_missing_info'][col] = {
                        'missing_count': int(flood_missing_count),
                        'missing_percentage': round(flood_missing_pct, 2),
                        'has_missing': flood_missing_count > 0
                    }
            else:
                # 如果没有flood_event=1的数据，记录为0
                for col in available_columns:
                    result['flood_event_missing_info'][col] = {
                        'missing_count': 0,
                        'missing_percentage': 0.0,
                        'has_missing': False
                    }
        else:
            # 如果没有flood_event列，记录为None
            result['flood_event_rows'] = None
            logging.warning(f"文件 {filename} 中没有flood_event列")
        
        return result
        
    except Exception as e:
        logging.error(f"处理文件 {filename} 时出错: {e}")
        return None


def check_missing_values(input_folder):
    """
    检查所有CSV文件中的空值
    
    参数:
        input_folder: 输入文件夹路径（step3的输出文件夹）
    """
    # 首先需要从step3的输入文件夹获取训练/验证集划分信息
    # 这里假设step3的输入文件夹路径
    step3_input_folder = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui16_612_1H"
    train_events, val_events = identify_train_val_sets(step3_input_folder)
    
    logging.info(f"识别到 {len(train_events)} 个训练集事件和 {len(val_events)} 个验证集事件")
    
    # 要检查的列（step3输出文件中的列名）
    target_columns = ['streamflow_obs_mm', 'streamflow_obs_m3s', 'p_anhui', 'pet_anhui']
    
    # 获取所有CSV文件
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    if not csv_files:
        logging.error(f"在 {input_folder} 中未找到任何csv文件")
        return
    
    logging.info(f"共找到 {len(csv_files)} 个csv文件待检查")
    
    # 检查每个文件
    all_results = []
    for csv_file in csv_files:
        result = check_missing_values_in_file(csv_file, train_events, val_events, target_columns)
        if result:
            all_results.append(result)
    
    # 汇总统计
    logging.info("\n" + "="*80)
    logging.info("空值检查结果汇总")
    logging.info("="*80)
    
    total_files = len(all_results)
    files_with_missing = 0
    files_with_flood_missing = 0
    column_summary = {col: {'total_missing': 0, 'files_with_missing': 0} for col in target_columns}
    flood_column_summary = {col: {'total_missing': 0, 'files_with_missing': 0} for col in target_columns}
    
    for result in all_results:
        has_any_missing = False
        has_flood_missing = False
        
        # 统计全部数据的空值
        for col, info in result['missing_info'].items():
            if info['has_missing']:
                has_any_missing = True
                column_summary[col]['total_missing'] += info['missing_count']
                column_summary[col]['files_with_missing'] += 1
        
        # 统计flood_event=1时的空值
        if result.get('flood_event_rows') is not None and result.get('flood_event_missing_info'):
            for col, info in result['flood_event_missing_info'].items():
                if info['has_missing']:
                    has_flood_missing = True
                    flood_column_summary[col]['total_missing'] += info['missing_count']
                    flood_column_summary[col]['files_with_missing'] += 1
        
        if has_any_missing or has_flood_missing:
            if has_any_missing:
                files_with_missing += 1
            if has_flood_missing:
                files_with_flood_missing += 1
            
            logging.info(f"\n文件: {result['filename']}")
            logging.info(f"  事件ID: {result['event_id']}")
            logging.info(f"  时期类型: {result['period_type']}")
            logging.info(f"  总行数: {result['total_rows']}")
            
            if has_any_missing:
                logging.info(f"  全部数据空值详情:")
                for col, info in result['missing_info'].items():
                    if info['has_missing']:
                        logging.info(f"    {col}: {info['missing_count']} 个空值 ({info['missing_percentage']}%)")
            
            if result.get('flood_event_rows') is not None:
                logging.info(f"  flood_event=1的行数: {result['flood_event_rows']}")
                if result.get('flood_event_missing_info'):
                    has_flood_missing_in_file = any(info['has_missing'] for info in result['flood_event_missing_info'].values())
                    if has_flood_missing_in_file:
                        logging.info(f"  flood_event=1时的空值详情:")
                        for col, info in result['flood_event_missing_info'].items():
                            if info['has_missing']:
                                logging.info(f"    {col}: {info['missing_count']} 个空值 ({info['missing_percentage']}%)")
    
    # 总体统计
    logging.info("\n" + "="*80)
    logging.info("总体统计")
    logging.info("="*80)
    logging.info(f"检查的文件总数: {total_files}")
    logging.info(f"存在空值的文件数: {files_with_missing}")
    logging.info(f"无空值的文件数: {total_files - files_with_missing}")
    logging.info(f"flood_event=1时存在空值的文件数: {files_with_flood_missing}")
    
    logging.info("\n各列空值统计（全部数据）:")
    for col in target_columns:
        if col in column_summary:
            summary = column_summary[col]
            logging.info(f"  {col}:")
            logging.info(f"    存在空值的文件数: {summary['files_with_missing']}")
            logging.info(f"    总空值数量: {summary['total_missing']}")
    
    logging.info("\n各列空值统计（flood_event=1时）:")
    for col in target_columns:
        if col in flood_column_summary:
            summary = flood_column_summary[col]
            logging.info(f"  {col}:")
            logging.info(f"    存在空值的文件数: {summary['files_with_missing']}")
            logging.info(f"    总空值数量: {summary['total_missing']}")


if __name__ == "__main__":
    input_folder = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\Dataset_CHINA\Anhui16_612_1H_Standardized"
    
    check_missing_values(input_folder)
    logging.info("检查完成！")
