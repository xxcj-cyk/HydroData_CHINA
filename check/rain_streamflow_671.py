import os
import xarray as xr
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties

# 常量定义
DATA_DIR = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\test"
OUTPUT_DIR = r"E:\Takusan_no_Code\Dataset\Processed_Dataset\test\output"
os.makedirs(OUTPUT_DIR, exist_ok=True)  # 确保主输出目录存在

EVENT_VAR = "flood_event"     # 洪水事件标记变量
RAIN_VAR = "p_anhui"          # 降雨变量名
PRED_VAR = "streamflow"       # 预报径流
TIME_VAR = "time_ture"             # 时间变量名

# 解决字体缺失问题（使用支持上标的字体）
font_path = "C:/Windows/Fonts/msyh.ttc"  # 微软雅黑，支持更多符号
my_font = FontProperties(fname=font_path)

def plot_event(time, rain, pred, save_dir, basin_name, event_id):
    """绘制单场洪水事件的降雨径流图（无实测径流）"""
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # 绘制降雨柱状图（倒置）
    ax1.bar(time, rain, width=0.03, color='skyblue', label="降雨 (mm/h)", alpha=0.6)
    ax1.set_ylabel("降雨 (mm/h)", fontproperties=my_font)
    ax1.invert_yaxis()

    # 绘制预测径流曲线图
    ax2 = ax1.twinx()
    ax2.plot(time, pred, label="预测径流", color='red', linestyle='--')
    ax2.set_ylabel("径流 (m³/s)", fontproperties=my_font)

    # 设置标题和图例
    plt.title(f"{basin_name} - 第 {event_id} 场洪水", fontproperties=my_font)
    ax2.legend(loc="upper right", prop=my_font)
    fig.autofmt_xdate()

    # 保存图像到对应文件的子文件夹
    out_path = os.path.join(save_dir, f"event_{event_id}.png")
    plt.savefig(out_path, dpi=300)
    plt.close()

def process_file(file_path):
    """处理单个NetCDF文件，提取事件并保存到单独文件夹"""
    try:
        # 打开数据集
        ds = xr.open_dataset(file_path)
        basin_name = os.path.splitext(os.path.basename(file_path))[0]
        print(f"处理文件: {basin_name}")

        # 创建该文件专属的子文件夹
        save_dir = os.path.join(OUTPUT_DIR, basin_name)
        os.makedirs(save_dir, exist_ok=True)

        # 获取变量数据
        flood_event = ds[EVENT_VAR].values
        rain = ds[RAIN_VAR].values
        pred = ds[PRED_VAR].values
        time = ds[TIME_VAR].values

        # 打印维度信息
        print(f"  事件数据维度: {flood_event.shape}")

        # 验证二维结构
        if flood_event.ndim != 2:
            raise ValueError(f"flood_event应为二维数组，当前维度: {flood_event.ndim}")
        
        num_events, time_length = flood_event.shape

        # 处理降雨/径流数据（确保是一维时间序列）
        if rain.ndim == 2:
            rain = rain[0, :] if rain.shape[0] == 1 else rain[0, :]
        if pred.ndim == 2:
            pred = pred[0, :] if pred.shape[0] == 1 else pred[0, :]

        # 遍历所有事件
        file_event_count = 0
        for event_idx in range(num_events):
            event_id = event_idx + 1  # 事件编号从1开始
            
            # 提取当前事件的时间序列
            event_series = flood_event[event_idx, :]
            event_series = np.nan_to_num(event_series, nan=0).astype(int)
            
            # 找出事件中值为1的时间点
            event_mask = event_series == 1
            if not np.any(event_mask):
                continue  # 跳过无数据的事件
            
            # 确定事件的时间范围
            event_indices = np.where(event_mask)[0]
            start_idx = event_indices[0]
            end_idx = event_indices[-1] + 1

            # 绘制当前事件
            plot_event(
                time[start_idx:end_idx],
                rain[start_idx:end_idx],
                pred[start_idx:end_idx],
                save_dir,
                basin_name,
                event_id
            )
            file_event_count += 1

        print(f"  生成 {file_event_count} 个事件图")
        return file_event_count  # 返回该文件的事件数用于总统计

    except Exception as e:
        print(f"[错误] 处理文件 {file_path} 时出错: {str(e)}")
        return 0
    finally:
        if 'ds' in locals():
            ds.close()

def process_all_files():
    """处理所有文件并统计总事件数"""
    files = [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.endswith(".nc")]
    print(f"共发现 {len(files)} 个NetCDF文件\n")
    
    total_events = 0  # 总事件数统计
    
    for file_path in tqdm(files, desc="处理进度"):
        total_events += process_file(file_path)
    
    # 最终只统计总事件数
    print(f"\n===== 处理完成 =====")
    print(f"总事件图数量: {total_events}")
    print(f"所有图表已保存至: {OUTPUT_DIR}")

if __name__ == "__main__":
    process_all_files()