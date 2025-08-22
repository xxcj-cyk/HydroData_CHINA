"""
@Author:				Yikai CHAI
@Email:					chaiyikai@mail.dlut.edu.cn
@Company:				Dalian University of Technology
@Date:					2025-08-21 23:54:23
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 00:02:06
"""


import os
import pandas as pd
from glob import glob

# 输入文件夹路径
Q_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Q'
Pmean_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Pmean\arithmetic'
PET_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_anhui-PET'
era5landPET_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_era5land-PET'
output_dir = r'E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H'

# 获取所有流域编码（Anhui_xxxxxxxx）
def get_basin_codes(folder, suffix):
	files = glob(os.path.join(folder, f'Anhui_*{suffix}'))
	codes = [os.path.basename(f).split('_')[1] for f in files]
	return set(codes)

codes_q = get_basin_codes(Q_dir, '_Q_Anhui.csv')
codes_p = get_basin_codes(Pmean_dir, '_Pmean_Anhui.csv')
codes_pet = get_basin_codes(PET_dir, '_PET_Anhui.csv')
codes_era5 = get_basin_codes(era5landPET_dir, '_PET_ERA5Land.csv')

all_codes = codes_q & codes_p & codes_pet & codes_era5
print(f'共找到{len(all_codes)}个流域编码')

# 生成完整时间序列（1960-01-01 00:00 到 2022-12-31 23:00）
time_index = pd.date_range('1960-01-01 00:00', '2022-12-31 23:00', freq='h')

def read_and_merge(code):
	# 构造文件路径
	file_q = os.path.join(Q_dir, f'Anhui_{code}_Q_Anhui.csv')
	file_p = os.path.join(Pmean_dir, f'Anhui_{code}_Pmean_Anhui.csv')
	file_pet = os.path.join(PET_dir, f'Anhui_{code}_PET_Anhui.csv')
	file_era5 = os.path.join(era5landPET_dir, f'Anhui_{code}_PET_ERA5Land.csv')

	# 读取数据并标准化时间标签
	def load_and_align(file):
		df = pd.read_csv(file)
		if 'time' in df.columns:
			df['time'] = pd.to_datetime(df['time'])
		else:
			df.index = pd.to_datetime(df.index)
			df['time'] = df.index
		value_cols = [col for col in df.columns if col != 'time']
		df = df[['time'] + value_cols]
		# 以time为索引，reindex到完整时间
		df = df.set_index('time').reindex(time_index)
		df['time'] = time_index
		return df.reset_index(drop=True)

	df_q = load_and_align(file_q)
	df_p = load_and_align(file_p)
	df_pet = load_and_align(file_pet)
	df_era5 = load_and_align(file_era5)

	# 合并所有数据，按time对齐
	df_merge = pd.DataFrame({'time': time_index})
	for df in [df_q, df_p, df_pet, df_era5]:
		df_merge = df_merge.merge(df, on='time', how='left')
	return df_merge

# 主循环
for code in all_codes:
	print(f'处理流域 {code}...')
	df = read_and_merge(code)
	out_file = os.path.join(output_dir, f'Anhui_{code}_1H.csv')
	df.to_csv(out_file, index=False, encoding='utf-8')
	print(f'已保存: {out_file}')

print('全部处理完成！')
