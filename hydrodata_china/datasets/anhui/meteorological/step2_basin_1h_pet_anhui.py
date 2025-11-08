
"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-06-09 11:06:49
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-21 22:25:06
"""


import os
import pandas as pd
import numpy as np
import calendar
from datetime import datetime, timedelta


# File paths
STATION_MAPPING_XLSX = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_Station_21\流域蒸发站对应表.xlsx"
EVAP_DIR = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_Station_21"
PET_MONTHLY_XLSX = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\PET_Station_21\多年平均月蒸散发.xlsx"
OUTPUT_DIR = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_anhui-PET"
# Parameter settings
START_YEAR = 1960
END_YEAR = 2022


def load_basin_station_mapping():
    """Load basin to evaporation station mapping."""
    print("---Loading station-basin mapping---")
    mapping_df = pd.read_excel(STATION_MAPPING_XLSX, sheet_name=1)
    id_col = "流域ID" if "流域ID" in mapping_df.columns else mapping_df.columns[0]
    station_col = "对应蒸发站" if "对应蒸发站" in mapping_df.columns else mapping_df.columns[1]
    basin_to_station = dict(zip(mapping_df[id_col], mapping_df[station_col]))
    return basin_to_station


def load_station_evap_data():
    """Load all station evaporation data."""
    print("---Loading station evaporation data---")
    evap_files = [f for f in os.listdir(EVAP_DIR) if f.endswith("_蒸发.xlsx")]
    station_data = {}
    for file in evap_files:
        file_path = os.path.join(EVAP_DIR, file)
        df = pd.read_excel(file_path)
        df = df[["站名", "站码", "时间", "水面蒸发量"]]
        df["时间"] = pd.to_datetime(df["时间"])
        station_name = df["站名"].iloc[0]
        station_data[station_name] = df
    return station_data


def generate_monthly_pet_hourly(basin_to_station):
    """Generate hourly PET from multi-year average monthly evaporation."""
    print("---Loading multi-year average monthly evaporation---")
    pet_xls = pd.ExcelFile(PET_MONTHLY_XLSX)
    monthly_pet_hourly = {}
    for basin_id, station_name in basin_to_station.items():
        sheet_name = f"{station_name}蒸发站"
        try:
            df = pet_xls.parse(sheet_name)
            avg_row = df[df["年"] == "多年平均"].iloc[0]
            monthly_pet = [avg_row[f"{i}月"] for i in range(1, 13)]
            records = {}
            for year in range(START_YEAR, END_YEAR+1):
                for month, pet in enumerate(monthly_pet, 1):
                    days = calendar.monthrange(year, month)[1]
                    hours = days * 24
                    pet_per_hour = pet / hours
                    for h in range(hours):
                        time = datetime(year, month, 1) + timedelta(hours=h)
                        records[time] = pet_per_hour
            monthly_pet_hourly[basin_id] = records
        except Exception:
            print(f"Warning: Cannot read average PET for basin {basin_id}, station {station_name}")
            monthly_pet_hourly[basin_id] = {}
    return monthly_pet_hourly


def process_basin_pet(basin_id, station_name, station_data, monthly_pet_hourly):
    """Process PET for a single basin and save to CSV."""
    print(f"Processing basin {basin_id} (station: {station_name})...")
    date_range = pd.date_range(start=f"{START_YEAR}-01-01 00:00:00", end=f"{END_YEAR}-12-31 23:00:00", freq="h")
    merged = pd.DataFrame({"时间": date_range})
    hourly_values = pd.Series(np.nan, index=date_range)
    if station_name in station_data:
        df = station_data[station_name]
        df = df.loc[(df["时间"].dt.year >= START_YEAR) & (df["时间"].dt.year <= END_YEAR)].copy()
        for _, row in df.iterrows():
            day = row["时间"]
            value = row["水面蒸发量"]
            for h in range(24):
                hour = day + timedelta(hours=h)
                if hour in hourly_values.index:
                    hourly_values[hour] = value / 24
    merged["水面蒸发量"] = hourly_values.values
    hourly_pet_dict = monthly_pet_hourly.get(basin_id, {})
    merged["补充PET"] = merged["时间"].map(lambda t: hourly_pet_dict.get(t, np.nan))
    merged["PET"] = merged["水面蒸发量"].combine_first(merged["补充PET"])
    merged_out = merged.rename(columns={"时间": "time", "PET": "pet_anhui"})
    output_file = os.path.join(OUTPUT_DIR, f"{basin_id}_PET_Anhui.csv")
    merged_out[["time", "pet_anhui"]].to_csv(output_file, index=False, encoding="utf-8-sig")
    print(f"PET data for basin {basin_id} saved to {output_file}")


def main():
    basin_to_station = load_basin_station_mapping()
    station_data = load_station_evap_data()
    monthly_pet_hourly = generate_monthly_pet_hourly(basin_to_station)
    print("---Processing PET for all basins---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for basin_id, station_name in basin_to_station.items():
        process_basin_pet(basin_id, station_name, station_data, monthly_pet_hourly)
    print("---All basins processed!---")

if __name__ == "__main__":
    main()