"""
@Author:             Yikai CHAI
@Email:              chaiyikai@mail.dlut.edu.cn
@Company:            Dalian University of Technology
@Date:               2025-08-19 09:26:13
@Last Modified by:   Yikai CHAI
@Last Modified time: 2025-08-22 11:42:41
"""


import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
from hydrodata_china.settings.rainfall_methods import arithmetic_mean, thiessen_polygon_mean, inverse_distance_weighting


# File paths
STATION_SHP = r"E:\GIS_Data\AnHui\Management\Precipitation_Station\Anhui_PST_93.shp"
BASIN_SHP = r"E:\GIS_Data\AnHui\Basin\Anhui_Basins_21.shp"
RAINFALL_FOLDER = r"E:\Takusan_no_Code\Dataset\Original_Dataset\Dataset_CHINA\Anhui\P_Station_21"
OUTPUT_FOLDER_BASE = r"E:\Takusan_no_Code\Dataset\Interim_Dataset\Dataset_CHINA\Anhui_1H_Pmean"
# Parameter settings
BUFFER_DISTANCE = 2000 # Buffer distance around basin (meters)
PROJECTED_CRS = "EPSG:32650"
RAIN_MEAN_METHOD = "arithmetic" # Rainfall interpolation method: "arithmetic", "thiessen", or "idw"


def collect_stations_in_buffer(stations_gdf, basins_gdf, buffer_distance):
    """
    For each basin, find all precipitation stations located within the basin and its buffer zone.
    Returns a DataFrame of stations in buffer zones and a mapping from basin ID to station codes.
    Args:
        stations_gdf (GeoDataFrame): Precipitation stations.
        basins_gdf (GeoDataFrame): Basin polygons.
        buffer_distance (float): Buffer distance in meters.
    Returns:
        all_stations_in_buffer (DataFrame): Basin-station pairs in buffer.
        basin_station_map (dict): Basin ID to list of station codes.
    """
    all_stations_in_buffer = pd.DataFrame(columns=["Basin_ID", "STCD"])
    basin_station_map = {}
    print("\n---Start processing all basins---")
    for _, basin in basins_gdf.iterrows():
        basin_id = basin["Basin_ID"]
        print(f"\n---Processing basin: {basin_id}---")
        basin_geom = basin.geometry
        basin_buffer = basin_geom.buffer(buffer_distance)
        # Find stations within buffer zone
        stations_in_basin_buffer = stations_gdf[stations_gdf.geometry.within(basin_buffer)]
        # Find stations strictly within basin
        stations_in_basin = stations_gdf[stations_gdf.geometry.within(basin_geom)]
        # Stations only in buffer (not in basin)
        stations_only_in_buffer = stations_in_basin_buffer[~stations_in_basin_buffer.index.isin(stations_in_basin.index)]
        temp_df = stations_in_basin_buffer[["STCD"]].copy()
        temp_df["Basin_ID"] = basin_id
        all_stations_in_buffer = pd.concat([all_stations_in_buffer, temp_df])
        print(f"Number of stations in basin {basin_id} and buffer: {len(stations_in_basin_buffer)}")
        if len(stations_only_in_buffer) > 0:
            for _, station in stations_only_in_buffer.iterrows():
                distance = basin_geom.distance(station.geometry)
                print(f"Station {station['STCD']} in buffer zone, distance to basin {basin_id}: {distance:.2f} m")
        else:
            print("No stations in buffer zone (excluding basin itself).")
        basin_station_map[basin_id] = stations_in_basin_buffer["STCD"].astype(str).tolist()
    return all_stations_in_buffer, basin_station_map



def process_rainfall_for_basin(basin_id, station_codes, rainfall_folder, output_folder):
    """
    For a given basin, read rainfall data from all associated stations, align time series,
    and calculate areal mean rainfall using the selected interpolation method.
    Outputs a CSV file with hourly rainfall for each station and the basin mean.
    Args:
        basin_id (str): Basin identifier.
        station_codes (list): List of station codes in basin/buffer.
        rainfall_folder (str): Folder containing station rainfall files.
        output_folder (str): Output folder for results.
    """
    station_dfs = {}
    min_tm, max_tm = None, None
    for stcd in station_codes:
        # Find rainfall file for each station
        matched_files = [f for f in os.listdir(rainfall_folder) if f"{stcd}-1h_processed.xlsx" in f]
        if not matched_files:
            print(f"Rainfall file not found for station {stcd}")
            continue
        file_path = os.path.join(rainfall_folder, matched_files[0])
        df = pd.read_excel(file_path)
        df["TM"] = pd.to_datetime(df["TM"])
        # Group by time, average duplicate records
        df = df.groupby("TM", as_index=False)["DRP"].mean()
        station_dfs[stcd] = df.set_index("TM")["DRP"]
        tms = df["TM"]
        # Track overall time range
        if min_tm is None or tms.min() < min_tm:
            min_tm = tms.min()
        if max_tm is None or tms.max() > max_tm:
            max_tm = tms.max()

    if station_dfs and min_tm is not None and max_tm is not None:
        # Create output folder if needed
        os.makedirs(output_folder, exist_ok=True)
    # Build full hourly time index based on min_tm and max_tm
        full_tm = pd.date_range(start=min_tm, end=max_tm, freq="h")
        result_df = pd.DataFrame({"TM": full_tm})
        # Add rainfall series for each station
        for stcd, drp_series in station_dfs.items():
            drp_series.index = pd.to_datetime(drp_series.index)
            result_df[stcd] = result_df["TM"].map(drp_series)

        # Calculate areal mean rainfall using selected method
        if RAIN_MEAN_METHOD == "arithmetic":
            result_df["P_mean"] = result_df.drop(columns=["TM"]).apply(
                lambda row: arithmetic_mean([val for val in row if not pd.isna(val)]), axis=1
            )
        elif RAIN_MEAN_METHOD == "thiessen":
            result_df["P_mean"] = result_df.drop(columns=["TM"]).apply(
                lambda row: thiessen_polygon_mean([val for val in row if not pd.isna(val)]), axis=1
            )
        elif RAIN_MEAN_METHOD == "idw":
            result_df["P_mean"] = result_df.drop(columns=["TM"]).apply(
                lambda row: inverse_distance_weighting([val for val in row if not pd.isna(val)]), axis=1
            )
        else:
            raise ValueError(f"Unknown rainfall interpolation method: {RAIN_MEAN_METHOD}")

        station_columns = [col for col in result_df.columns if col not in ["TM", "P_mean"]]
        new_columns = ["time", "p_anhui"] + [f"p_{col}" for col in station_columns]
        result_df = result_df[["TM", "P_mean"] + station_columns]
        result_df.columns = new_columns
        result_df["time"] = pd.to_datetime(result_df["time"])
        out_csv = os.path.join(output_folder, f"{basin_id}_Pmean_Anhui.csv")
        result_df.to_csv(out_csv, index=False)
        print(f"Rainfall data for basin {basin_id} saved to {out_csv}")
    else:
        print(f"No available rainfall data for basin {basin_id}")


def main():
    """
    Main workflow:
        1. Load GIS shapefiles for stations and basins.
        2. Project to specified CRS for spatial analysis.
        3. Find stations in each basin and buffer zone.
        4. For each basin, process rainfall data and calculate areal mean.
        5. Output results to CSV files.
    """
    print("---Loading GIS data...---")
    stations_gdf = gpd.read_file(STATION_SHP)
    basins_gdf = gpd.read_file(BASIN_SHP)
    stations_gdf = stations_gdf.to_crs(PROJECTED_CRS)
    basins_gdf = basins_gdf.to_crs(PROJECTED_CRS)
    print("---GIS data loaded, start processing...---")

    # Set output folder according to method
    if RAIN_MEAN_METHOD in ["arithmetic", "thiessen", "idw"]:
        output_folder = os.path.join(OUTPUT_FOLDER_BASE, RAIN_MEAN_METHOD)
        os.makedirs(output_folder, exist_ok=True)
    else:
        raise ValueError(f"Unknown rainfall interpolation method: {RAIN_MEAN_METHOD}")

    # Count stations in all basins and buffer zones
    all_stations_in_buffer, basin_station_map = collect_stations_in_buffer(
        stations_gdf, basins_gdf, BUFFER_DISTANCE)

    # Process rainfall data for each basin
    for basin_id, station_codes in tqdm(basin_station_map.items(), total=len(basin_station_map), desc="Rainfall interpolation progress"):
        process_rainfall_for_basin(
            basin_id,
            station_codes,
            RAINFALL_FOLDER,
            output_folder
        )

    # Output all basins and buffer zone station info
    out_name = os.path.join(output_folder, "Stations_in_Basins_and_Buffer.csv")
    all_stations_in_buffer = all_stations_in_buffer.drop_duplicates().sort_values(by=["Basin_ID", "STCD"])
    all_stations_in_buffer.to_csv(out_name, index=False)
    print(f"\nAll basins and buffer zone stations saved to {out_name}")
    print("\n---All basins processed!---")


if __name__ == "__main__":
    main()