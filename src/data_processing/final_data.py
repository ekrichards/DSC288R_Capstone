import os
import duckdb
import pandas as pd
import yaml
from tqdm import tqdm

CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]  # Add all your YAML files here

def load_yaml_files(file_paths):
    """Load multiple YAML files and merge their content."""
    merged_config = {}
    for path in file_paths:
        with open(path, "r") as file:
            config = yaml.safe_load(file)
            if config:
                merged_config.update(config)  # Merge dictionaries (override duplicate keys)
    return merged_config

# Load and merge configurations
config = load_yaml_files(CONFIG_FILES)

# Extract merge settings
FLIGHT_DIR = config["paths"]["processed_flight_data"]
WEATHER_DIR = config["paths"]["processed_noaa_data"]
FINAL_DIR = config["paths"]["final_by_year"]
FINAL_FILE = config["paths"]["final_combined"]
YEARS = config["overall"]["years"]
DELETE_SOURCE_FILES = config["final_data"]["delete_processed"]
DELETE_INTERMEDIATE_FILES = config["final_data"]["delete_merged"]

# Ensure output directory exists
os.makedirs(os.path.dirname(FINAL_FILE), exist_ok=True)

def merge_flight_weather(year):
    """Merge flight data with origin & destination weather for a given year."""
    flight_file = os.path.join(FLIGHT_DIR, f"processed_flight_{year}.parquet")
    weather_file = os.path.join(WEATHER_DIR, f"processed_noaa_{year}.parquet")

    if not os.path.exists(flight_file) or not os.path.exists(weather_file):
        tqdm.write(f"Skipping {year}: Missing flight or weather file.")
        return None

    tqdm.write(f"Merging {year} flight and weather data...")

    # Load flight and weather data into DuckDB
    con = duckdb.connect(database=":memory:")

    # Read Parquet files into DuckDB tables
    con.execute(f"CREATE TABLE flights AS SELECT * FROM read_parquet('{flight_file}')")
    con.execute(f"CREATE TABLE weather AS SELECT * FROM read_parquet('{weather_file}')")

    # Perform LEFT JOINs for Origin and Destination weather
    merged_df = con.execute("""
        SELECT 
            f.*, 
            w_origin.PRCP AS Origin_PRCP, 
            w_origin.SNOW AS Origin_SNOW, 
            w_origin.SNWD AS Origin_SNWD, 
            w_origin.TMAX AS Origin_TMAX, 
            w_origin.TMIN AS Origin_TMIN,
            w_dest.PRCP AS Dest_PRCP, 
            w_dest.SNOW AS Dest_SNOW, 
            w_dest.SNWD AS Dest_SNWD, 
            w_dest.TMAX AS Dest_TMAX, 
            w_dest.TMIN AS Dest_TMIN
        FROM flights f
        LEFT JOIN weather w_origin 
            ON f.Origin = w_origin.STATION 
            AND f.FlightDate = w_origin.DATE
        LEFT JOIN weather w_dest
            ON f.Dest = w_dest.STATION 
            AND f.FlightDate = w_dest.DATE
        WHERE w_origin.STATION IS NOT NULL  
              OR w_dest.STATION IS NOT NULL
    """).fetchdf()

    # Close DuckDB connection
    con.close()

    # Add code here

    # Save merged dataset
    merged_file = os.path.join(os.path.dirname(FINAL_FILE), f"final_{year}.parquet")
    merged_df.to_parquet(merged_file, index=False)

    # Delete source flight & weather files if enabled
    if DELETE_SOURCE_FILES:
        os.remove(flight_file)
        os.remove(weather_file)
        tqdm.write(f"Deleted source files for {year}")

    return merged_file

if __name__ == "__main__":
    tqdm.write("Starting flight and weather data merge...")

    merged_files = []

    for year in tqdm(YEARS, desc="Merging yearly data"):
        merged_file = merge_flight_weather(year)
        if merged_file:
            merged_files.append(merged_file)

    if merged_files:
        tqdm.write("Combining all merged datasets into a single Parquet file...")

        # Read all merged files and concatenate into one DataFrame
        df_combined = pd.concat([pd.read_parquet(f) for f in merged_files], ignore_index=True)

        # Save final merged dataset
        df_combined.to_parquet(FINAL_FILE, index=False)

        tqdm.write(f"Final merged dataset saved: {FINAL_FILE}")

        # Delete intermediate merged files if option is enabled
        if DELETE_INTERMEDIATE_FILES:
            for file in merged_files:
                os.remove(file)
                tqdm.write(f"Deleted intermediate file: {file}")

    tqdm.write("Merging process completed.")