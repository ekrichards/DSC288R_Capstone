import os
import pandas as pd
import yaml
from tqdm import tqdm

# Load configuration from YAML
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

# Extract settings from YAML
RAW_DIR = config["paths"]["extracted_noaa_data"]      # Directory with raw NOAA data
CLEAN_DIR = config["paths"]["processed_noaa_data"]   # Directory to save cleaned data
YEARS = config["noaa_data"]["years"]           # List of years to process
CORE_ELEMENTS = set(config["noaa_data"]["elements"])  # Elements to keep
STATION_KEY_PATH = config["paths"]["airport_station_data"]  # Path to station-airport key CSV
DELETE_CSV = config["noaa_data"]["delete_csv"]  # Delete raw NOAA CSVs after processing?

# Ensure save directory exists
os.makedirs(CLEAN_DIR, exist_ok=True)

# Load station key and create a mapping (STATION → AIRPORT_CODE)
station_key_df = pd.read_csv(STATION_KEY_PATH, usecols=["Closest_Station", "Airport"])
valid_stations = set(station_key_df["Closest_Station"].astype(str))  # Set of valid stations
station_mapping = dict(zip(station_key_df["Closest_Station"].astype(str), station_key_df["Airport"]))  # Mapping

def clean_noaa_file(file_path, year):
    """Reads, cleans, and transforms a NOAA GHCN file, filtering first, then replacing stations with airport codes."""
    save_path = os.path.join(CLEAN_DIR, f"{year}_clean.parquet")

    # Read only needed columns
    df = pd.read_csv(
        file_path,
        usecols=[0, 1, 2, 3],  # STATION, DATE, ELEMENT, VALUE
        names=["STATION", "DATE", "ELEMENT", "VALUE"],  # Assign column names
        dtype={"STATION": str, "DATE": str, "ELEMENT": str, "VALUE": float},  # Ensure types
        skiprows=1  # Skip header row since names are assigned manually
    )

    # Filter for relevant stations (removes unnecessary rows early)
    df = df[df["STATION"].isin(valid_stations)]

    # Filter only for required core elements (before date conversion for efficiency)
    df = df[df["ELEMENT"].isin(CORE_ELEMENTS)]

    # Replace station IDs with corresponding airport codes
    df["STATION"] = df["STATION"].map(station_mapping)

    # Convert date format from YYYYMMDD → YYYY-MM-DD
    df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")

    # Pivot table to make ELEMENT values into separate columns
    df = df.pivot_table(index=["STATION", "DATE"], columns="ELEMENT", values="VALUE", aggfunc="first")

    # Reset index to flatten DataFrame
    df.reset_index(inplace=True)

    # Ensure all core columns exist (fill missing ones with NaN)
    for col in CORE_ELEMENTS:
        if col not in df.columns:
            df[col] = pd.NA

    # Save to Parquet for efficiency
    df.to_parquet(save_path, index=False)

    # Delete raw CSV file if DELETE_CSV is set to True
    if DELETE_CSV:
        os.remove(file_path)
        tqdm.write(f"Deleted raw CSV file: {file_path}")

    return save_path

if __name__ == "__main__":
    tqdm.write(f"Cleaning NOAA data for years: {YEARS}")
    
    for year in tqdm(YEARS, desc="Processing NOAA files"):
        raw_file_path = os.path.join(RAW_DIR, f"{year}.csv")

        if os.path.exists(raw_file_path):
            save_path = clean_noaa_file(raw_file_path, year)
            tqdm.write(f"Saved cleaned file: {save_path}")
        else:
            tqdm.write(f"Warning: {raw_file_path} not found, skipping.")

    tqdm.write("All NOAA files cleaned and saved.")