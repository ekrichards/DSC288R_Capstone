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

# Extract flight data settings
RAW_DIR = config["paths"]["extracted_flight_data"]    # Raw flight data directory
CLEAN_DIR = config["paths"]["processed_flight_data"]  # Directory to save cleaned data
DELETE_PQ = config["flight_data"]["delete_raw"]       # Delete raw Parquet files after processing?
KEEP_COLUMNS = config["flight_data"]["keep_columns"]  # Columns to keep

# Ensure clean directory exists
os.makedirs(CLEAN_DIR, exist_ok=True)

def clean_flight_file(file_path):
    """Reads, cleans, and transforms a flight data Parquet file."""
    filename = os.path.basename(file_path)
    save_path = os.path.join(CLEAN_DIR, filename)

    # Read Parquet file with selected columns
    df = pd.read_parquet(file_path, columns=KEEP_COLUMNS)

    # Convert date format
    df["FlightDate"] = df["FlightDate"].astype('datetime64[s]')

    # Save cleaned file as Parquet
    df.to_parquet(save_path, index=False)

    # Delete raw Parquet file if DELETE_RAW is set to True
    if DELETE_PQ:
        os.remove(file_path)
        tqdm.write(f"Deleted raw Parquet file: {file_path}")

    return save_path

if __name__ == "__main__":
    tqdm.write(f"Cleaning flight data in {RAW_DIR}")

    # Process all Parquet files in the raw directory
    flight_files = [os.path.join(RAW_DIR, f) for f in os.listdir(RAW_DIR) if f.endswith(".parquet")]

    for file_path in tqdm(flight_files, desc="Processing flight files"):
        save_path = clean_flight_file(file_path)
        tqdm.write(f"Saved cleaned file: {save_path}")

    tqdm.write("All flight data cleaned and saved.")
