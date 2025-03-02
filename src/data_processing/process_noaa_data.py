# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, SpinnerColumn, TextColumn

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

# Import logging and configuration utilities
from utils.logger_helper import setup_loggers  # Handles log file and console logging
from utils.config_loader import load_yaml_files  # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
# Load configuration files
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]
config = load_yaml_files(CONFIG_FILES)

# Extract settings from configuration
SOURCE_DIR = config["paths"]["extracted_noaa_data"]  # Directory with raw NOAA data
STATION_KEY_PATH = config["paths"]["airport_station_data"]  # Path to station-airport mapping CSV
CORE_ELEMENTS = set(config["noaa_data"]["elements"])  # Elements to retain
ZERO_OUT_ELEMENTS = set(config["noaa_data"]["zero_out_elements"])  # Elements where NaN should be replaced with 0
SAVE_DIR = config["paths"]["processed_noaa_data"]  # Directory for saving cleaned data
DELETE_SOURCE = config["noaa_data"]["delete_csv"]  # Whether to delete original CSV files after processing

# Ensure output directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Load Station Mapping ────────────────────────────────────────────────────
# Read station-airport mapping file
station_key_df = pd.read_csv(STATION_KEY_PATH, usecols=["Closest_Station", "Airport"])
valid_stations = set(station_key_df["Closest_Station"].astype(str))  # Set of valid station IDs
station_mapping = dict(zip(station_key_df["Closest_Station"].astype(str), station_key_df["Airport"]))  # Mapping

# ─── Setup Loggers ───────────────────────────────────────────────────────────
# Initialize logging for console (rich_logger) and file output (file_logger)
LOG_FILENAME = "noaa_processing"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Data Cleaning Function ──────────────────────────────────────────────────
def clean_noaa_file(file_path, progress, task_id):
    """
    Cleans and transforms a NOAA GHCN dataset, filtering by relevant stations and elements,
    replacing station IDs with airport codes, and saving as a Parquet file.

    Args:
        file_path (str): Path to the raw NOAA CSV file.
        progress (Progress): Shared progress instance.
        task_id (int): The task ID for updating the spinner status.
    
    Returns:
        str: Path to the cleaned Parquet file (if successful), or None if the file was missing.
    """
    filename = os.path.basename(file_path)  # Extract filename
    year = filename.replace("extracted_noaa_", "").replace(".csv", "")  # Extract year from filename
    save_path = os.path.join(SAVE_DIR, f"processed_noaa_{year}.parquet")

    try:
        # Log processing start
        file_logger.info(f"Processing {filename}...")

        # Read CSV with specified columns and data types
        df = pd.read_csv(
            file_path,
            usecols=[0, 1, 2, 3],  # STATION, DATE, ELEMENT, VALUE
            names=["STATION", "DATE", "ELEMENT", "VALUE"],  # Assign column names
            dtype={"STATION": str, "DATE": str, "ELEMENT": str, "VALUE": float},
            skiprows=1  # Skip header row since column names are manually assigned
        )

        # Filter only relevant stations
        df = df[df["STATION"].isin(valid_stations)]

        # Filter for core elements
        df = df[df["ELEMENT"].isin(CORE_ELEMENTS)]

        # Replace station IDs with corresponding airport codes
        df["STATION"] = df["STATION"].map(station_mapping)

        # Convert date format from YYYYMMDD to YYYY-MM-DD
        df["DATE"] = pd.to_datetime(df["DATE"], format="%Y%m%d")
        df["DATE"] = df["DATE"].dt.floor("D")

        # Pivot table to convert ELEMENT values into separate columns
        df = df.pivot_table(index=["STATION", "DATE"], columns="ELEMENT", values="VALUE", aggfunc="first")

        # Reset index to flatten DataFrame
        df.reset_index(inplace=True)

        # Ensure all core elements exist (fill missing ones with NaN)
        df[list(CORE_ELEMENTS)] = df.reindex(columns=list(CORE_ELEMENTS)).fillna(pd.NA)

        # Replace missing values for elements in zero_out_elements with 0
        df[list(ZERO_OUT_ELEMENTS)] = df[list(ZERO_OUT_ELEMENTS)].fillna(0)

        # Save cleaned data to Parquet format
        df.to_parquet(save_path, index=False)

        # Optionally delete the original CSV file
        if DELETE_SOURCE:
            os.remove(file_path)
            file_logger.info(f"Deleted raw CSV file: {file_path}")

        # Log successful processing
        rich_logger.info(f"Successfully processed {filename}")
        file_logger.info(f"Successfully processed {filename}")

    except Exception as e:
        # Log processing failure
        rich_logger.error(f"Error processing {filename}: {e}")
        file_logger.error(f"Error processing {filename}: {e}")

    finally:
        # Remove task from progress display after completion
        progress.remove_task(task_id)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info(f"Starting NOAA data processing")
    file_logger.info(f"Starting NOAA data processing")
    
    # Get all CSV files in the extracted directory
    raw_files = [os.path.join(SOURCE_DIR, f) for f in os.listdir(SOURCE_DIR) if f.endswith(".csv")]

    if not raw_files:
        # Log warning if no files are found
        rich_logger.warning("No extracted NOAA CSV files found in the source directory")
        file_logger.warning("No extracted NOAA CSV files found in the source directory")
    else:
        # Initialize a progress task with a spinner indicator
        with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
            with ThreadPoolExecutor() as executor: # Use multiple threads for parallel processing
                futures = {}

                # Create progress spinner tasks and submit extraction jobs
                for file_path in raw_files:
                    filename = os.path.basename(file_path)
                    task_id = progress.add_task(f"Processing {filename}...")
                    futures[executor.submit(clean_noaa_file, file_path, progress, task_id)] = filename

                # Wait for all tasks to complete
                for future in as_completed(futures):
                    future.result()

        # Log completion message
        rich_logger.info("All NOAA data processing complete")
        file_logger.info("All NOAA data processing complete")