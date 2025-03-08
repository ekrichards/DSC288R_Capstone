# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import subprocess
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
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]
config = load_yaml_files(CONFIG_FILES)

# Extract key configuration values
KAGGLE_DATASETS = config["flight_data"]["kaggle"]  # List of Kaggle datasets to download
YEARS = config["overall"]["years"]              # List of years to download
SAVE_DIR = config["paths"]["raw_flight_data"]  # Directory to save Kaggle datasets

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
LOG_FILENAME = "flight_download"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Kaggle API File List Function ────────────────────────────────────────────
def list_kaggle_files():
    """Lists all available files in the Kaggle dataset."""
    command = f"kaggle datasets files -d {KAGGLE_DATASETS} --csv"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if result.returncode != 0:
        error_msg = result.stderr.strip()
        rich_logger.error(f"Error fetching file list from Kaggle: {error_msg}")
        file_logger.error(f"Error fetching file list from Kaggle: {error_msg}")
        sys.exit(1)

    file_list = result.stdout.split("\n")[1:]  # Skip header row
    return [line.split(",")[0] for line in file_list if line]

# ─── Kaggle API Download Function ────────────────────────────────────────────
def download_kaggle_file(file_name, progress, task_id):
    """Downloads a specific file from a Kaggle dataset."""
    save_path = os.path.join(SAVE_DIR, file_name)

    try:
        file_logger.info(f"Downloading {file_name}...")

        # Construct Kaggle API command to download only the selected file
        command = f"kaggle datasets download -d {KAGGLE_DATASETS} -f {file_name} -p {SAVE_DIR} --force"
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        if result.returncode == 0:
            rich_logger.info(f"Successfully downloaded {file_name}")
            file_logger.info(f"Successfully downloaded {file_name}")
        else:
            error_msg = result.stderr.decode("utf-8").strip()
            rich_logger.error(f"Error downloading {file_name}: {error_msg}")
            file_logger.error(f"Error downloading {file_name}: {error_msg}")

    except Exception as e:
        rich_logger.error(f"Unexpected error downloading {file_name}: {e}")
        file_logger.error(f"Unexpected error downloading {file_name}: {e}")

    finally:
        progress.remove_task(task_id)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting Kaggle dataset download process")
    file_logger.info("Starting Kaggle dataset download process")

    # Get available files from Kaggle
    available_files = list_kaggle_files()

    # Filter the files to download only the relevant years
    files_to_download = [f"Combined_Flights_{year}.parquet" for year in YEARS if f"Combined_Flights_{year}.parquet" in available_files]

    # Check if all requested years exist
    missing_years = [year for year in YEARS if f"Combined_Flights_{year}.parquet" not in available_files]

    if missing_years:
        rich_logger.error(f"Missing data for years: {missing_years}. Skipping these downloads")
        file_logger.error(f"Missing data for years: {missing_years}. Skipping these downloads")

    # Exit if no valid files are available
    if not files_to_download:
        rich_logger.error("No valid files available for the requested years")
        file_logger.error("No valid files available for the requested years")
        sys.exit(1)

    with Progress(
        SpinnerColumn(),
        TextColumn("{task.description}"),  # Task description
    ) as progress:
        with ThreadPoolExecutor() as executor:
            futures = {}

            for file_name in files_to_download:
                task_id = progress.add_task(f"Downloading {file_name}...", total=0)
                futures[executor.submit(download_kaggle_file, file_name, progress, task_id)] = file_name

            for future in as_completed(futures):
                future.result()

    rich_logger.info("All selected Kaggle dataset downloads complete")
    file_logger.info("All selected Kaggle dataset downloads complete")
