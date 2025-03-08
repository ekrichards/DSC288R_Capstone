# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, BarColumn, DownloadColumn, TextColumn

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

# Import logging and configuration utilities
from utils.logger_helper import setup_loggers  # Handles log file and console logging
from utils.config_loader import load_yaml_files  # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
# Load relevant configuration files
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]
config = load_yaml_files(CONFIG_FILES)

# Extract key configuration values
SOURCE_URL = config["noaa_data"]["base_url"]    # NOAA dataset base URL
YEARS = config["overall"]["years"]              # List of years to download
SAVE_DIR = config["paths"]["raw_noaa_data"]     # Directory to save downloaded data

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
# Initialize logging for console (rich_logger) and file output (file_logger)
LOG_FILENAME = "noaa_download"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Download Function ───────────────────────────────────────────────────────
def download_file(year, progress, task_id):
    """
    Downloads a GHCN yearly dataset from NOAA and saves it locally.
    
    Args:
        year (int): The year of the dataset to be downloaded.
        progress (Progress): Shared progress bar instance for tracking progress.
        task_id (int): The task ID for updating the progress bar.
    """
    filename = f"{year}.csv.gz"  # Construct filename
    file_url = SOURCE_URL + filename  # Construct full URL
    save_path = os.path.join(SAVE_DIR, filename)  # Local save path

    try:
        # Log download start
        file_logger.info(f"Downloading {filename}...")

        # Send request to NOAA server with streaming enabled
        response = requests.get(file_url, stream=True)
        response.raise_for_status()  # Raise an error if request fails
        total_size = int(response.headers.get("content-length", 0))  # Get total file size

        # Update progress task
        progress.update(task_id, total=total_size)

        # Download in chunks and update progress bar
        with open(save_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):  # Download in 1KB chunks
                if chunk:
                    file.write(chunk)  
                    progress.update(task_id, advance=len(chunk))  

        # Log successful download
        rich_logger.info(f"Successfully downloaded {filename}")
        file_logger.info(f"Successfully downloaded {filename}")

    except requests.RequestException as e:
        # Log failure
        rich_logger.error(f"Error downloading {year}: {e}")
        file_logger.error(f"Error downloading {year}: {e}")

    finally:
        # Remove task from progress display after completion
        progress.remove_task(task_id)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting NOAA data download process")
    file_logger.info("Starting NOAA data download process")

    # Initialize progress bar to visually track downloads
    with Progress(
        TextColumn("{task.description}"),  # Task description
        DownloadColumn(),  # Shows downloaded size
        BarColumn()  # Shows graphical progress bar
    ) as progress:
        with ThreadPoolExecutor() as executor: # Use multiple threads for faster downloads
            futures = {}

            # Submit download tasks and track them with progress bar
            for year in YEARS:
                task_id = progress.add_task(f"Downloading {year}.csv.gz...", total=0)  # Initialize task
                futures[executor.submit(download_file, year, progress, task_id)] = year

            # Wait for all tasks to complete
            for future in as_completed(futures):
                future.result()

    # Log completion message
    rich_logger.info("All NOAA data downloads complete")
    file_logger.info("All NOAA data downloads complete")