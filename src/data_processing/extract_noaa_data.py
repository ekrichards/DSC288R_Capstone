# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import gzip
import shutil
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
# Load relevant configuration files
CONFIG_FILES = ["config/paths.yaml", "config/data.yaml"]
config = load_yaml_files(CONFIG_FILES)

# Extract key configuration values
SOURCE_DIR = config["paths"]["raw_noaa_data"]  # Directory containing .gz files
SAVE_DIR = config["paths"]["extracted_noaa_data"]  # Directory for extracted CSV files
DELETE_SOURCE = config["noaa_data"]["delete_gz"]  # Boolean flag for deleting original .gz files after extraction

# Ensure the extraction directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
# Initialize logging for console (rich_logger) and file output (file_logger)
LOG_FILENAME = "noaa_extract"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Extraction Function ─────────────────────────────────────────────────────
def extract_file(gz_path, progress, task_id):
    """
    Extracts a GHCN dataset from .gz format and optionally deletes the original file.
    
    Args:
        gz_path (str): Path to the .gz file that needs to be extracted.
        progress (Progress): Shared progress instance for tracking extraction status.
        task_id (int): The task ID for updating the spinner status.
    """
    gz_filename = os.path.basename(gz_path)  # Get the filename from the path
    csv_filename = f"extracted_noaa_{gz_filename.replace('.csv.gz', '.csv')}"  # Construct output filename
    csv_path = os.path.join(SAVE_DIR, csv_filename)  # Define the extraction path

    try:
        # Log extraction start
        file_logger.info(f"Extracting {gz_filename}...")

        # Extract .gz file to .csv format
        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)  # Copy file contents from compressed to uncompressed format

        # Delete the original .gz file if configured to do so
        if DELETE_SOURCE:
            os.remove(gz_path)
            rich_logger.info(f"Deleted raw NOAA gz file: {gz_filename}")
            file_logger.info(f"Deleted raw NOAA gz file: {gz_filename}")

        # Log successful extraction
        rich_logger.info(f"Successfully extracted {gz_filename}")
        file_logger.info(f"Successfully extracted {gz_filename}")

    except Exception as e:
        # Log extraction failure
        rich_logger.error(f"Error extracting {gz_filename}: {e}")
        file_logger.error(f"Error extracting {gz_filename}: {e}")

    finally:
        # Remove task from progress display after completion
        progress.remove_task(task_id)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting NOAA data extraction process")
    file_logger.info("Starting NOAA data extraction process")

    # Get a list of all .gz files in the source directory
    gz_files = [os.path.join(SOURCE_DIR, f) for f in os.listdir(SOURCE_DIR) if f.endswith(".csv.gz")]

    if not gz_files:
        # Log warning if no files are found
        rich_logger.warning("No raw NOAA .gz files found in the source directory")
        file_logger.warning("No raw NOAA .gz files found in the source directory")
    else:
        # Initialize a progress task with a spinner indicator
        with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
            with ThreadPoolExecutor() as executor: # Use multiple threads for parallel extraction
                futures = {}

                # Create progress spinner tasks and submit extraction jobs
                for gz_file in gz_files:
                    gz_filename = os.path.basename(gz_file)
                    task_id = progress.add_task(f"Extracting {gz_filename}...")  # Add a task for tracking
                    futures[executor.submit(extract_file, gz_file, progress, task_id)] = gz_filename

                # Wait for all tasks to complete
                for future in as_completed(futures):
                    future.result()

    # Log completion message
    rich_logger.info("All NOAA data extractions complete")
    file_logger.info("All NOAA data extractions complete")