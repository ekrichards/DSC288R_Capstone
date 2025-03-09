import os
import sys
import zipfile
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
SOURCE_DIR = config["paths"]["raw_flight_data"]  # Path to the ZIP archive containing flight data
SAVE_DIR = config["paths"]["extracted_flight_data"]  # Directory where extracted Parquet files will be saved
DELETE_SOURCE = config["flight_data"]["delete_zip"]

# Ensure the extraction directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
# Initialize logging for console (rich_logger) and file output (file_logger)
LOG_FILENAME = "flight_extract"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Extraction Function ─────────────────────────────────────────────────────
def extract_parquet_files(zip_path, extract_dir, progress, task_id):
    """
    Extracts all Parquet files from a ZIP archive, renames them based on their year,
    and saves them to the specified directory.

    Args:
        zip_path (str): Path to the ZIP archive.
        extract_dir (str): Directory to save extracted files.
        delete_source (bool): Whether to delete the ZIP file after extraction.
        progress (Progress): Progress bar instance.
        task_id (int): Task ID for tracking progress.
    """
    try:
        zip_filename = os.path.basename(zip_path)
        file_logger.info(f"Extracting {zip_filename}...")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            parquet_files = [f for f in zip_ref.namelist() if f.endswith(".parquet")]

            if not parquet_files:
                rich_logger.warning(f"No raw Flight .zip files found in the source directory")
                file_logger.warning(f"No raw Flight .zip files found in the source directory")
                return

            for file in parquet_files:
                # Determine which year is present in the file name
                matched_years = [str(y) for y in range(2000, 2030) if str(y) in file]
                year_str = matched_years[0] if matched_years else "unknown"

                # Construct new file name and destination path
                new_filename = f"extracted_flight_{year_str}.parquet"
                new_path = os.path.join(extract_dir, new_filename)

                # Extract and save the file
                with zip_ref.open(file) as src, open(new_path, 'wb') as dest:
                    dest.write(src.read())

        # Log successful extraction
        rich_logger.info(f"Successfully extracted {file} as {new_filename}")
        file_logger.info(f"Successfully extracted {file} as {new_filename}")

        # Delete ZIP file after successful extraction if enabled
        if DELETE_SOURCE:
            os.remove(zip_path)
            rich_logger.info(f"Deleted raw Flight zip file: {zip_filename}")
            file_logger.info(f"Deleted raw Flight zip file: {zip_filename}")

    except Exception as e:
        # Log extraction failure
        rich_logger.error(f"Error extracting {file}: {e}")
        file_logger.error(f"Error extracting {file}: {e}")
    finally:
        # Remove task from progress display after completion
        progress.remove_task(task_id)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting flight data extraction process")
    file_logger.info("Starting flight data extraction process")

    # Find all ZIP files in the source directory
    zip_files = [os.path.join(SOURCE_DIR, f) for f in os.listdir(SOURCE_DIR) if f.endswith(".zip")]

    if not zip_files:
        rich_logger.warning("No ZIP files found in the source directory")
        file_logger.warning("No ZIP files found in the source directory")
    else:
        with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
            with ThreadPoolExecutor() as executor:
                futures = {}

                for zip_file in zip_files:
                    zip_filename = os.path.basename(zip_file)
                    task_id = progress.add_task(f"Extracting from {zip_filename}...")
                    futures[executor.submit(extract_parquet_files, zip_file, SAVE_DIR, progress, task_id)] = zip_file

                for future in as_completed(futures):
                    future.result()

    # Log completion message
    rich_logger.info("All flight data extractions complete")
    file_logger.info("All flight data extractions complete")