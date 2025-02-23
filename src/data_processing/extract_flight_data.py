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
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]
config = load_yaml_files(CONFIG_FILES)

# Extract key configuration values
SOURCE_PATH = config["paths"]["raw_flight_data"]  # Path to the ZIP archive containing flight data
YEARS = config["overall"]["years"]  # List of years to filter relevant files
SAVE_DIR = config["paths"]["extracted_flight_data"]  # Directory where extracted Parquet files will be saved

# Ensure the extraction directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
# Initialize logging for console (rich_logger) and file output (file_logger)
LOG_FILENAME = "flight_extract"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Extraction Function ─────────────────────────────────────────────────────
def extract_parquet_file(zip_path, file, extract_dir, years, progress, task_id):
    """
    Extracts a single Parquet file from a ZIP archive, renames it based on its year,
    and saves it to the specified directory.

    Args:
        zip_path (str): Path to the ZIP archive.
        file (str): Name of the file to extract.
        extract_dir (str): Directory to save the extracted file.
        years (list): List of years used for filtering.
        progress (Progress): Rich progress bar instance.
        task_id (int): Task identifier for progress tracking.
    """
    try:
        # Log extraction start
        file_logger.info(f"Extracting {file}...")

        # Determine which year is present in the file name
        matched_years = [str(y) for y in years if str(y) in file]
        year_str = matched_years[0] if matched_years else "unknown"  # Use "unknown" if no match found

        # Construct new file name and destination path
        new_filename = f"extracted_flight_{year_str}.parquet"
        new_path = os.path.join(extract_dir, new_filename)

        # Extract and save the file
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            with zip_ref.open(file) as src, open(new_path, 'wb') as dest:
                dest.write(src.read())

        # Log successful extraction
        rich_logger.info(f"Successfully extracted {file} as {new_filename}")
        file_logger.info(f"Successfully extracted {file} as {new_filename}")

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

    # Open the ZIP archive and list all Parquet files that match the specified years
    with zipfile.ZipFile(SOURCE_PATH, 'r') as zip_ref:
        parquet_files = [
            f for f in zip_ref.namelist()
            if f.endswith(".parquet") and any(str(y) in f for y in YEARS)
        ]

    if not parquet_files:
        # Log warning if no files are found
        rich_logger.warning("No matching Parquet files found in the ZIP archive")
        file_logger.warning("No matching Parquet files found in the ZIP archive")
    else:
        # Initialize a progress task with a spinner indicator
        with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
            with ThreadPoolExecutor() as executor: # Use multiple threads for parallel processing
                futures = {}

                # Create progress spinner tasks and submit extraction jobs
                for file in parquet_files:
                    task_id = progress.add_task(f"Extracting {file}...")
                    futures[executor.submit(extract_parquet_file, SOURCE_PATH, file, SAVE_DIR, YEARS, progress, task_id)] = file

                # Wait for all extractions to complete
                for future in as_completed(futures):
                    future.result()

    # Log completion message
    rich_logger.info("All flight data extractions complete")
    file_logger.info("All flight data extractions complete")