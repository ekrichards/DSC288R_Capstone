import os
import sys
import requests
import yaml
import logging
from datetime import datetime
from rich.progress import Progress
from rich.logging import RichHandler

# Dynamically get the project root (one level up from `src/`)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)  # Add to Python's module search path

from utils.logger_helper import setup_loggers
from utils.config_loader import load_yaml_files

# # ─── Helper Function: Set Up Loggers ─────────────────────────
# def setup_loggers(log_filename):
#     """Setup separate loggers for console (Rich) and file logging, with timestamped logs."""
    
#     # ─ Ensure "logs/" folder exists ─
#     log_dir = os.path.join(os.getcwd(), "logs")  # Always in repo root
#     os.makedirs(log_dir, exist_ok=True)

#     # ─ Append timestamp to log filename ─
#     timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#     full_log_path = os.path.join(log_dir, f"{log_filename}_{timestamp}.log")

#     # ─ Console Logger (Pretty Terminal Logging) ─
#     rich_logger = logging.getLogger("console_logger")
#     rich_logger.setLevel(logging.INFO)

#     rich_handler = RichHandler(rich_tracebacks=True)
#     rich_logger.addHandler(rich_handler)

#     # ─ File Logger (Save Logs to File) ─
#     file_logger = logging.getLogger("file_logger")
#     file_logger.setLevel(logging.INFO)

#     file_handler = logging.FileHandler(full_log_path, mode="w")
#     file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
#     file_handler.setFormatter(file_formatter)

#     file_logger.addHandler(file_handler)

#     return rich_logger, file_logger  # Return both loggers

# ─── Load Configuration ──────────────────────────────────────
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]
# def load_yaml_files(file_paths):
#     merged_config = {}
#     for path in file_paths:
#         with open(path, "r") as file:
#             config = yaml.safe_load(file)
#             if config:
#                 merged_config.update(config)
#     return merged_config

config = load_yaml_files(CONFIG_FILES)

# ─── Setup Loggers ───────────────────────────────────────────
LOG_FILENAME = "noaa_download"  # Base name for log file
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Extract Configuration Settings ──────────────────────────
BASE_URL = config["noaa_data"]["base_url"]
YEARS = config["overall"]["years"]
SAVE_DIR = config["paths"]["raw_noaa_data"]
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Download Function ───────────────────────────────────────
def download_file(year):
    """Download a GHCN yearly dataset with rich progress bar & logging."""
    filename = f"{year}.csv.gz"
    file_url = BASE_URL + filename
    save_path = os.path.join(SAVE_DIR, filename)

    try:
        rich_logger.info(f"Starting download for {year}")
        file_logger.info(f"Starting download for {year}")

        response = requests.get(file_url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))

        with Progress() as progress:
            task = progress.add_task(f"[cyan]Downloading {filename}[/]", total=total_size)

            with open(save_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
                        progress.update(task, advance=len(chunk))

        rich_logger.info(f"Successfully downloaded {filename}")
        file_logger.info(f"Successfully downloaded {filename}")

    except requests.RequestException as e:
        rich_logger.error(f"Error downloading {year}: {e}")
        file_logger.error(f"Error downloading {year}: {e}")

# ─── Main Execution ──────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting all downloads")
    file_logger.info("Starting all downloads")

    for year in YEARS:
        download_file(year)

    rich_logger.info("All downloads completed!")
    file_logger.info("All downloads completed!")

# import os
# import requests
# import yaml
# from tqdm import tqdm

# # List of YAML files to load
# CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]  # Add all your YAML files here

# def load_yaml_files(file_paths):
#     """Load multiple YAML files and merge their content."""
#     merged_config = {}
#     for path in file_paths:
#         with open(path, "r") as file:
#             config = yaml.safe_load(file)
#             if config:
#                 merged_config.update(config)  # Merge dictionaries (override duplicate keys)
#     return merged_config

# # Load and merge configurations
# config = load_yaml_files(CONFIG_FILES)

# # Extract settings
# BASE_URL = config["noaa_data"]["base_url"]
# YEARS = config["overall"]["years"]
# SAVE_DIR = config["paths"]["raw_noaa_data"]

# # Ensure save directory exists
# os.makedirs(SAVE_DIR, exist_ok=True)

# def download_file(year):
#     """Download a GHCN yearly dataset (gzipped) using tqdm progress bar."""
#     filename = f"{year}.csv.gz"
#     file_url = BASE_URL + filename
#     save_path = os.path.join(SAVE_DIR, filename)

#     try:
#         # Request to check file size
#         response = requests.get(file_url, stream=True)
#         response.raise_for_status()
#         total_size = int(response.headers.get("content-length", 0))

#         # Download file with tqdm progress bar
#         with open(save_path, "wb") as file, tqdm(
#             desc=f"Downloading {filename}",
#             total=total_size,
#             unit="B",
#             unit_scale=True,
#             unit_divisor=1024,
#             leave=True
#         ) as bar:
#             for chunk in response.iter_content(chunk_size=1024):
#                 if chunk:
#                     file.write(chunk)
#                     bar.update(len(chunk))

#         tqdm.write(f"Downloaded {filename}")

#     except Exception as e:
#         tqdm.write(f"Error downloading {year}: {e}")

# if __name__ == "__main__":
#     # Sequentially download each year's file
#     for year in YEARS:
#         download_file(year)

#     tqdm.write("All downloads completed!")