import os
import gzip
import shutil
import yaml
import logging
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# List of YAML files to load
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]

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

# Extract settings
SOURCE_DIR = config["paths"]["raw_noaa_data"]
SAVE_DIR = config["paths"]["extracted_noaa_data"]
DELETE_GZ = config["noaa_data"]["delete_gz"]

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

def extract_file(gz_path):
    """Extract a GHCN dataset from .gz and optionally delete the original file."""
    gz_filename = os.path.basename(gz_path)
    csv_filename = f"extracted_noaa_{gz_filename.replace('.csv.gz', '.csv')}"
    csv_path = os.path.join(SAVE_DIR, csv_filename)

    try:
        logging.info(f"Extracting {gz_filename}...")

        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        logging.info(f"Extracted {gz_filename} to {csv_path}")

        # Delete the compressed file if DELETE_GZ is True
        if DELETE_GZ:
            os.remove(gz_path)
            logging.info(f"Deleted {gz_filename} to save space.")

    except Exception as e:
        logging.error(f"Error extracting {gz_filename}: {e}")

if __name__ == "__main__":
    # Find all .gz files in SOURCE_DIR
    gz_files = [os.path.join(SOURCE_DIR, f) for f in os.listdir(SOURCE_DIR) if f.endswith(".csv.gz")]

    if not gz_files:
        logging.warning("No .gz files found in the source directory.")
    else:
        with ThreadPoolExecutor(max_workers=4) as executor:
            executor.map(extract_file, gz_files)

        logging.info("All extractions completed!")

# import os
# import gzip
# import shutil
# import yaml
# from concurrent.futures import ThreadPoolExecutor
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
# YEARS = config["overall"]["years"]
# SOURCE_DIR = config["paths"]["raw_noaa_data"]
# SAVE_DIR = config["paths"]["extracted_noaa_data"]
# DELETE_GZ = config["noaa_data"]["delete_gz"] # Delete .gz file after extraction?

# # Ensure save directory exists
# os.makedirs(SAVE_DIR, exist_ok=True)

# def extract_file(year):
#     """Extract a GHCN yearly dataset from .gz, optionally delete the .gz."""
#     gz_filename = f"{year}.csv.gz"
#     csv_filename = f"extracted_noaa_{year}.csv"
#     gz_path = os.path.join(SOURCE_DIR, gz_filename)
#     csv_path = os.path.join(SAVE_DIR, csv_filename)

#     # Check if .gz file exists
#     if not os.path.exists(gz_path):
#         tqdm.write(f".gz file for {year} not found, skipping...")
#         return

#     try:
#         tqdm.write(f"Extracting {gz_filename}...")
#         with gzip.open(gz_path, "rb") as f_in:
#             with open(csv_path, "wb") as f_out:
#                 shutil.copyfileobj(f_in, f_out)
#         tqdm.write(f"Extracted {gz_filename} to {csv_path}")

#         # Delete the compressed file if DELETE_GZ is True
#         if DELETE_GZ:
#             os.remove(gz_path)
#             tqdm.write(f"Deleted {gz_filename} to save space.")

#     except Exception as e:
#         tqdm.write(f"Error extracting {year}: {e}")

# if __name__ == "__main__":
#     with ThreadPoolExecutor(max_workers=5) as executor:
#         executor.map(extract_file, YEARS)

#     tqdm.write("All extractions completed!")
