import os
import gzip
import shutil
import yaml
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

CONFIG_PATH = "config/main.yaml"

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# Extract settings
YEARS = config["noaa_data"]["years"]
SOURCE_DIR = config["paths"]["noaa_data_raw"]
SAVE_DIR = config["paths"]["data_noaa_extracted"]

# Delete .gz file after extraction?
DELETE_GZ = config["noaa_data"]["delete_gz"]

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

def extract_file(year):
    """Extract a GHCN yearly dataset from .gz, optionally delete the .gz."""
    gz_filename = f"{year}.csv.gz"
    csv_filename = f"{year}.csv"
    gz_path = os.path.join(SOURCE_DIR, gz_filename)
    csv_path = os.path.join(SAVE_DIR, csv_filename)

    # Check if .gz file exists
    if not os.path.exists(gz_path):
        tqdm.write(f".gz file for {year} not found, skipping...")
        return

    try:
        tqdm.write(f"Extracting {gz_filename}...")
        with gzip.open(gz_path, "rb") as f_in:
            with open(csv_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        tqdm.write(f"Extracted {gz_filename} to {csv_path}")

        # Delete the compressed file if DELETE_GZ is True
        if DELETE_GZ:
            os.remove(gz_path)
            tqdm.write(f"Deleted {gz_filename} to save space.")

    except Exception as e:
        tqdm.write(f"Error extracting {year}: {e}")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(extract_file, YEARS)

    tqdm.write("All extractions completed!")
