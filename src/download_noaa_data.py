import os
import requests
import gzip
import shutil
import yaml
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# Load configuration from YAML
CONFIG_PATH = "config/main.yaml"

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# Extract settings
BASE_URL = config["noaa_data"]["base_url"]
YEARS = config["noaa_data"]["years"]
SAVE_DIR = config["paths"]["noaa_data_raw"]
DELETE_GZ = config["noaa_data"]["delete_gz"]

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

def download_and_extract(year):
    """Download and extract a GHCN yearly dataset with tqdm progress bar."""
    filename = f"{year}.csv.gz"
    file_url = BASE_URL + filename
    save_path = os.path.join(SAVE_DIR, filename)
    extracted_path = os.path.join(SAVE_DIR, f"{year}.csv")

    try:
        # Send request to check file size
        response = requests.get(file_url, stream=True)
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))

        # Download file with tqdm progress bar
        with open(save_path, "wb") as file, tqdm(
            desc=f"Downloading {filename}",
            total=total_size,
            unit="B",
            unit_scale=True,
            unit_divisor=1024,
            position=YEARS.index(year),  # Ensures tqdm bars don't overlap
            leave=True
        ) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    bar.update(len(chunk))

        tqdm.write(f"Downloaded {filename}")

        # Extract the .gz file
        tqdm.write(f"Extracting {filename}...")
        with gzip.open(save_path, "rb") as f_in:
            with open(extracted_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        tqdm.write(f"Extracted {filename} to {extracted_path}")

        # Delete the compressed file if DELETE_GZ is True
        if DELETE_GZ:
            os.remove(save_path)
            tqdm.write(f"Deleted {filename} to save space.")

    except Exception as e:
        tqdm.write(f"Error processing {year}: {e}")

if __name__ == "__main__":
    # Assign each thread a unique ID for tqdm positioning
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_and_extract, YEARS)

    tqdm.write("All downloads and extractions completed!")