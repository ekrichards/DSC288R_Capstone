import os
import requests
import yaml
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

CONFIG_PATH = "config/main.yaml"

with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

# Extract settings
BASE_URL = config["noaa_data"]["base_url"]
YEARS = config["noaa_data"]["years"]
SAVE_DIR = config["paths"]["noaa_data_raw"]

# Ensure save directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

def download_file(year):
    """Download a GHCN yearly dataset (gzipped) using tqdm progress bar."""
    filename = f"{year}.csv.gz"
    file_url = BASE_URL + filename
    save_path = os.path.join(SAVE_DIR, filename)

    try:
        # Request to check file size
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
            position=YEARS.index(year),
            leave=True
        ) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
                    bar.update(len(chunk))

        tqdm.write(f"Downloaded {filename}")

    except Exception as e:
        tqdm.write(f"Error downloading {year}: {e}")

if __name__ == "__main__":
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, YEARS)

    tqdm.write("All downloads completed!")
