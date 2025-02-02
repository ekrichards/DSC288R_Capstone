import os
import requests
import yaml
from tqdm import tqdm

# List of YAML files to load
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]  # Add all your YAML files here

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
BASE_URL = config["noaa_data"]["base_url"]
YEARS = config["overall"]["years"]
SAVE_DIR = config["paths"]["raw_noaa_data"]

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
    # Sequentially download each year's file
    for year in YEARS:
        download_file(year)

    tqdm.write("All downloads completed!")