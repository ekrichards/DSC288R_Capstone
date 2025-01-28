import yaml

# Load configuration
with open("config/main.yaml", "r") as f:
    config = yaml.safe_load(f)

# Access paths
ZIP_PATH = config["paths"]["raw_data"]
EXTRACT_DIR = config["paths"]["extracted_data"]

def extract_parquet_files(zip_path, extract_dir):
    """Extract only Parquet files from a zip archive."""
    import os, zipfile
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        parquet_files = [f for f in zip_ref.namelist() if f.endswith(".parquet")]
        for file in parquet_files:
            zip_ref.extract(file, extract_dir)
            print(f"Extracted: {file}")

if __name__ == "__main__":
    extract_parquet_files(ZIP_PATH, EXTRACT_DIR)