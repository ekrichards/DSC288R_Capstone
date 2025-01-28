import zipfile
import os

ZIP_PATH = "data/raw/archive.zip"
EXTRACT_DIR = "data/extracted"

def extract_parquet_files(zip_path, extract_dir):
    """Extract only Parquet files from a zip archive."""
    os.makedirs(extract_dir, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        parquet_files = [f for f in zip_ref.namelist() if f.endswith(".parquet")]
        for file in parquet_files:
            zip_ref.extract(file, extract_dir)
            print(f"Extracted: {file}")

if __name__ == "__main__":
    extract_parquet_files(ZIP_PATH, EXTRACT_DIR)