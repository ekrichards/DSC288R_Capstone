import yaml
import os
import zipfile

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

# Access paths
ZIP_PATH = config["paths"]["raw_flight_data"]
EXTRACT_DIR = config["paths"]["extracted_flight_data"]
FLIGHT_YEARS = config["overall"]["years"]

def extract_parquet_files(zip_path, extract_dir, years):
    """Extract only Parquet files that include any of the specified years in the filename 
    from a zip archive, renaming them to extracted_flight_<year>.parquet."""
    
    os.makedirs(extract_dir, exist_ok=True)
    
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Filter for parquet files that contain at least one of the specified years
        parquet_files = [
            f for f in zip_ref.namelist() 
            if f.endswith(".parquet") and any(str(y) in f for y in years)
        ]
        
        for file in parquet_files:
            # Extract the file first
            zip_ref.extract(file, extract_dir)
            
            # Determine which year is in the filename
            # (Assumes each file name has a single matching year from the FLIGHT_YEARS list)
            matched_years = [str(y) for y in years if str(y) in file]
            if matched_years:
                # Just use the first matched year; adjust if you need different logic
                year_str = matched_years[0]
            else:
                year_str = "unknown"  # fallback if no match (shouldn't happen given the filter)

            old_path = os.path.join(extract_dir, file)
            new_filename = f"extracted_flight_{year_str}.parquet"
            new_path = os.path.join(extract_dir, new_filename)
            
            # Rename the extracted file
            os.rename(old_path, new_path)
            print(f"Extracted and renamed: {file} -> {new_filename}")

if __name__ == "__main__":
    extract_parquet_files(ZIP_PATH, EXTRACT_DIR, FLIGHT_YEARS)


# import yaml

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

# # Access paths
# ZIP_PATH = config["paths"]["raw_flight_data"]
# EXTRACT_DIR = config["paths"]["extracted_flight_data"]
# FLIGHT_YEARS = config["overall"]["years"]

# def extract_parquet_files(zip_path, extract_dir, years):
#     """Extract only Parquet files that include any of the specified years in the filename from a zip archive."""
#     import os
#     import zipfile
    
#     os.makedirs(extract_dir, exist_ok=True)
#     with zipfile.ZipFile(zip_path, 'r') as zip_ref:
#         # Filter for parquet files that contain at least one of the specified years
#         parquet_files = [
#             f for f in zip_ref.namelist() 
#             if f.endswith(".parquet") and any(str(y) in f for y in years)
#         ]
        
#         for file in parquet_files:
#             zip_ref.extract(file, extract_dir)
#             print(f"Extracted: {file}")

# if __name__ == "__main__":
#     extract_parquet_files(ZIP_PATH, EXTRACT_DIR, FLIGHT_YEARS)