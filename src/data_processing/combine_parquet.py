import yaml
import os
import pandas as pd
from tqdm import tqdm

# Load configuration
with open("config/paths.yaml", "r") as f:
    config = yaml.safe_load(f)

INPUT_DIR = config["paths"]["extracted_flight_data"]
OUTPUT_FILE = config["paths"]["combined_parquet_file"]

def combine_parquet_files(input_dir, output_file):
    """Combine Parquet files into a single DataFrame with a progress bar."""
    parquet_files = [
        os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".parquet")
    ]
    if not parquet_files:
        raise ValueError(f"No Parquet files found in {input_dir}")

    combined_df = pd.concat(
        [pd.read_parquet(f) for f in tqdm(parquet_files, desc="Reading Parquet files")], 
        ignore_index=True
    )

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    combined_df.to_parquet(output_file, index=False)
    print(f"Combined data saved to {output_file}")

if __name__ == "__main__":
    combine_parquet_files(INPUT_DIR, OUTPUT_FILE)