import os
import pandas as pd

INPUT_DIR = "data/extracted"
OUTPUT_FILE = "data/processed/combined_data.parquet"

def combine_parquet_files(input_dir, output_file):
    """Combine Parquet files into a single DataFrame."""
    parquet_files = [
        os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".parquet")
    ]
    if not parquet_files:
        raise ValueError(f"No Parquet files found in {input_dir}")

    combined_df = pd.concat([pd.read_parquet(f) for f in parquet_files], ignore_index=True)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    combined_df.to_parquet(output_file, index=False)
    print(f"Combined data saved to {output_file}")

if __name__ == "__main__":
    combine_parquet_files(INPUT_DIR, OUTPUT_FILE)
