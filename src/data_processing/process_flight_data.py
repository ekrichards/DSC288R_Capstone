import os
import sys
import numpy as np
import pandas as pd
import holidays
from sklearn.utils import resample
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.progress import Progress, SpinnerColumn, TextColumn

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

from utils.logger_helper import setup_loggers   # Handles log file and console logging
from utils.config_loader import load_yaml_files # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = ["config/paths.yaml", "config/data.yaml"]  # Add all your YAML files
config = load_yaml_files(CONFIG_FILES)

# Extract flight data settings
SOURCE_DIR = config["paths"]["extracted_flight_data"]   # Raw flight data directory
KEEP_COLUMNS = config["flight_data"]["keep_columns"]    # Columns to keep
SAVE_DIR = config["paths"]["processed_flight_data"]     # Directory to save cleaned data
DELETE_SOURCE = config["flight_data"]["delete_pq"]      # Delete source Parquet files after processing

# Ensure the clean directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
LOG_FILENAME = "flight_data_processing"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Helper Functions for Data Transformations ───────────────────────────────
def undersample_delays(df):
    """Balances the dataset by downsampling non-delayed flights to match delayed flights."""
    if "DepDel15" in df.columns:
        delayed_flights = df[df['DepDel15'] == 1]
        on_time_flights = df[df['DepDel15'] == 0]
        on_time_sampled = resample(on_time_flights, replace=False, n_samples=len(delayed_flights), random_state=42)
        df = pd.concat([delayed_flights, on_time_sampled])
    return df

def convert_flight_date(df):
    """Converts FlightDate from YYYYMMDD format to a proper datetime format."""
    if "FlightDate" in df.columns:
        df["FlightDate"] = pd.to_datetime(df["FlightDate"], format="%Y%m%d")
        df["FlightDate"] = df["FlightDate"].dt.floor("D")
    return df

def categorize_airtime(df):
    """Categorizes flights based on airtime duration."""
    if "AirTime" in df.columns:
        df["AirTimeCategory"] = np.select([
            (df["AirTime"] < 120),
            (df["AirTime"] >= 120) & (df["AirTime"] < 360),
            (df["AirTime"] >= 360)
        ], [0, 1, 2], default="Unknown")
    return df

def categorize_distance(df):
    """Categorizes flights based on airtime duration."""
    if "Distance" in df.columns:
        df["DistanceCategory"] = np.select([
            (df["Distance"] < 500),
            (df["Distance"] >= 500) & (df["Distance"] < 2500),
            (df["Distance"] >= 2500)
        ], [0, 1, 2], default="Unknown")
    return df

def categorize_time_of_day(df):
    """Assigns a time-of-day category based on departure time."""
    if "CRSDepTime" in df.columns:
        df["TimeofDay"] = np.select([
            (df["CRSDepTime"] < 600),
            (df["CRSDepTime"] >= 600) & (df["CRSDepTime"] < 1200),
            (df["CRSDepTime"] >= 1200) & (df["CRSDepTime"] < 1800),
            (df["CRSDepTime"] >= 1800)
        ], [0, 1, 2, 3], default="Unknown")
    return df

def convert_military_time(df):
    """Converts a military time column in a DataFrame to total minutes in a day."""
    def military_to_minutes(time):
        hh = time // 100
        mm = time % 100
        if mm >= 60 or hh >= 24:
            return None
        return hh * 60 + mm
    for col in ['CRSDepTime', 'CRSArrTime']:
        df[col] = df[col].apply(military_to_minutes)
    return df

def add_cyclical_features(df):
    """Encodes cyclical time-based features using sine and cosine transformations."""
    df['DayOfYear'] = df['FlightDate'].dt.dayofyear
    for col, period in [("CRSDepTime", 2358), ("CRSArrTime", 2358), ("DayOfYear", 365), ("DayOfWeek", 7), ("Month", 12)]:
        if col in df.columns:
            df[f'{col}_sin'] = np.sin(2 * np.pi * df[col] / period)
            df[f'{col}_cos'] = np.cos(2 * np.pi * df[col] / period)
    return df

def add_holiday_indicators(df):
    """Adds a holiday indicator and a 'near holiday' flag based on U.S. holiday data."""
    if "FlightDate" in df.columns:
        df["FlightDate"] = pd.to_datetime(df["FlightDate"])  # Ensure it's datetime
        us_holidays = pd.to_datetime(list(holidays.US(years=range(2018, 2023)).keys()))  # Convert holidays to datetime

        # Fast holiday indicator using vectorized .isin()
        df['Holiday_Indicator'] = df['FlightDate'].isin(us_holidays).astype(int)

        # Fast 'near holiday' check using broadcasting (fixing error)
        days_window = 3  # Change if needed
        holiday_ranges = np.concatenate([
            (us_holidays + pd.Timedelta(days=offset)).to_numpy()
            for offset in range(-days_window, days_window + 1)
        ])

        df['Near_Holiday'] = df['FlightDate'].isin(holiday_ranges).astype(int)

    return df

def add_weekend_indicator(df):
    df['Weekend_Indicator'] = df['DayOfWeek'].apply(lambda x: 1 if x in [1, 7] else 0)
    return df

def add_working_indicator(df):
    df['Working_Day'] = np.where((df['Weekend_Indicator'] == 1) | (df['Holiday_Indicator'] == 1), 0, 1)
    return df

# ─── Flight Data Cleaning Function ───────────────────────────────────────────
def clean_flight_file(file_path, progress, task_id):
    """
    Processes a single flight data file: cleans, categorizes, and saves.
    
    Args:
        file_path (str): Path to the raw flight parquet file.
        progress (Progress): Shared progress instance.
        task_id (int): The task ID for updating the spinner status.
    """
    filename = os.path.basename(file_path)
    year_str = filename.replace("extracted_flight_", "").replace(".parquet", "")
    save_path = os.path.join(SAVE_DIR, f"processed_flight_{year_str}.parquet")

    try:
        # Log processing start
        rich_logger.info(f"Started processing {filename}")
        file_logger.info(f"Started processing {filename}")
        progress.update(task_id, description=f"Loading {filename}...")

        df = pd.read_parquet(file_path, columns=KEEP_COLUMNS)
        rich_logger.info(f"Loaded {filename} with {df.shape[0]} rows and {df.shape[1]} columns")
        file_logger.info(f"Loaded {filename} with {df.shape[0]} rows and {df.shape[1]} columns")
        progress.update(task_id, description=f"Applying transformations to {filename}...")

        # Define transformation steps with logging
        steps = [
            ("applying undersampling", undersample_delays),
            ("converting flight date", convert_flight_date),
            ("categorizing airtime duration", categorize_airtime),
            ("categorizing flight distance", categorize_distance),
            ("categorizing time of day", categorize_time_of_day),
            ("converting military time", convert_military_time),
            ("adding cyclical features", add_cyclical_features),
            ("adding holiday indicators", add_holiday_indicators),
            ("adding weekend indicators", add_weekend_indicator),
            ("adding workday indicators", add_working_indicator)
        ]

        for step_desc, step_func in steps:
            progress.update(task_id, description=f"Working on {step_desc} for {filename}...")
            file_logger.info(f"Working on {step_desc} for {filename}...")
            df = step_func(df)
            rich_logger.info(f"Successfully completed {step_desc} for {filename}")
            file_logger.info(f"Successfully completed {step_desc} for {filename}")

        # Save processed data
        progress.update(task_id, description=f"Saving processed file {filename}...")
        df.to_parquet(save_path, index=False)
        rich_logger.info(f"Saved processed file: {save_path}")
        file_logger.info(f"Saved processed file: {save_path}")
        del df  # Free up memory

        if DELETE_SOURCE:
            os.remove(file_path)
            rich_logger.info(f"Deleted raw Flight parquet file: {file_path}")
            file_logger.info(f"Deleted raw Flight parquet file: {file_path}")

        # Final success log
        rich_logger.info(f"Successfully processed {filename}")
        file_logger.info(f"Successfully processed {filename}")

    except Exception as e:
        # Log processing failure
        rich_logger.error(f"Error processing {filename}: {e}")
        file_logger.error(f"Error processing {filename}: {e}")

    finally:
        # Remove task from progress display after completion
        progress.remove_task(task_id)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting flight data processing")
    file_logger.info("Starting flight data processing")
    
    # Identify all Parquet files in the source directory
    flight_files = [os.path.join(SOURCE_DIR, f) for f in os.listdir(SOURCE_DIR) if f.endswith(".parquet")]
    
    if not flight_files:
        # Log warning if no files are found
        rich_logger.warning("No extracted flight parquet files found in the source directory")
        file_logger.warning("No extracted flight parquet files found in the source directory")
    else:
        # Initialize a progress task with a spinner indicator
        with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
            with ThreadPoolExecutor() as executor: # Use multiple threads for parallel processing
                futures = {}

                # Create progress spinner tasks and submit extraction jobs
                for file_path in flight_files:
                    filename = os.path.basename(file_path)
                    task_id = progress.add_task(f"Processing {filename}...")
                    futures[executor.submit(clean_flight_file, file_path, progress, task_id)] = filename
                
                # Wait for all tasks to complete
                for future in as_completed(futures):
                    future.result()
    
    # Log completion message
    rich_logger.info("All flight data processing complete")
    file_logger.info("All flight data processing complete")