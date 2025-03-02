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

from utils.logger_helper import setup_loggers  # Handles log file and console logging
from utils.config_loader import load_yaml_files  # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = ["config/paths.yaml", "config/process/base.yaml"]  # Add all your YAML files
config = load_yaml_files(CONFIG_FILES)

# Extract flight data settings
SOURCE_DIR = config["paths"]["extracted_flight_data"] # Raw flight data directory
KEEP_COLUMNS = config["flight_data"]["keep_columns"] # Columns to keep
SAVE_DIR = config["paths"]["processed_flight_data"] # Directory to save cleaned data
DELETE_SOURCE = config["flight_data"]["delete_pq"] # Delete source Parquet files after processing

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

def categorize_delay(df):
    """Creates a categorical variable for departure delays based on severity."""
    if "DepDelay" in df.columns:
        df["DelayCategory"] = np.select([
            (df["DepDelay"] <= 0),
            (df["DepDelay"] > 0) & (df["DepDelay"] < 60),
            (df["DepDelay"] >= 60)
        ], ["On-time", "Moderate Delay", "Severe Delay"], default="Unknown")
    return df

def categorize_airtime(df):
    """Categorizes flights based on airtime duration."""
    if "AirTime" in df.columns:
        df["AirTimeCategory"] = np.select([
            (df["AirTime"] < 120),
            (df["AirTime"] >= 120) & (df["AirTime"] < 360),
            (df["AirTime"] >= 360)
        ], ["Short", "Medium", "Long"], default="Unknown")
    return df

def categorize_time_of_day(df):
    """Assigns a time-of-day category based on departure time."""
    if "CRSDepTime" in df.columns:
        df["TimeofDay"] = np.select([
            (df["CRSDepTime"] < 600),
            (df["CRSDepTime"] >= 600) & (df["CRSDepTime"] < 1200),
            (df["CRSDepTime"] >= 1200) & (df["CRSDepTime"] < 1800),
            (df["CRSDepTime"] >= 1800)
        ], ["Early Morning", "Morning", "Afternoon", "Evening"], default="Unknown")
    return df

def add_cyclical_features(df):
    """Encodes cyclical time-based features using sine and cosine transformations."""
    df['DayOfYear'] = df['FlightDate'].dt.dayofyear
    for col, period in [("Month", 12), ("DayOfWeek", 7), ("DayOfYear", 365), ("CRSDepTime", 2400)]:
        if col in df.columns:
            df[f'{col}_sin'] = np.sin(2 * np.pi * df[col] / period)
            df[f'{col}_cos'] = np.cos(2 * np.pi * df[col] / period)
    return df

def add_holiday_indicators(df):
    """Adds a holiday indicator and a 'near holiday' flag based on U.S. holiday data."""
    if "FlightDate" in df.columns:
        us_holidays = {pd.to_datetime(k): v for k, v in holidays.US(years=range(2018, 2023)).items()}
        holiday_dates = pd.to_datetime(list(us_holidays.keys()))
        df['Holiday_Indicator'] = df['FlightDate'].isin(holiday_dates).astype(int)
        
        def is_near_holiday(flight_date, holiday_dict, days=3):
            flight_date = pd.to_datetime(flight_date)
            for holiday_date in holiday_dict.keys():
                if abs((flight_date - holiday_date).days) <= days:
                    return 1
            return 0
        
        df['Near_Holiday'] = df['FlightDate'].apply(lambda x: is_near_holiday(x, us_holidays))
    return df

# ─── Flight Data Cleaning Function ───────────────────────────────────────────
def clean_flight_file(file_path, progress, task_id):
    """Processes a single flight data file: cleans, categorizes, and saves."""
    filename = os.path.basename(file_path)
    year_str = filename.replace("extracted_flight_", "").replace(".parquet", "")
    save_path = os.path.join(SAVE_DIR, f"processed_flight_{year_str}.parquet")
    
    try:
        # Log processing start
        file_logger.info(f"Processing {filename}...")
        df = pd.read_parquet(file_path, columns=KEEP_COLUMNS) # Load only necessary columns

        # Apply cleaning and transformation steps sequentially
        df = (df.pipe(undersample_delays)
                .pipe(convert_flight_date)
                .pipe(categorize_delay)
                .pipe(categorize_airtime)
                .pipe(categorize_time_of_day)
                .pipe(add_cyclical_features)
                .pipe(add_holiday_indicators))
        
        df.to_parquet(save_path, index=False) # Save processed file
        if DELETE_SOURCE:
            os.remove(file_path) # Delete original file if required
            file_logger.info(f"Deleted raw Parquet file: {file_path}")

        # Log successful processing
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
    rich_logger.info("Starting flight data processing (this may take a while)")
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