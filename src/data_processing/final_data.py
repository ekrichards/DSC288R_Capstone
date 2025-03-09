import os
import sys
import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import TargetEncoder
from rich.progress import Progress, SpinnerColumn, TextColumn
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

from utils.logger_helper import setup_loggers  # Handles log file and console logging
from utils.config_loader import load_yaml_files  # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = ["config/paths.yaml", "config/data.yaml"]  # Add all YAML files here
config = load_yaml_files(CONFIG_FILES)

# Extract paths and settings from configuration
FLIGHT_DIR = config["paths"]["processed_flight_data"]
WEATHER_DIR = config["paths"]["processed_noaa_data"]
SAVE_DIR = config["paths"]["final_by_year"]
YEARS = config["overall"]["years"]
DELETE_SOURCE_FILES = config["final_data"]["delete_processed"]

# Ensure output directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
LOG_FILENAME = "flight_weather_merge"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Helper Functions ────────────────────────────────────────────────────────
def add_rolling_averages(df):
    """Compute rolling averages for weather-related variables efficiently."""
    df['FlightDate'] = pd.to_datetime(df['FlightDate'])
    df = df.sort_values(['Origin', 'FlightDate'])

    variables = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']
    time_windows = {'weekly': 7, 'monthly': 30}

    # Vectorized rolling mean calculation
    for period, window in time_windows.items():
        grouped = df.groupby('Origin')[['Origin_TMIN', 'Origin_TMAX', 'Origin_PRCP', 'Origin_SNOW', 'Origin_SNWD']]
        df[[f'{period}_avg_origin_{var.lower()}' for var in variables]] = grouped.transform(lambda x: x.rolling(window=window, min_periods=1).mean())

        grouped_dest = df.groupby('Dest')[['Dest_TMIN', 'Dest_TMAX', 'Dest_PRCP', 'Dest_SNOW', 'Dest_SNWD']]
        df[[f'{period}_avg_dest_{var.lower()}' for var in variables]] = grouped_dest.transform(lambda x: x.rolling(window=window, min_periods=1).mean())

    # Handle missing values
    df.bfill(inplace=True)
    df.ffill(inplace=True)

    return df

def add_generic_rolling_averages(df, columns=['DepDelayMinutes'], windows={'weekly': 7, 'monthly': 30}):
    """Compute rolling averages for general-purpose features based on Origin."""
    df['FlightDate'] = pd.to_datetime(df['FlightDate'])
    df = df.sort_values(['Origin', 'FlightDate'])

    for period, window in windows.items():
        grouped = df.groupby('Origin')[columns]
        df[[f'{period}_avg_origin_{col.lower()}' for col in columns]] = grouped.transform(lambda x: x.rolling(window=window, min_periods=1).mean())

    # Handle missing values
    df.bfill(inplace=True)
    df.ffill(inplace=True)

    return df

def train_test_split_encoder(df, cat_cols=["Airline", "Origin", "Dest", "AirTimeCategory", "TimeofDay"], target_col="DepDel15"):
    """Performs a train-test split and applies Target Encoding to categorical features."""
    y = df[target_col]
    X = df.drop(columns=[target_col])

    # Split dataset
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Apply Target Encoding
    encoder = TargetEncoder(random_state=42)
    X_train.loc[:, cat_cols] = encoder.fit_transform(X_train[cat_cols], y_train)
    X_test.loc[:, cat_cols] = encoder.transform(X_test[cat_cols])

    # Attach target column back safely
    X_train.loc[:, target_col] = y_train
    X_test.loc[:, target_col] = y_test

    return X_train, X_test

def merge_flight_weather(year, progress, task_id):
    """Merges flight data with corresponding weather data for a given year."""
    flight_file = os.path.join(FLIGHT_DIR, f"processed_flight_{year}.parquet")
    weather_file = os.path.join(WEATHER_DIR, f"processed_noaa_{year}.parquet")

    if not os.path.exists(flight_file):
        rich_logger.warning(f"Skipping {year}: Missing flight file.")
        file_logger.warning(f"Skipping {year}: Missing flight file.")
        progress.remove_task(task_id)
        return None
    
    if not os.path.exists(weather_file):
        rich_logger.warning(f"Skipping {year}: Missing weather file.")
        file_logger.warning(f"Skipping {year}: Missing weather file.")
        progress.remove_task(task_id)
        return None

    try:
        # Log merging start
        file_logger.info(f"Merging {year}...")

        # Setup duckdb
        con = duckdb.connect(database=":memory:")

        con.execute(f"CREATE TABLE flights AS SELECT * FROM read_parquet('{flight_file}')")
        con.execute(f"CREATE TABLE weather AS SELECT * FROM read_parquet('{weather_file}')")

        # Perform LEFT JOINs for Origin and Destination weather
        merged_df = con.execute("""
            SELECT 
                f.*, 
                w_origin.PRCP AS Origin_PRCP, 
                w_origin.SNOW AS Origin_SNOW, 
                w_origin.SNWD AS Origin_SNWD, 
                w_origin.TMAX AS Origin_TMAX, 
                w_origin.TMIN AS Origin_TMIN,
                w_dest.PRCP AS Dest_PRCP, 
                w_dest.SNOW AS Dest_SNOW, 
                w_dest.SNWD AS Dest_SNWD, 
                w_dest.TMAX AS Dest_TMAX, 
                w_dest.TMIN AS Dest_TMIN
            FROM flights f
            LEFT JOIN weather w_origin 
                ON f.Origin = w_origin.STATION 
                AND f.FlightDate = w_origin.DATE
            LEFT JOIN weather w_dest
                ON f.Dest = w_dest.STATION 
                AND f.FlightDate = w_dest.DATE
            WHERE w_origin.STATION IS NOT NULL  
                OR w_dest.STATION IS NOT NULL
        """).fetchdf()

        con.close()

        if DELETE_SOURCE_FILES:
            os.remove(flight_file)
            os.remove(weather_file)
            rich_logger.info(f"Deleted processed files for {year}")
            file_logger.info(f"Deleted processed files for {year}")

        # Log successful merging
        rich_logger.info(f"Successfully merged {year}")
        file_logger.info(f"Successfully merged {year}")

        return merged_df

    except Exception as e:
        # Log merging failure
        rich_logger.error(f"Error merged {year}: {e}")
        file_logger.error(f"Error merged {year}: {e}")

    finally:
        # Remove task from progress display after completion
        progress.remove_task(task_id)


# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    rich_logger.info("Starting flight and weather data merge")
    file_logger.info("Starting flight and weather data merge")

    merged_dataframes = []

    with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
        # Multi-threaded Merging Step
        merge_tasks = {}

        with ThreadPoolExecutor() as executor:
            for year in YEARS:
                task_id = progress.add_task(f"Merging {year}...")
                future = executor.submit(merge_flight_weather, year, progress, task_id)
                merge_tasks[future] = year

            for future in as_completed(merge_tasks):
                merged_df = future.result()
                if merged_df is not None:
                    merged_dataframes.append(merged_df)

        if merged_dataframes:
            # Concatenation Step
            concat_task = progress.add_task("Concatenating datasets...")
            file_logger.info("Concatenating datasets...")
            final_df = pd.concat(merged_dataframes, ignore_index=True)
            del merged_dataframes  # Free up memory
            progress.remove_task(concat_task)
            rich_logger.info("Successfully concatenated datasets")
            file_logger.info("Successfully concatenated datasets")

            # Rolling Averages Step
            rolling_task = progress.add_task("Applying rolling averages...")
            file_logger.info("Applying rolling averages...")
            final_df = add_rolling_averages(final_df)
            final_df = add_generic_rolling_averages(final_df)
            final_df.drop('FlightDate', axis=1, inplace=True) # Drops datetime columns
            progress.remove_task(rolling_task)
            rich_logger.info("Successfully applied rolling averages")
            file_logger.info("Successfully applied rolling averages")

            # Train-Test Split Step
            split_task = progress.add_task("Performing train-test split...")
            file_logger.info("Performing train-test split...")
            train_data, test_data = train_test_split_encoder(final_df)
            del final_df  # Free up memory
            progress.remove_task(split_task)
            rich_logger.info("Successfully performed train-test split")
            file_logger.info("Successfully performed train-test split")

            # Saving Train/Test Data
            save_task = progress.add_task("Saving train/test splits...")
            file_logger.info("Saving train/test splits...")
            train_path = os.path.join(SAVE_DIR, "train_data.parquet")
            test_path = os.path.join(SAVE_DIR, "test_data.parquet")

            train_data.to_parquet(train_path, index=False)
            test_data.to_parquet(test_path, index=False)

            rich_logger.info(f"Saved train/test splits")
            file_logger.info(f"Saved train/test splits")

            progress.remove_task(save_task)

        else:
            rich_logger.warning("No valid data was merged.")
            file_logger.warning("No valid data was merged.")

    # Log completion message
    rich_logger.info("All data finalization complete")
    file_logger.info("All data finalization complete")