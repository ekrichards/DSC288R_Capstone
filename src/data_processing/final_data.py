import os
import sys
import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import TargetEncoder, StandardScaler
from rich.progress import Progress, SpinnerColumn, TextColumn
from concurrent.futures import ThreadPoolExecutor, as_completed

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

from utils.logger_helper import setup_loggers   # Handles log file and console logging
from utils.config_loader import load_yaml_files # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = ["config/paths.yaml", "config/data.yaml"]  # Add all YAML files here
config = load_yaml_files(CONFIG_FILES)

# Extract paths and settings from configuration
FLIGHT_DIR = config["paths"]["processed_flight_data"]           # Processed flight data dictionary
WEATHER_DIR = config["paths"]["processed_noaa_data"]            # Processed NOAA data dictionary
SAVE_DIR = config["paths"]["final_by_year"]                     # Final save directory
YEARS = config["overall"]["years"]                              # Years to process
DELETE_SOURCE_FILES = config["final_data"]["delete_processed"]  # Optional deletion of source data to save space

# Ensure output directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
LOG_FILENAME = "flight_weather_merge"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Helper Functions ────────────────────────────────────────────────────────
def merge_flight_weather(year, progress, task_id):
    """
    Merges flight data with corresponding weather data for a given year.
    
    Args:
        year (int): Year to process.
        progress (Progress): Shared progress instance.
        task_id (int): The task ID for updating the spinner status.
    """
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

def add_rolling_averages_weather(df):
    """Compute rolling averages for weather-related variables efficiently for both Origin and Destination."""

    variables = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']
    time_windows = {'weekly': 7, 'monthly': 30}

    # Step 1: Aggregate daily weather values per Origin & Destination
    origin_vars = [f'Origin_{var}' for var in variables]
    dest_vars = [f'Dest_{var}' for var in variables]

    origin_daily_avg = df.groupby(['Origin', 'FlightDate'])[origin_vars].mean().reset_index()
    dest_daily_avg = df.groupby(['Dest', 'FlightDate'])[dest_vars].mean().reset_index()

    # Step 2: Compute rolling averages separately for Origin & Destination
    origin_rolling_avg_cols = []  # List to store new rolling avg column names
    dest_rolling_avg_cols = []

    for period, window in time_windows.items():
        for var in variables:
            # Rolling average for Origin-specific weather
            origin_col = f'Origin_{var}'
            origin_avg_col = f'{period}_avg_origin_{var.lower()}'
            origin_daily_avg[origin_avg_col] = (
                origin_daily_avg.groupby('Origin')[origin_col]
                .apply(lambda x: x.ffill().bfill())
                .rolling(window=window, min_periods=1)
                .mean()
                .shift(1)  # Exclude the current day
                .reset_index(level=0, drop=True)
            )
            origin_rolling_avg_cols.append(origin_avg_col)

            # Rolling average for Destination-specific weather
            dest_col = f'Dest_{var}'
            dest_avg_col = f'{period}_avg_dest_{var.lower()}'
            dest_daily_avg[dest_avg_col] = (
                dest_daily_avg.groupby('Dest')[dest_col]
                .apply(lambda x: x.ffill().bfill())
                .rolling(window=window, min_periods=1)
                .mean()
                .shift(1)  # Exclude the current day
                .reset_index(level=0, drop=True)
            )
            dest_rolling_avg_cols.append(dest_avg_col)

    # Step 3: Merge rolling averages back to the full dataset using DuckDB
    conn = duckdb.connect()
    conn.register("df", df)
    conn.register("origin_avg", origin_daily_avg)
    conn.register("dest_avg", dest_daily_avg)

    # Select only rolling average columns (ignoring duplicate originals)
    origin_rolling_avgs = ", ".join([f"origin_avg.{col}" for col in origin_rolling_avg_cols])
    dest_rolling_avgs = ", ".join([f"dest_avg.{col}" for col in dest_rolling_avg_cols])

    df_merged = conn.execute(f"""
        SELECT df.*, 
               {origin_rolling_avgs},
               {dest_rolling_avgs}
        FROM df
        LEFT JOIN origin_avg 
        ON df.Origin = origin_avg.Origin AND df.FlightDate = origin_avg.FlightDate
        LEFT JOIN dest_avg 
        ON df.Dest = dest_avg.Dest AND df.FlightDate = dest_avg.FlightDate
    """).fetchdf()

    conn.close()
    
    return df_merged

def add_rolling_averages_delays(df, columns=['DepDelayMinutes'], windows={'weekly': 7, 'monthly': 30}):
    """Compute rolling averages in Pandas and use DuckDB for faster merging."""

    # Step 1: Aggregate to daily means per Origin
    daily_avg = df.groupby(['Origin', 'FlightDate'])[columns].mean().reset_index()

    # Step 2: Compute rolling averages on the smaller dataset
    rolling_avg_cols = []
    for period, window in windows.items():
        for col in columns:
            avg_col_name = f"{period}_avg_origin_{col.lower()}"
            daily_avg[avg_col_name] = (
                daily_avg.groupby('Origin')[col]
                .apply(lambda x: x.ffill().bfill())
                .rolling(window=window, min_periods=1)
                .mean()
                .shift(1)  # Excludes the current day from the rolling average
                .reset_index(level=0, drop=True)
            )
            rolling_avg_cols.append(avg_col_name)

    # Step 3: Merge rolling averages back to the full dataset using DuckDB
    conn = duckdb.connect()  # Establish DuckDB connection
    conn.register("df", df)  # Register Pandas DataFrame as DuckDB table
    conn.register("daily_avg", daily_avg)  # Register daily_avg table

    # Select only rolling averages from daily_avg to prevent duplicate columns
    select_rolling_avgs = ", ".join([f"daily_avg.{col}" for col in rolling_avg_cols])

    df_merged = conn.execute(f"""
        SELECT df.*, {select_rolling_avgs}
        FROM df
        LEFT JOIN daily_avg 
        ON df.Origin = daily_avg.Origin AND df.FlightDate = daily_avg.FlightDate
    """).fetchdf()  # Convert result back to Pandas DataFrame

    conn.close()  # Close DuckDB connection
    return df_merged

def add_rolling_flight_avg(df, column='DepDelayMinutes', windows=[10, 50, 100]):
    """Computes rolling average delay for past flights per Origin for multiple window sizes."""
    
    # Optimize memory usage
    df['Origin'] = df['Origin'].astype('category')

    # Sort by Origin, FlightDate, and TimeSinceMidnight for proper chronological order
    df.sort_values(by=['Origin', 'FlightDate', 'CRSDepTime'], inplace=True)

    # Compute rolling averages for each specified window size
    for window in windows:
        df[f'past_{window}_avg_delay'] = (
            df.groupby('Origin', observed=True)[column]
            .transform(lambda x: x.shift(1).rolling(window=window, min_periods=1).mean())
            .bfill().ffill()
        )

    df['Origin'] = df['Origin'].astype(str)

    return df

def add_cumulative_flight_count(df):
    """Adds a column counting the number of flights before each flight on the same day and at the same origin."""
    
    # Sort by Origin, FlightDate, and TimeSinceMidnight for correct order
    df.sort_values(by=['Origin', 'FlightDate', 'CRSDepTime'], inplace=True)
    
    # Compute cumulative count of flights before each flight on the same day and location
    df['cumulative_flights_before'] = df.groupby(['Origin', 'FlightDate']).cumcount()
    
    return df

def drop_and_scale(df, exclude_cols=[
    'Airline', 'Origin', 'Dest', 'AirTimeCategory', 'DistanceCategory',
    'CRSDepTime_sin', 'CRSDepTime_cos', 'CRSArrTime_sin', 'CRSArrTime_cos',
    'DayOfYear_sin', 'DayOfYear_cos', 'DayOfWeek_sin', 'DayOfWeek_cos',
    'Month_sin', 'Month_cos', 'Holiday_Indicator', 'Near_Holiday',
    'Weekend_Indicator', 'Working_Day', 'DepDelayMinutes', 'DepDel15']):

    """Drops all NaN values, scales numerical features"""
    # Drop NaN values
    df.dropna(inplace=True)

    # Identify numerical columns for scaling (excluding specified columns)
    num_cols = [col for col in df.columns if col not in exclude_cols]

    # Convert numerical columns to float BEFORE scaling to avoid dtype issues
    df[num_cols] = df[num_cols].astype(float)

    # Initialize scaler and scale numerical features
    scaler = StandardScaler()
    df.loc[:, num_cols] = scaler.fit_transform(df.loc[:, num_cols])

    return df

def train_test_split_encoder(df, cat_cols=["Airline", "Origin", "Dest", "AirTimeCategory", "DistanceCategory"], target_col="DepDel15"):
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
            final_df = add_rolling_averages_weather(final_df)
            final_df = add_rolling_averages_delays(final_df)
            final_df = add_rolling_flight_avg(final_df)
            final_df = add_cumulative_flight_count(final_df)
            final_df.drop('FlightDate', axis=1, inplace=True)
            progress.remove_task(rolling_task)
            rich_logger.info("Successfully applied rolling averages")
            file_logger.info("Successfully applied rolling averages")

            # Train-Test Split Step
            split_task = progress.add_task("Encoding and splitting data...")
            file_logger.info("Encoding and splitting data...")
            final_df = drop_and_scale(final_df)
            train_data, test_data = train_test_split_encoder(final_df)
            del final_df  # Free up memory
            progress.remove_task(split_task)
            rich_logger.info("Successfully encoded and split data")
            file_logger.info("Successfully encoded and split data")

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