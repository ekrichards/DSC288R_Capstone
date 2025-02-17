import os
import pandas as pd
import yaml
from tqdm import tqdm
import numpy as np
from sklearn.utils import resample
import holidays

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

# Extract flight data settings
RAW_DIR = config["paths"]["extracted_flight_data"]    # Raw flight data directory
CLEAN_DIR = config["paths"]["processed_flight_data"]  # Directory to save cleaned data
DELETE_PQ = config["flight_data"]["delete_pq"]        # Delete raw Parquet files after processing?
KEEP_COLUMNS = config["flight_data"]["keep_columns"]  # Columns to keep

# Ensure the clean directory exists
os.makedirs(CLEAN_DIR, exist_ok=True)

def clean_flight_file(file_path):
    """Reads, cleans, and transforms a flight data Parquet file, then saves it as processed_flight_<year>.parquet."""
    
    # 1. Parse the filename to extract the year
    filename = os.path.basename(file_path)  # e.g. "extracted_flight_2018.parquet"
    # Assuming the input filenames always follow the pattern extracted_flight_<year>.parquet:
    year_str = filename.replace("extracted_flight_", "").replace(".parquet", "")
    
    # 2. Construct the new output filename
    new_filename = f"processed_flight_{year_str}.parquet"
    save_path = os.path.join(CLEAN_DIR, new_filename)

    # 3. Read Parquet file with selected columns
    df = pd.read_parquet(file_path, columns=KEEP_COLUMNS)

    # 4. Convert FlightDate to datetime (if needed)
    if "FlightDate" in df.columns:
        df["FlightDate"] = df["FlightDate"].astype('datetime64[s]')

    # 5. Create new categorical columns (make sure required columns exist in KEEP_COLUMNS)
    
    # -- a) DelayCategory based on DepDelay
    if "DepDelay" in df.columns:
        # On-time if DepDelay ≤ 0
        # Moderate Delay if 0 < DepDelay < 60
        # Severe Delay if DepDelay ≥ 60
        conditions = [
            (df["DepDelay"] <= 0),
            (df["DepDelay"] > 0) & (df["DepDelay"] < 60),
            (df["DepDelay"] >= 60),
        ]
        categories = ["On-time", "Moderate Delay", "Severe Delay"]
        df["DelayCategory"] = np.select(conditions, categories, default="Unknown")
    
    # -- b) AirTimeCategory based on AirTime
    if "AirTime" in df.columns:
        # Short if < 120
        # Medium if 120 ≤ AirTime < 360
        # Long if ≥ 360
        conditions = [
            (df["AirTime"] < 120),
            (df["AirTime"] >= 120) & (df["AirTime"] < 360),
            (df["AirTime"] >= 360),
        ]
        categories = ["Short", "Medium", "Long"]
        df["AirTimeCategory"] = np.select(conditions, categories, default="Unknown")

    # -- c) TimeofDay based on DepTime
    #    Early Morning if DepTime < 600
    #    Morning if 600 ≤ DepTime < 1200
    #    Afternoon if 1200 ≤ DepTime < 1800
    #    Evening if DepTime ≥ 1800
    if "DepTime" in df.columns:
        conditions = [
            (df["DepTime"] < 600),
            (df["DepTime"] >= 600) & (df["DepTime"] < 1200),
            (df["DepTime"] >= 1200) & (df["DepTime"] < 1800),
            (df["DepTime"] >= 1800),
        ]
        categories = ["Early Morning", "Morning", "Afternoon", "Evening"]
        df["TimeofDay"] = np.select(conditions, categories, default="Unknown")
    
    if "DepDel15" in df.columns:
        # Separate delayed and non-delayed flights
        delayed_flights = df[df['DepDel15'] == 1]
        on_time_flights = df[df['DepDel15'] == 0]

        # Undersample non-delayed to match delayed (1:1 ratio)
        on_time_flights_sampled = resample(on_time_flights,
                                        replace=False,  # No duplicates
                                        n_samples=len(delayed_flights),  # Match delayed count
                                        random_state=42)

        # Combine undersampled data
        df = pd.concat([delayed_flights, on_time_flights_sampled])

    # d) Added Random Under Sampling
    if "DepDel15" in df.columns:
        # Separate delayed and non-delayed flights
        delayed_flights = df[df['DepDel15'] == 1]
        on_time_flights = df[df['DepDel15'] == 0]

        # Undersample non-delayed to match delayed (1:1 ratio)
        on_time_flights_sampled = resample(on_time_flights,
                                        replace=False,  # No duplicates
                                        n_samples=len(delayed_flights),  # Match delayed count
                                        random_state=42)

        # Combine undersampled data
        df = pd.concat([delayed_flights, on_time_flights_sampled])

    # e) Cyclical Data
    if "Month" in df.columns:
        # Transform Month into cyclical features
        df['Month_sin'] = np.sin(2 * np.pi * df['Month'] / 12)
        df['Month_cos'] = np.cos(2 * np.pi * df['Month'] / 12)

    if "DayOfWeek" in df.columns:
        # Transform DayOfWeek into cyclical features
        df['DayOfWeek_sin'] = np.sin(2 * np.pi * df['DayOfWeek'] / 7)
        df['DayOfWeek_cos'] = np.cos(2 * np.pi * df['DayOfWeek'] / 7)

    if "FlightDate" in df.columns:
        # Convert FlightDate to day-of-year (if applicable)
        df['FlightDate'] = pd.to_datetime(df['FlightDate'])
        df['DayOfYear'] = df['FlightDate'].dt.dayofyear 

        # Transform DayOfYear into cyclical features
        df['DayOfYear_sin'] = np.sin(2 * np.pi * df['DayOfYear'] / 365)
        df['DayOfYear_cos'] = np.cos(2 * np.pi * df['DayOfYear'] / 365)

    # f) Holidays/Near Holidays
    if "FlightDate" in df.columns:
        # Load US holidays for 2018-2022 and ensure dates are in `datetime64[ns]`
        us_holidays = {pd.to_datetime(k): v for k, v in holidays.US(years=range(2018, 2023)).items()}

        # Convert to a list of dates
        holiday_dates_dynamic = pd.to_datetime(list(us_holidays.keys()))

        # Create a Holiday Indicator (1 if flight is on a holiday, else 0)
        df['Holiday_Indicator'] = df['FlightDate'].isin(holiday_dates_dynamic).astype(int)

        def is_near_holiday(flight_date, holiday_dict, days=3):
            """
            Checks if flight_date is within `days` of any holiday in the provided holiday dictionary.
            
            Parameters:
            - flight_date (str or datetime): The flight date to check.
            - holiday_dict (dict): Dictionary of holidays (date -> holiday name).
            - days (int): Number of days to consider as "near" a holiday.
            
            Returns:
            - tuple (bool, str): Whether it's near a holiday and the holiday info.
            """
            flight_date = pd.to_datetime(flight_date)  # Convert input to Pandas datetime64

            for holiday_date, holiday_name in holiday_dict.items():
                if abs((flight_date - holiday_date).days) <= days:
                    return (True, f"Near {holiday_date.strftime('%Y-%m-%d')} ({holiday_name})")
            
            return (False, "Not near a holiday")

        df['Near_Holiday'] = df['FlightDate'].apply(lambda x: is_near_holiday(x, us_holidays)[0])


    # 6. Save cleaned file as Parquet with the new filename
    df.to_parquet(save_path, index=False)

    # 7. Delete raw Parquet file if DELETE_PQ is set to True
    if DELETE_PQ:
        os.remove(file_path)
        tqdm.write(f"Deleted raw Parquet file: {file_path}")

    return save_path

if __name__ == "__main__":
    tqdm.write(f"Cleaning flight data in {RAW_DIR}")

    # Process all Parquet files in the raw directory
    flight_files = [
        os.path.join(RAW_DIR, f) 
        for f in os.listdir(RAW_DIR) 
        if f.endswith(".parquet")
    ]

    for file_path in tqdm(flight_files, desc="Processing flight files"):
        save_path = clean_flight_file(file_path)
        tqdm.write(f"Saved cleaned file: {save_path}")

    tqdm.write("All flight data cleaned and saved.")
