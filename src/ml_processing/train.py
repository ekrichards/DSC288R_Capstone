# ─── Load Libraries ──────────────────────────────────────────────────────────
import sys
import argparse
import pandas as pd
import pickle
import os
import warnings
from rich.progress import Progress, SpinnerColumn, TextColumn
from sklearn.linear_model import LinearRegression, LogisticRegression, SGDClassifier
from sklearn.ensemble import HistGradientBoostingRegressor

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

# Import logging and configuration utilities
from utils.logger_helper import setup_loggers   # Handles log file and console logging
from utils.config_loader import load_yaml_files # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
# Load relevant configuration files
CONFIG_FILES = ["config/paths.yaml", "config/models/base.yaml"]
config = load_yaml_files(CONFIG_FILES)

# Extract key configuration values
SOURCE_PATH = config["paths"]["final_train"]    # Path to the parquet file containing training data
MODEL_CONFIG = config["models"]                 # Model parameters and features
SAVE_DIR = config["paths"]["trained_models"]    # Directory where trained models will be saved

# Ensure the models directory exists
os.makedirs(SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
# Initialize logging for console (rich_logger) and file output (file_logger)
LOG_FILENAME = "train_models"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Training Function ───────────────────────────────────────────────────────
def train_model(model_name):
    """Trains the specified model using configuration from the YAML file."""
    data = pd.read_parquet(SOURCE_PATH)
    model_config = MODEL_CONFIG.get(model_name)
    
    if not model_config:
        rich_logger.error(f"Model '{model_name}' not found in config file.")
        file_logger.error(f"Model '{model_name}' not found in config file.")
    
    # Select features (exclude the ones in "exclude_features")
    exclude_features = model_config.get("exclude_features", [])
    target_column = model_config["target"]
    
    features = [col for col in data.columns if col not in exclude_features + [target_column]]
    X = data[features]
    y = data[target_column]
    
    # Model selection
    if model_name == "linear_regression":
        model = LinearRegression()
    elif model_name == "logistic_regression":
        model = LogisticRegression(**model_config["params"])
    elif model_name == "histgradientboosting_regression":
        model = HistGradientBoostingRegressor(**model_config["params"])
    elif model_name == "sgd_classifier":
        model = SGDClassifier(**model_config["params"])
    else:
        rich_logger.error("Unsupported model type")
        file_logger.error("Unsupported model type")
    
    rich_logger.info(f"Starting {model_name} model training")
    file_logger.info(f"Starting {model_name} model training")
    
    with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
        train_task = progress.add_task(f"Training {model_name} model...")
        file_logger.info(f"Training {model_name} model...")

        # Capture warnings (e.g., max iterations reached) and log them
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                model.fit(X, y)
            except Exception as e:
                rich_logger.error(f"Training failed for {model_name}: {e}")
                file_logger.error(f"Training failed for {model_name}: {e}")
                raise
            
            for warning in w:
                warning_message = f"{warning.category.__name__}: {warning.message}"
                rich_logger.warning(warning_message)
                file_logger.warning(warning_message)
                
        progress.remove_task(train_task)
        rich_logger.info(f"Successfully trained {model_name}")
        file_logger.info(f"Successfully trained {model_name}")

    # Save model
    save_task = progress.add_task(f"Saving {model_name} model...")
    file_logger.info(f"Saving {model_name} model...")
    model_filename = f"{model_name}.pkl"
    model_path = os.path.join(SAVE_DIR, model_filename)
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    progress.remove_task(save_task)
    
    rich_logger.info(f"Model saved to {model_path}")
    file_logger.info(f"Model saved to {model_path}")

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True, help="Model type (linear_regression, logistic_regression, histgradientboosting_regression, sgd_classifier)")
    args = parser.parse_args()
    
    train_model(args.model)