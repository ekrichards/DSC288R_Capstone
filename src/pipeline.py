# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import subprocess
import argparse

# ─── Load Utilities ──────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
sys.path.append(PROJECT_ROOT)

# Import logging utilities
from utils.logger_helper import setup_loggers  # Handles log file and console logging
from utils.config_loader import load_yaml_files  # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = ["config/models/base.yaml"]
config = load_yaml_files(CONFIG_FILES)  # Load YAML configs

# Get the list of available models from config.yaml
AVAILABLE_MODELS = list(config["models"].keys())

# ─── Setup Loggers ───────────────────────────────────────────────────────────
LOG_FILENAME = "pipeline"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Utility Functions ───────────────────────────────────────────────────────
def run_step(command):
    """Runs a shell command and handles errors."""
    rich_logger.info(f"Executing: {command}")
    file_logger.info(f"Executing: {command}")
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        rich_logger.error(f"Error executing: {command}. Exiting.")
        file_logger.error(f"Error executing: {command}. Exiting.")
        sys.exit(1)

def run_data_steps():
    """Runs all data preprocessing steps in a defined order."""
    data_steps = [
        "python src/data_processing/download_noaa_data.py",
        "python src/data_processing/extract_noaa_data.py",
        "python src/data_processing/extract_flight_data.py",
        "python src/data_processing/process_noaa_data.py",
        "python src/data_processing/process_flight_data.py",
        "python src/data_processing/final_data.py",
    ]

    rich_logger.info("Running all data processing steps in sequence...")
    file_logger.info("Running all data processing steps in sequence...")

    for step in data_steps:
        run_step(step)

    rich_logger.info("Data processing completed successfully!")
    file_logger.info("Data processing completed successfully!")

def train_model(model):
    """Trains a specific model."""
    if model not in AVAILABLE_MODELS:
        rich_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        file_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        sys.exit(1)
    run_step(f"python src/ml_processing/train.py --model {model}")

def train_all_models():
    """Trains all available models."""
    rich_logger.info("Training all models...")
    file_logger.info("Training all models...")
    for model in AVAILABLE_MODELS:
        train_model(model)

def tune_model(model):
    """Tunes hyperparameters for a specific model."""
    if model not in AVAILABLE_MODELS:
        rich_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        file_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        sys.exit(1)
    run_step(f"python src/ml_processing/tune.py --model {model}")

def tune_all_models():
    """Tunes hyperparameters for all available models."""
    rich_logger.info("Tuning hyperparameters for all models...")
    file_logger.info("Tuning hyperparameters for all models...")
    for model in AVAILABLE_MODELS:
        tune_model(model)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML Pipeline CLI")
    
    parser.add_argument("--data", action="store_true", help="Run all data preprocessing steps")
    parser.add_argument("--train", choices=AVAILABLE_MODELS + ["all"], help="Train a specific model or all models")
    parser.add_argument("--tune", choices=AVAILABLE_MODELS + ["all"], help="Tune hyperparameters for a specific model or all models")

    args = parser.parse_args()

    # Execute based on arguments
    if args.data:
        run_data_steps()

    if args.train:
        if args.train == "all":
            train_all_models()
        else:
            train_model(args.train)

    if args.tune:
        if args.tune == "all":
            tune_all_models()
        else:
            tune_model(args.tune)
