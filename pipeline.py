# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import subprocess
import argparse

# ─── Load Utilities ──────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))
sys.path.append(PROJECT_ROOT)

# Import logging utilities
from utils.logger_helper import setup_loggers  # Handles log file and console logging
from utils.config_loader import load_yaml_files  # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = ["config/models.yaml"]
config = load_yaml_files(CONFIG_FILES)  # Load YAML configs

# Get the list of available models from config.yaml
AVAILABLE_MODELS = list(config["models"].keys())
CLASSIFICATION_MODELS = [m for m in AVAILABLE_MODELS if config["models"][m]["type"] == "clf"]
REGRESSION_MODELS = [m for m in AVAILABLE_MODELS if config["models"][m]["type"] == "reg"]

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
        rich_logger.error(f"Error executing: {command}")
        file_logger.error(f"Error executing: {command}")
        sys.exit(1)

def run_data_steps():
    """Runs all data preprocessing steps in a defined order."""
    data_steps = [
        "python src/data_processing/download_flight_data.py",
        "python src/data_processing/download_noaa_data.py",
        "python src/data_processing/extract_noaa_data.py",
        "python src/data_processing/extract_flight_data.py",
        "python src/data_processing/process_noaa_data.py",
        "python src/data_processing/process_flight_data.py",
        "python src/data_processing/final_data.py",
    ]

    rich_logger.info("Running all data processing steps in sequence")
    file_logger.info("Running all data processing steps in sequence")

    for step in data_steps:
        run_step(step)

    rich_logger.info("Data processing completed successfully!")
    file_logger.info("Data processing completed successfully!")

def get_model_list(selection):
    """Returns the appropriate list of models based on user selection."""
    if selection == "all":
        return AVAILABLE_MODELS
    elif selection == "clf":
        return CLASSIFICATION_MODELS
    elif selection == "reg":
        return REGRESSION_MODELS
    elif selection in AVAILABLE_MODELS:
        return [selection]
    else:
        rich_logger.error(f"Invalid model selection: {selection}")
        file_logger.error(f"Invalid model selection: {selection}")
        sys.exit(1)

def train_model(model, base=False):
    """Trains a specific model with or without hyperparameters."""
    if model not in AVAILABLE_MODELS:
        rich_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        file_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        sys.exit(1)
    
    base_flag = "--base" if base else ""
    run_step(f"python src/ml_processing/train.py --model {model} {base_flag}")

def train_models(selection, base=False):
    """Trains all available models with or without hyperparameters."""
    models = get_model_list(selection)
    mode_label = "base models" if base else "parameter-tuned models"
    rich_logger.info(f"Training {selection} ({mode_label})")
    file_logger.info(f"Training {selection} ({mode_label})")
    for model in models:
        train_model(model, base=base)

def tune_model(model):
    """Tunes hyperparameters for a specific model."""
    if model not in AVAILABLE_MODELS:
        rich_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        file_logger.error(f"Model '{model}' not found. Available models: {AVAILABLE_MODELS}")
        sys.exit(1)
    run_step(f"python src/ml_processing/tune.py --model {model}")

def tune_models(selection):
    """Tunes hyperparameters for all available models."""
    models = get_model_list(selection)
    rich_logger.info(f"Tuning hyperparameters for {selection}")
    file_logger.info(f"Tuning hyperparameters for {selection}")
    for model in models:
        tune_model(model)

def run_pipeline():
    """Runs the full pipeline: data processing and model training (parameter-tuned versions)."""
    rich_logger.info("Running full pipeline (data processing + model training)")
    file_logger.info("Running full pipeline (data processing + model training)")
    
    run_data_steps()
    train_models("all", base=False)

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ML Pipeline CLI")
    
    parser.add_argument("--data", action="store_true", help="Run all data preprocessing steps")
    parser.add_argument("--train", choices=AVAILABLE_MODELS + ["all", "clf", "reg"], help="Train a specific model or all models")
    parser.add_argument("--tune", choices=AVAILABLE_MODELS + ["all", "clf", "reg"], help="Tune hyperparameters for a specific model or all models")
    parser.add_argument("--base", action="store_true", help="Train models without hyperparameters")
    parser.add_argument("--run", action="store_true", help="Run full data processing and train all parameter-tuned models")

    args = parser.parse_args()

    # Execute based on arguments
    if args.run:
        run_pipeline()
    
    if args.data:
        run_data_steps()

    if args.train:
        train_models(args.train, base=args.base)

    if args.tune:
        tune_models(args.tune)