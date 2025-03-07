# ─── train.py ──────────────────────────────────────────────────────────────────
import os
import sys
import argparse
import pickle
import importlib

# Ensure the project root is on sys.path so we can do importlib on src.models.*
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
sys.path.append(PROJECT_ROOT)

from utils.logger_helper import setup_loggers
from utils.config_loader import load_yaml_files

# Load configuration
CONFIG_FILES = [
    "config/paths.yaml",
    "config/process/base.yaml",
    "config/models/base.yaml"
]
config = load_yaml_files(CONFIG_FILES)

# The directory where we'll save the .pkl files:
MODEL_SAVE_DIR = os.path.join(PROJECT_ROOT, "models")
os.makedirs(MODEL_SAVE_DIR, exist_ok=True)

# Set up logging
LOG_FILENAME = "train_models"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# A “factory” that maps model names to Python module import paths
MODEL_FACTORY = {
    "linear_regression": "src.ml_processing.models.linear_regression",
    "logistic_regression": "src.ml_processing.models.logistic_regression",
    # "random_forest": "src.models.random_forest",
    # etc...
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train a specific model.")
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        choices=MODEL_FACTORY.keys(),
        help="Model name to train (e.g. linear_regression, logistic_regression)"
    )
    args = parser.parse_args()

    model_name = args.model
    model_module_path = MODEL_FACTORY[model_name]

    # Dynamically import the selected model file
    file_logger.info(f"Importing {model_module_path} for training...")
    try:
        model_module = importlib.import_module(model_module_path)
    except ImportError as e:
        file_logger.error(f"Could not import {model_module_path}: {e}")
        sys.exit(1)

    # Grab hyperparameters for this model from config
    model_params = config["models"].get(model_name, {})
    file_logger.info(f"Using hyperparams: {model_params}")

    # Train the model by calling the model’s train function
    rich_logger.info(f"Training {model_name} ...")
    model = model_module.train_model(config, model_params)

    # Save the resulting model to the "models/" folder
    model_save_path = os.path.join(MODEL_SAVE_DIR, f"{model_name}.pkl")
    with open(model_save_path, "wb") as f:
        pickle.dump(model, f)

    file_logger.info(f"Saved {model_name} model to {model_save_path}")
    rich_logger.info(f"Done training {model_name}!")
