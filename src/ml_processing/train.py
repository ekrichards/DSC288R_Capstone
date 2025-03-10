# ─── Load Libraries ──────────────────────────────────────────────────────────
import sys
import argparse
import pandas as pd
import pickle
import os
import warnings
from rich.progress import Progress, SpinnerColumn, TextColumn
from sklearn.ensemble import HistGradientBoostingRegressor, HistGradientBoostingClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression, SGDClassifier, SGDRegressor
from sklearn.neural_network import MLPRegressor, MLPClassifier

# ─── Load Utilities ──────────────────────────────────────────────────────────
# Define project root path and ensure utility modules are accessible
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)

# Import logging and configuration utilities
from utils.logger_helper import setup_loggers   # Handles log file and console logging
from utils.config_loader import load_yaml_files # Loads configuration settings from YAML files

# ─── Load Configuration ──────────────────────────────────────────────────────
# Load relevant configuration files
CONFIG_FILES = ["config/paths.yaml", "config/models.yaml"]
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
def train_model(model_name, base=False):
    """Trains the specified model using configuration from the YAML file.
    
    If `base=True`, the model is trained with default settings (no hyperparameters except `random_state=42` where applicable).
    """
    data = pd.read_parquet(SOURCE_PATH)
    model_config = MODEL_CONFIG.get(model_name)
    
    if not model_config:
        rich_logger.error(f"Model '{model_name}' not found in config file.")
        file_logger.error(f"Model '{model_name}' not found in config file.")
        return
    
    # # Check if the target column is DepDelayMinutes and filter accordingly
    # if model_config["target"] == "DepDelayMinutes":
    #     data = data[data["DepDel15"] == 1]

    # Select features (exclude the ones in "exclude_features")
    exclude_features = model_config.get("exclude_features", [])
    target_column = model_config["target"]
    features = [col for col in data.columns if col not in exclude_features + [target_column]]
    X = data[features]
    y = data[target_column]
    
    # Select parameters based on `base` mode
    if base:
        model_params = {"random_state": 42} if "random_state" in model_config.get("params", {}) else {}
    else:
        model_params = model_config.get("params", {})

    # Model selection
    if model_name == "lin_reg":
        model = LinearRegression(**model_params)
    elif model_name == "log_reg":
        model = LogisticRegression(**model_params)
    elif model_name == "hgb_reg":
        model = HistGradientBoostingRegressor(**model_params)
    elif model_name == "hgb_clf":
        model = HistGradientBoostingClassifier(**model_params)
    elif model_name == "sgd_clf":
        model = SGDClassifier(**model_params)
    elif model_name == "sgd_reg":
        model = SGDRegressor(**model_params)
    elif model_name == "mlp_reg":
        model = MLPRegressor(**model_params)
    elif model_name == "mlp_clf":
        model = MLPClassifier(**model_params)
    else:
        rich_logger.error("Unsupported model type")
        file_logger.error("Unsupported model type")
        return
    
    mode_label = "base model" if base else "parameter-tuned model"
    rich_logger.info(f"Starting {mode_label} training for {model_name}")
    file_logger.info(f"Starting {mode_label} training for {model_name}")
    
    with Progress(SpinnerColumn(), TextColumn("{task.description}")) as progress:
        train_task = progress.add_task(f"Training {model_name} ({mode_label})...")
        file_logger.info(f"Training {model_name} ({mode_label})...")

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
        rich_logger.info(f"Successfully trained {model_name} ({mode_label})")
        file_logger.info(f"Successfully trained {model_name} ({mode_label})")
    
    # Create model-specific folder
    model_dir = os.path.join(SAVE_DIR, model_name)
    os.makedirs(model_dir, exist_ok=True)

    # Save model
    save_task = progress.add_task(f"Saving {model_name} ({mode_label}) model...")
    file_logger.info(f"Saving {model_name} ({mode_label}) model...")
    
    model_suffix = "_base" if base else ""
    model_filename = f"{model_name}{model_suffix}.pkl"
    model_path = os.path.join(model_dir, model_filename)
    
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    
    progress.remove_task(save_task)
    
    rich_logger.info(f"Model saved to {model_path}")
    file_logger.info(f"Model saved to {model_path}")

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train various machine learning models using the specified configuration."
    )
    parser.add_argument(
        "--model", 
        type=str, 
        required=True, 
        choices=[
            "lin_reg", "log_reg", 
            "hgb_reg", "hgb_clf", 
            "sgd_reg", "sgd_clf", 
            "mlp_reg", "mlp_clf"
        ],
        help=(
            "Specify the model to train:\n"
            "  lin_reg   - Linear Regression\n"
            "  log_reg   - Logistic Regression\n"
            "  hgb_reg   - HistGradientBoosting Regressor\n"
            "  hgb_clf   - HistGradientBoosting Classifier\n"
            "  sgd_reg   - Stochastic Gradient Descent Regressor\n"
            "  sgd_clf   - Stochastic Gradient Descent Classifier\n"
            "  mlp_reg   - Multi-Layer Perceptron Regressor\n"
            "  mlp_clf   - Multi-Layer Perceptron Classifier\n"
        )
    )
    
    parser.add_argument(
        "--base", 
        action="store_true", 
        help="Train the model with default hyperparameters (random_state=42 if applicable)."
    )
    parser.add_argument("--base", action="store_true", help="Train the model without hyperparameters (default: uses parameters from config)")
    args = parser.parse_args()
    
    train_model(args.model, base=args.base)
