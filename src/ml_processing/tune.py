import sys
import argparse
import pandas as pd
import pickle
import os
import warnings
import json
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from sklearn.model_selection import GridSearchCV, ParameterGrid
from sklearn.linear_model import LinearRegression, LogisticRegression, SGDClassifier
from sklearn.ensemble import HistGradientBoostingRegressor

# Load utilities
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from utils.logger_helper import setup_loggers
from utils.config_loader import load_yaml_files

# Load configuration
CONFIG_FILES = ["config/paths.yaml", "config/models/base.yaml"]
config = load_yaml_files(CONFIG_FILES)

SOURCE_PATH = config["paths"]["final_train"]
MODEL_CONFIG = config["models"]
SAVE_DIR = config["paths"]["trained_models"]
os.makedirs(SAVE_DIR, exist_ok=True)

# Setup loggers
LOG_FILENAME = "train_models_tuning"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

def train_model_with_tuning(model_name):
    """Trains the specified model with hyperparameter tuning."""
    data = pd.read_parquet(SOURCE_PATH)
    model_config = MODEL_CONFIG.get(model_name)
    
    if not model_config:
        rich_logger.error(f"Model '{model_name}' not found in config file.")
        file_logger.error(f"Model '{model_name}' not found in config file.")
        return
    
    exclude_features = model_config.get("exclude_features", [])
    target_column = model_config["target"]
    features = [col for col in data.columns if col not in exclude_features + [target_column]]
    X = data[features]
    y = data[target_column]
    
    param_grid = model_config.get("param_grid", {})
    total_combinations = len(list(ParameterGrid(param_grid)))
    
    # Model selection with explicit parameter handling
    if model_name == "linear_regression":
        model = LinearRegression()
    elif model_name == "logistic_regression":
        model = LogisticRegression()
    elif model_name == "histgradientboosting_regression":
        model = HistGradientBoostingRegressor()
    elif model_name == "sgd_classifier":
        model = SGDClassifier()
    else:
        rich_logger.error("Unsupported model type")
        file_logger.error("Unsupported model type")
        return
    
    rich_logger.info(f"Starting hyperparameter tuning for {model_name}")
    file_logger.info(f"Starting hyperparameter tuning for {model_name}")
    
    with Progress(SpinnerColumn(), BarColumn(), TextColumn("{task.description}")) as progress:
        tuning_task = progress.add_task(f"Tuning {model_name} model...", total=total_combinations)
        file_logger.info(f"Total hyperparameter combinations: {total_combinations}")
        
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            try:
                grid_search = GridSearchCV(model, param_grid, cv=5, n_jobs=-1, verbose=3)
                
                for i, params in enumerate(ParameterGrid(param_grid)):
                    param_task = progress.add_task(f"Training {model_name} ({i+1}/{total_combinations})", total=1)
                    rich_logger.info(f"Training model {i+1}/{total_combinations} with params: {params}")
                    file_logger.info(f"Training model {i+1}/{total_combinations} with params: {params}")
                    
                    progress.update(param_task, advance=1)
                    progress.remove_task(param_task)
                
                grid_search.fit(X, y)
            except Exception as e:
                rich_logger.error(f"Tuning failed for {model_name}: {e}")
                file_logger.error(f"Tuning failed for {model_name}: {e}")
                raise
            
            for warning in w:
                warning_message = f"{warning.category.__name__}: {warning.message}"
                rich_logger.warning(warning_message)
                file_logger.warning(warning_message)
        
        progress.remove_task(tuning_task)
        rich_logger.info(f"Best parameters found: {grid_search.best_params_}")
        file_logger.info(f"Best parameters found: {grid_search.best_params_}")
        
    # Save best parameters
    param_filename = f"{model_name}_tuning_params.json"
    param_path = os.path.join(SAVE_DIR, param_filename)
    with open(param_path, "w") as f:
        json.dump(grid_search.best_params_, f, indent=4)
    rich_logger.info(f"Best parameters saved to {param_path}")
    file_logger.info(f"Best parameters saved to {param_path}")
    
    # Save best model
    model_filename = f"{model_name}_tuning.pkl"
    model_path = os.path.join(SAVE_DIR, model_filename)
    with open(model_path, "wb") as f:
        pickle.dump(grid_search.best_estimator_, f)
    
    rich_logger.info(f"Tuned model saved to {model_path}")
    file_logger.info(f"Tuned model saved to {model_path}")

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True, help="Model type (linear_regression, logistic_regression, histgradientboosting_regression, sgd_classifier)")
    args = parser.parse_args()
    
    train_model_with_tuning(args.model)
