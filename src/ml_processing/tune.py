# ─── Load Libraries ──────────────────────────────────────────────────────────
import sys
import argparse
import pandas as pd
import pickle
import os
import warnings
from sklearn.model_selection import RandomizedSearchCV, ParameterGrid, train_test_split
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
LOG_FILENAME = "tune_hyperparameters"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Hyperparameter Tuning Function ──────────────────────────────────────────
def train_model_with_tuning(model_name):
    """Trains the specified model with hyperparameter tuning."""
    data = pd.read_parquet(SOURCE_PATH)
    model_config = MODEL_CONFIG.get(model_name)
    
    if not model_config:
        rich_logger.error(f"Model '{model_name}' not found in config file")
        file_logger.error(f"Model '{model_name}' not found in config file")
        return

    # # Check if the target column is DepDelayMinutes and filter accordingly
    # if model_config["type"] == "reg":
    #     data = data[data["DepDel15"] == 1]
    
    data, _ = train_test_split(data, test_size=0.90, random_state=42, stratify=data['DepDel15'])
    rich_logger.info(f"Sampled 10% of data set for faster tuning")
    file_logger.info(f"Sampled 10% of data set for faster tuning")
    
    # Select features (exclude the ones in "exclude_features")
    exclude_features = model_config.get("exclude_features", [])
    target_column = model_config["target"]
    features = [col for col in data.columns if col not in exclude_features + [target_column]]
    X = data[features]
    y = data[target_column]
    
    param_dist = model_config.get("param_dist", {})
    param_combinations = list(ParameterGrid(param_dist))  # Full parameter space
    total_param_space = len(param_combinations)
    total_samples = model_config.get("n_iter", 10)  # Number of random samples to draw
    
    # Model selection
    if model_name == "lin_reg":
        model = LinearRegression()
    elif model_name == "log_reg":
        model = LogisticRegression()
    elif model_name == "hgb_reg":
        model = HistGradientBoostingRegressor()
    elif model_name == "hgb_clf":
        model = HistGradientBoostingClassifier()
    elif model_name == "sgd_clf":
        model = SGDClassifier()
    elif model_name == "sgd_reg":
        model = SGDRegressor()
    elif model_name == "mlp_reg":
        model = MLPRegressor()
    elif model_name == "mlp_clf":
        model = MLPClassifier()
    else:
        rich_logger.error("Unsupported model type")
        file_logger.error("Unsupported model type")
        return
    
    rich_logger.info(f"Starting randomized hyperparameter tuning for {model_name}")
    file_logger.info(f"Starting randomized hyperparameter tuning for {model_name}")
    file_logger.info(f"Total possible hyperparameter combinations: {total_param_space}")
    file_logger.info(f"Randomly sampling {total_samples} hyperparameter sets for tuning")
    
    # # Log planned runs before training starts
    # file_logger.info("All possible hyperparameter combinations:")
    # for i, params in enumerate(param_combinations):
    #     file_logger.info(f"  [{i+1}/{total_param_space}] {params}")
    
    print("--VERBOSE OUTPUT BEGIN--")

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        try:
            grid_search = RandomizedSearchCV(model, param_distributions=param_dist, n_iter=total_samples, cv=5, n_jobs=-1, verbose=3, random_state=42)
            grid_search.fit(X, y)  # Will let verbose print to terminal, not captured
            print("--VERBOSE OUTPUT END--")
        except Exception as e:
            rich_logger.error(f"Tuning failed for {model_name}: {e}")
            file_logger.error(f"Tuning failed for {model_name}: {e}")
            raise
        
        for warning in w:
            warning_message = f"{warning.category.__name__}: {warning.message}"
            rich_logger.warning(warning_message)
            file_logger.warning(warning_message)

    # Resume structured logging after training
    rich_logger.info(f"Best parameters found: {grid_search.best_params_}")
    file_logger.info(f"Best parameters found: {grid_search.best_params_}")


    # Convert cv_results_ to a DataFrame for better readability
    cv_results_df = pd.DataFrame(grid_search.cv_results_)

    # Select relevant columns (parameters and mean test scores)
    param_columns = [col for col in cv_results_df.columns if col.startswith("param_")]
    results_df = cv_results_df[param_columns + ["mean_test_score"]]

    # Log all searched parameters and their scores
    rich_logger.info(f"All parameters searched and their scores:\n{results_df.to_string(index=False)}")
    file_logger.info(f"All parameters searched and their scores:\n{results_df.to_string(index=False)}")
    

    # Create model-specific folder
    model_dir = os.path.join(SAVE_DIR, model_name)
    os.makedirs(model_dir, exist_ok=True)

    # Save full grid search object
    gridsearch_filename = f"{model_name}_random_search.pkl"
    gridsearch_path = os.path.join(model_dir, gridsearch_filename)
    with open(gridsearch_path, "wb") as f:
        pickle.dump(grid_search, f)
        
    rich_logger.info(f"Full grid search object saved to {gridsearch_path}")
    file_logger.info(f"Full grid search object saved to {gridsearch_path}")
    
    # # Save the best model
    # model_filename = f"{model_name}_best_tuned.pkl"
    # model_path = os.path.join(model_dir, model_filename)
    # with open(model_path, "wb") as f:
    #     pickle.dump(grid_search.best_estimator_, f)
    
    # rich_logger.info(f"Tuned model saved to {model_path}")
    # file_logger.info(f"Tuned model saved to {model_path}")

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Hyperparameter tune various machine learning models using a subset of the data."
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
            "Specify the model to tune:\n"
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
    args = parser.parse_args()
    
    train_model_with_tuning(args.model)
