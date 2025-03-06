# ─── Load Libraries ──────────────────────────────────────────────────────────
import os
import sys
import pickle
import argparse

# ─── Load Utilities ──────────────────────────────────────────────────────────
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "."))
sys.path.append(PROJECT_ROOT)

from utils.logger_helper import setup_loggers
from utils.config_loader import load_yaml_files

# If you have your own data pipeline function somewhere, import it:
# from data.data_pipeline import load_data  # Example
def load_data():
    """
    Replace with your real data loading & feature engineering steps.
    For demonstration, returns X_train, y_train, X_val, y_val, etc.
    """
    # Dummy placeholder
    import numpy as np
    np.random.seed(42)
    X_train = np.random.rand(100, 5)
    y_train = np.random.randint(0, 2, size=100)
    X_val = np.random.rand(20, 5)
    y_val = np.random.randint(0, 2, size=20)
    return X_train, y_train, X_val, y_val

# ─── Load Configuration ──────────────────────────────────────────────────────
CONFIG_FILES = [
    "config/paths.yaml",
    "config/process/base.yaml",
    "config/model/base.yaml"
]
config = load_yaml_files(CONFIG_FILES)

MODEL_SAVE_DIR = config["paths"]["trained_models"]
os.makedirs(MODEL_SAVE_DIR, exist_ok=True)

# ─── Setup Loggers ───────────────────────────────────────────────────────────
LOG_FILENAME = "train_models"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

# ─── Define a Factory Mapping ────────────────────────────────────────────────
# Instead of multiple if/elif, you can do something like this:
MODEL_FACTORY = {
    "linear_regression": "models.linear_regression",
    "logistic_regression": "models.logistic_regression",
    # "random_forest_reg": "models.random_forest_regressor",
    # "random_forest_clf": "models.random_forest_classifier",
    # etc.
}

# ─── Main Execution ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train a specific model")
    parser.add_argument(
        "--model", 
        type=str, 
        required=True, 
        choices=MODEL_FACTORY.keys(), 
        help="Which model to train"
    )
    args = parser.parse_args()

    # 1. Load data
    X_train, y_train, X_val, y_val = load_data()

    # 2. Dynamically import the selected model’s training function
    model_module_path = MODEL_FACTORY[args.model]
    # e.g. model_module_path = "models.linear_regression"

    file_logger.info(f"Importing model module: {model_module_path}")
    model_module = __import__(model_module_path, fromlist=["train_model"])  
    # fromlist=["train_model"] ensures we can access model_module.train_model

    # 3. Load the relevant hyperparams from config
    # We'll assume you have config["models"]["linear_regression"], etc.
    model_params = config["models"].get(args.model, {})
    file_logger.info(f"Found model params in config: {model_params}")

    # 4. Train the model
    rich_logger.info(f"Training {args.model}...")
    model = model_module.train_model(X_train, y_train, model_params)

    # 5. (Optional) You might do validation or custom steps here
    # We are skipping full testing/evaluation at this time

    # 6. Save the model to disk
    model_save_path = os.path.join(MODEL_SAVE_DIR, f"{args.model}.pkl")
    with open(model_save_path, "wb") as f:
        pickle.dump(model, f)

    file_logger.info(f"Saved {args.model} model to {model_save_path}")
    rich_logger.info(f"Completed training for {args.model}!")
