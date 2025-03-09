import os
import json

# Set the path to your kaggle.json file
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
KAGGLE_JSON_PATH = os.path.join(PROJECT_ROOT, "config", "kaggle.json")

# Ensure kaggle.json exists before continuing
if not os.path.exists(KAGGLE_JSON_PATH):
    raise FileNotFoundError(f"❌ kaggle.json not found at {KAGGLE_JSON_PATH}. Please place it in the config/ folder.")

# Remove any existing Kaggle credentials
os.environ.pop("KAGGLE_USERNAME", None)
os.environ.pop("KAGGLE_KEY", None)

# Load Kaggle credentials manually
with open(KAGGLE_JSON_PATH, "r") as f:
    kaggle_creds = json.load(f)
    os.environ["KAGGLE_USERNAME"] = kaggle_creds["username"]
    os.environ["KAGGLE_KEY"] = kaggle_creds["key"]

from kaggle.api.kaggle_api_extended import KaggleApi
# Authenticate manually
api = KaggleApi()
api.authenticate()

# Verify authentication
print("✅ Kaggle API authentication successful!")

# List available datasets as a test
datasets = api.dataset_list()
print("Available Datasets:", datasets)
