import subprocess
import sys
import os

# Load utilities
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
sys.path.append(PROJECT_ROOT)
from utils.logger_helper import setup_loggers

# Setup loggers
LOG_FILENAME = "train_models_tuning"
rich_logger, file_logger = setup_loggers(LOG_FILENAME)

def run_tuning_as_subprocess(model_name):
    """Runs hyperparameter tuning as a subprocess and logs the output in real-time."""
    command = [sys.executable, "src/ml_processing/train_model_child.py", "--model", model_name]

    rich_logger.info(f"Starting subprocess for {model_name} tuning...")
    file_logger.info(f"Starting subprocess for {model_name} tuning...")

    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1  # Line-buffered
    )

    # Read output in real-time
    for line in process.stdout:
        rich_logger.info(line.strip())
        file_logger.info(line.strip())

    for line in process.stderr:
        rich_logger.error(line.strip())
        file_logger.error(line.strip())

    process.wait()  # Ensure subprocess completes

    if process.returncode != 0:
        rich_logger.error(f"Subprocess for {model_name} failed with exit code {process.returncode}")
        file_logger.error(f"Subprocess for {model_name} failed with exit code {process.returncode}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True, help="Model to tune")
    args = parser.parse_args()

    run_tuning_as_subprocess(args.model)
