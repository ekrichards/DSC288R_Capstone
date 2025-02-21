import os
import logging
from datetime import datetime
from rich.progress import Progress
from rich.logging import RichHandler

# ─── Helper Function: Set Up Loggers ─────────────────────────
def setup_loggers(log_filename):
    """Setup separate loggers for console (Rich) and file logging, with timestamped logs."""
    
    # ─ Ensure "logs/" folder exists ─
    log_dir = os.path.join(os.getcwd(), "logs")  # Always in repo root
    os.makedirs(log_dir, exist_ok=True)

    # ─ Append timestamp to log filename ─
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    full_log_path = os.path.join(log_dir, f"{log_filename}_{timestamp}.log")

    # ─ Console Logger (Pretty Terminal Logging) ─
    rich_logger = logging.getLogger("console_logger")
    rich_logger.setLevel(logging.INFO)

    rich_handler = RichHandler(rich_tracebacks=True)
    rich_logger.addHandler(rich_handler)

    # ─ File Logger (Save Logs to File) ─
    file_logger = logging.getLogger("file_logger")
    file_logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(full_log_path, mode="w")
    file_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler.setFormatter(file_formatter)

    file_logger.addHandler(file_handler)

    return rich_logger, file_logger  # Return both loggers