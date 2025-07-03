import os
from pathlib import Path
import logging

# Configure basic logging for this module
# Note: In a larger application, you'd likely use the central logging configuration.
# This is here for standalone usability and demonstration.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def initialize_research_environment():
    """
    Checks for the existence of the core research directory structure and creates it if missing.
    This ensures the environment is ready for use when the main application starts.
    """
    base_path = Path(__file__).parent
    logging.info(f"Initializing AlphaHome research environment at '{base_path}'...")

    required_dirs = [
        "projects",
        "templates",
        "tools",
        "archives",
        "backtest_lab",
        "data_sandbox",
        "docs",
        "notebooks",
        "prototypes",
    ]

    all_ok = True
    for dir_name in required_dirs:
        dir_path = base_path / dir_name
        if not dir_path.is_dir():
            logging.warning(f"Required directory '{dir_path}' not found. Creating it...")
            try:
                dir_path.mkdir(parents=True, exist_ok=True)
            except OSError as e:
                logging.error(f"Failed to create directory '{dir_path}': {e}")
                all_ok = False
    
    if all_ok:
        logging.info("Research environment structure is verified.")
    else:
        logging.error("Failed to initialize one or more research environment directories.") 