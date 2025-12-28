import logging
import os
from datetime import datetime

def generate_log_object(PLATFORM:str, Organization:str):
    """
    Generates a log file.
    """
    
    logger = logging.getLogger(f"Benchmarking")
    logger.setLevel(logging.INFO)
    
    if not logger.handlers:
        # set path relative to main folder / main app
        logs_path = "logs/scrapers"

        # Create console_handler and set level to debug
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        # Add formatter to console_handler
        formatter_string = f"%(asctime)s - %(name)s - %(levelname)s - {PLATFORM}_{Organization}_scraper: %(message)s"
        formatter = logging.Formatter(formatter_string)
        console_handler.setFormatter(formatter)

        # Add console_handler to logger
        logger.addHandler(console_handler)

        # Create file_handler and set level to debug, set path relative to main folder / main app
        file_handler = logging.FileHandler(filename=os.path.join(logs_path, f'log_file_{datetime.now().strftime("%Y-%m-%d")}.csv'))
        file_handler.setLevel(logging.INFO)

        # Add formatter to file_handler
        formatter_string = f"%(asctime)s\t%(name)s\t%(levelname)s\t{PLATFORM}_{Organization}_scraper\t%(message)s"
        formatter = logging.Formatter(formatter_string)
        file_handler.setFormatter(formatter)

        # Add file_handler to logger
        logger.addHandler(file_handler)
    
    return logger