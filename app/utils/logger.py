import logging
import time
import psutil
import functools
import logging
import os
from datetime import datetime


# Ensure 'log' directory exists
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Create the logger
logger = logging.getLogger("project_logger")
logger.setLevel(logging.INFO)

# Create a file handler (logs to file)
log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
file_handler = logging.FileHandler(log_filename)
file_handler.setLevel(logging.INFO)

# Create a console handler (logs to console)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
console_handler.setFormatter(formatter)

# Add handlers to the logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Ensure 'log' directory exists
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# Configure logging
log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
logging.basicConfig(filename=log_filename, 
                    level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

def log_performance(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Track start time and memory usage
        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / (1024 ** 2)  # Memory in MB

        # Execute the function
        result = func(*args, **kwargs)

        # Track end time and memory usage
        end_time = time.time()
        end_memory = process.memory_info().rss / (1024 ** 2)  # Memory in MB

        # Calculate time taken and memory used
        execution_time = end_time - start_time
        memory_used = end_memory - start_memory

        # Log the performance
        logging.info(f"Function '{func.__name__}' executed in {execution_time:.4f} seconds")
        logging.info(f"Memory used: {memory_used:.4f} MB")

        return result

    return wrapper