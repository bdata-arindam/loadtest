# main.py

import json
import time
import logging
import psutil  # For CPU and memory utilization
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Function to read the configuration from config.json
def read_config(config_file):
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            return config_data
    except FileNotFoundError:
        print(f"Error: Config file '{config_file}' not found.")
        return {}

# Function to execute operations based on the configuration
def execute_operations(config):
    operation_switch = config.get("operation_switch", "single")  # Default to "single" mode
    if operation_switch == "single":
        # Perform a single operation based on the configuration
        if config.get("data_type") == "csv":
            csvdruidload.load_csv_data_to_druid("data.csv", config.get("csv_columns", []), config.get("druid_config", {}), config.get("druid_operation", "insert"), config.get("memory_percentage", 2))
        elif config.get("data_type") == "fixed_length":
            load_fixed_data_to_druid.process_files_serially(config.get("file_pattern", ""), config.get("fixed_length_config", []), config.get("druid_config", {}), config.get("datetime_position", 1), config.get("memory_percentage", 2))
    elif operation_switch == "parallel":
        # Perform operations in parallel based on the configuration
        if config.get("data_type") == "csv":
            # Implement parallel CSV data loading
            pass
        elif config.get("data_type") == "fixed_length":
            # Implement parallel fixed-length data loading
            pass
    else:
        error_message = "Invalid operation_switch value in config.json."
        logger.error(error_message)

# Function to initialize logging
def setup_logging(log_file):
    logging.basicConfig(filename=log_file, level=logging.ERROR, format='%(asctime)s:ERROR:%(name)s:%(levelname)s:%(message)s')

# Function to monitor changes in the config.json file
class ConfigFileHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith("config.json"):
            print("config.json has been modified.")
            config = read_config("config.json")
            execute_operations(config)

if __name__ == "__main__":
    log_file = "main_module.log"  # Specify the main module log file
    setup_logging(log_file)

    # Start monitoring the config.json file for changes
    event_handler = ConfigFileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=".", recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(5)  # Check for changes every 5 seconds
    except KeyboardInterrupt:
        observer.stop()
    observer.join()