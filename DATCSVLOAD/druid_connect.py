# druid_connect.py

import json
from pydruid import *
import psutil  # For CPU and memory utilization

# Function to read Druid configuration from druid_connect.json
def read_druid_config(config_file):
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            return config_data
    except FileNotFoundError:
        print(f"Error: Druid config file '{config_file}' not found.")
        return {}

# Function to establish a connection to Druid
def establish_druid_connection(druid_config, memory_percentage=2):
    try:
        # Memory and CPU utilization configuration
        total_memory = psutil.virtual_memory().total
        max_memory_bytes = total_memory * (memory_percentage / 100)
        max_memory_mb = int(max_memory_bytes / (1024 ** 2))

        # Limit memory usage for Druid queries
        DruidPyQuery.MAX_QUERIES_MEMORY = max_memory_mb

        return PyDruid(**druid_config)
    except Exception as e:
        print(f"Error establishing a connection to Druid: {str(e)}")
        return None

# Example usage:
if __name__ == "__main__":
    druid_config = read_druid_config("druid_connect.json")

    # Example: Establish a connection to Druid
    druid_client = establish_druid_connection(druid_config)
    if druid_client:
        print("Connected to Druid successfully.")
