# load_fixed_data_to_druid.py

import json
import os
import pandas as pd
from pydruid import *
from pydruid.client import *
from pydruid.query import *
import psutil  # For CPU and memory utilization

# Function to read configuration from config.json
def read_config(config_file):
    try:
        with open(config_file, 'r') as f:
            config_data = json.load(f)
            return config_data
    except FileNotFoundError:
        print(f"Error: Config file '{config_file}' not found.")
        return {}

# Function to extract data from fixed-length rows
def extract_fixed_data(input_file, config, datetime_position, memory_percentage=2):
    data = []
    try:
        # Memory and CPU utilization configuration
        total_memory = psutil.virtual_memory().total
        max_memory_bytes = total_memory * (memory_percentage / 100)
        max_memory_mb = int(max_memory_bytes / (1024 ** 2))

        # Limit memory usage for pandas
        pd.options.memory_usage_deprecated = False
        pd.options.memory_usage_mb = max_memory_mb

        with open(input_file, 'r') as f:
            for line in f:
                row = {}
                datetime_value = None
                for col_config in config:
                    col_name = col_config["column_name"]
                    if col_name == "datetime":
                        datetime_value = line[datetime_position:]
                    else:
                        start_pos = col_config["start_position"]
                        length = col_config["length"]
                        value = line[start_pos - 1:start_pos - 1 + length].strip()
                        row[col_name] = value
                if datetime_value:
                    row["datetime"] = datetime_value
                data.append(row)
        return pd.DataFrame(data)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        return None

# Function to load fixed-length data into Druid using specified operation
def load_fixed_data_to_druid(input_file, config_file, druid_config, operation, datasource_name, datetime_position=1, memory_percentage=2):
    try:
        # Establish a connection to Druid
        druid_client = PyDruid(**druid_config)

        # Extract data from fixed-length file
        data = extract_fixed_data(input_file, config_file, datetime_position, memory_percentage)

        if data is not None:
            # Perform Druid operation (insert, replace, etc.)
            if operation == "insert":
                druid_client.insert(data)
            elif operation == "replace":
                druid_client.replace(data)
            elif operation == "overwrite":
                druid_client.overwrite(data)
            elif operation == "append":
                druid_client.append(data)
            elif operation == "new_datasource":
                druid_client.create_new_datasource(data)
            else:
                print("Invalid Druid operation specified.")

            print("Data loaded successfully into Druid.")
    except Exception as e:
        print(f"Error loading data into Druid: {str(e)}")

# Example usage:
if __name__ == "__main__":
    druid_config = read_druid_config("druid_connect.json")

    # Example: Perform an "insert" operation into a Druid datasource named "my_datasource"
    load_fixed_data_to_druid("fixed_data.txt", "fixed_load.json", druid_config, "insert", "my_datasource")
