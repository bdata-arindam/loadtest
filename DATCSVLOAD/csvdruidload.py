# csvdruidload.py

import json
import os
import pandas as pd
from pydruid import *
from pydruid.client import *
from pydruid.query import *
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

# Function to load CSV data into Druid
def load_csv_data_to_druid(csv_file, csv_columns, druid_config, druid_operation, memory_percentage=2):
    try:
        # Memory and CPU utilization configuration
        total_memory = psutil.virtual_memory().total
        max_memory_bytes = total_memory * (memory_percentage / 100)
        max_memory_mb = int(max_memory_bytes / (1024 ** 2))

        # Limit memory usage for pandas
        pd.options.memory_usage_deprecated = False
        pd.options.memory_usage_mb = max_memory_mb

        # Establish a connection to Druid
        druid_client = PyDruid(**druid_config)

        # Read CSV data into a DataFrame with memory optimization
        df = pd.read_csv(csv_file, usecols=csv_columns)

        # Perform Druid operation (insert, replace, etc.)
        if druid_operation == "insert":
            druid_client.insert(df)
        elif druid_operation == "replace":
            druid_client.replace(df)
        elif druid_operation == "overwrite":
            druid_client.overwrite(df)
        elif druid_operation == "append":
            druid_client.append(df)
        elif druid_operation == "new_datasource":
            druid_client.create_new_datasource(df)
        else:
            print("Invalid Druid operation specified.")

        print("Data loaded successfully into Druid.")
    except Exception as e:
        print(f"Error loading data into Druid: {str(e)}")

# Example usage:
if __name__ == "__main__":
    druid_config = read_druid_config("druid_connect.json")

    # Define the CSV columns to load
    csv_columns = ["column1", "column2", "column3"]

    # Example: Load CSV data into Druid with an "insert" operation
    load_csv_data_to_druid("data.csv", csv_columns, druid_config, "insert")
