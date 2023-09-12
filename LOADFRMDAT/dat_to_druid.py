import time
import json
import os
import datetime
import re
from email_alert import send_email_alert
from druid_operations import append_to_druid, read_column_definitions

# Function to read configuration from config.json
def read_config():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

# Function to list .dat files in a directory with the specified pattern from config.json
def list_dat_files(directory, pattern):
    return [f for f in os.listdir(directory) if re.match(pattern, f)]

def main():
    config = read_config()
    columns = read_column_definitions()
    dat_directory = config['dat_directory']
    polling_interval = config['polling_interval']
    dat_file_pattern = config['dat_file_pattern']

    while True:
        dat_files = list_dat_files(dat_directory, dat_file_pattern)
        if dat_files:
            record_count = 0

            for file in dat_files:
                file_path = os.path.join(dat_directory, file)
                append_to_druid(file_path, config, columns)
                record_count += 1

            if config['email_alert']['enabled'] and record_count > 0:
                current_hour = datetime.datetime.now().hour
                alert_interval_hours = config['email_alert']['alert_interval_hours']
                if alert_interval_hours > 0 and current_hour % alert_interval_hours == 0:
                    send_email_alert(config, len(dat_files), record_count)

        time.sleep(polling_interval)

if __name__ == "__main__":
    main()
