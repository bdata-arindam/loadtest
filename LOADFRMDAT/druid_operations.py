import requests
import base64
import datetime

def append_to_druid(file_path, config, columns):
    # Prepare the URL and authentication headers
    url = f"{config['druid_api_url']}/v1/post/{config['datasource']}"
    username = config['druid_username']
    password = config['druid_password']
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        'Authorization': f"Basic {encoded_credentials}"
    }

    # Get the current datetime
    current_datetime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Open the .dat file, read lines, and process each line
    with open(file_path, 'rb') as dat_file:
        for line in dat_file:
            # Create an empty list to store column values
            values = []

            # Iterate through the column definitions
            for column_def in columns:
                col_name = column_def['name']
                start_pos = column_def['start']
                end_pos = start_pos + column_def['length']

                # Extract the value for the column based on positions
                value = line[start_pos:end_pos].decode().strip()
                values.append(value)

            # Prepend datetime as the first column value
            values.insert(0, current_datetime)

            # Prepare the data to be sent to Druid
            data = ','.join(values).encode()

            # Send the data to Druid
            response = requests.post(url, headers=headers, data=data)

            if response.status_code == 200:
                print(f"Row appended to Druid: {values}")
            else:
                print(f"Failed to append row to Druid. Status code: {response.status_code}")
