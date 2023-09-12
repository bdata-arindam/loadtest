import json
import threading
import time
import psutil
from kafka import KafkaConsumer, KafkaProducer

# Load the initial configuration from src_dstn_config.json
def load_config():
    with open('src_dstn_config.json', 'r') as config_file:
        return json.load(config_file)

# Function to write the last offset to the recovery file
def write_recovery_file(last_offset, recovery_file):
    with open(recovery_file, 'w') as recovery:
        recovery.write(str(last_offset))

# Function to read the last offset from the recovery file
def read_recovery_file(recovery_file):
    try:
        with open(recovery_file, 'r') as recovery:
            return int(recovery.read())
    except FileNotFoundError:
        return None

# Function to calculate minimum required memory and CPU threads
def calculate_minimum_requirements(config):
    memory_required = (config['num_consumer_threads'] + config['num_producer_threads']) * config['memory_per_thread']
    cpu_threads_required = config['num_consumer_threads'] + config['num_producer_threads']
    return memory_required, cpu_threads_required

# Function to dynamically update the configuration from config.json
def dynamic_config_update():
    while True:
        config = load_config()
        num_consumer_threads = config['num_consumer_threads']
        num_producer_threads = config['num_producer_threads']

        if config != dynamic_config_update.current_config:
            print("Configuration changed. Applying updates...")

            # Perform any necessary cleanup or updates here
            # For example, you can stop and restart consumer/producer threads
            
            # Re-calculate and print minimum requirements
            min_memory, min_cpu_threads = calculate_minimum_requirements(config)
            print(f"Minimum Memory Required: {min_memory} MB")
            print(f"Minimum CPU Threads Required: {min_cpu_threads}")

            # Update the current configuration
            dynamic_config_update.current_config = config

        time.sleep(config['config_check_interval'])

# Initialize the current configuration
dynamic_config_update.current_config = load_config()

# Start the dynamic config update thread
config_update_thread = threading.Thread(target=dynamic_config_update)
config_update_thread.daemon = True
config_update_thread.start()

# Kafka consumer function
def consume_and_aggregate(consumer_id):
    config = load_config()
    recovery_file = config['recovery_file']

    consumer = KafkaConsumer(
        config['source_topic'],
        bootstrap_servers=config['source_bootstrap_servers'],
        security_protocol=config['security_protocol'],
        ssl_cafile=config['ssl_cafile'],
        ssl_certfile=config['ssl_certfile'],
        ssl_keyfile=config['ssl_keyfile'],
        sasl_mechanism=config['sasl_mechanism'],  # Kerberos mechanism
        sasl_kerberos_service_name=config['sasl_kerberos_service_name'],  # Kerberos service name
        auto_offset_reset='earliest' if last_offset is None else 'latest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Assuming JSON format
    )

    # Initialize aggregation results
    aggregation_result = {}

    while True:
        try:
            for message in consumer:
                message_value = message.value

                # Apply optional filter if defined in config
                if 'filter_column' in config and 'filter_value' in config:
                    filter_column = config['filter_column']
                    filter_value = config['filter_value']
                    if filter_column in message_value and message_value[filter_column] != filter_value:
                        continue  # Skip message if it doesn't match filter criteria

                # Perform optional aggregation if defined in config
                if 'aggregation_column' in config and 'aggregation_function' in config:
                    aggregation_column = config['aggregation_column']
                    aggregation_function = config['aggregation_function']

                    if aggregation_column not in message_value:
                        continue  # Skip message if aggregation column doesn't exist

                    if aggregation_function == 'SUM':
                        aggregation_result[aggregation_column] = aggregation_result.get(aggregation_column, 0) + message_value[aggregation_column]
                    elif aggregation_function == 'COUNT':
                        aggregation_result[aggregation_column] = aggregation_result.get(aggregation_column, 0) + 1

                # Optionally, select specific columns (if defined in column.json)
                if 'selected_columns' in config:
                    selected_columns = config['selected_columns']
                    message_value = {col: message_value[col] for col in selected_columns if col in message_value}

                # Process the filtered and aggregated message (you can define your own logic here)
                print(message_value)

                # Commit the offset to acknowledge message consumption
                consumer.commit()

                # Periodically write aggregation result to an output file
                if time.time() % config['output_interval'] == 0:
                    with open(config['output_file'], 'w') as out_file:
                        out_file.write(f"Aggregation Result: {aggregation_result}\n")
        except Exception as e:
            print(f"Consumer {consumer_id} Error: {str(e)}")
            write_recovery_file(offset, recovery_file)
            time.sleep(config['error_retry_interval'])  # Wait before retrying

# Load the configuration
config = load_config()
num_consumer_threads = config['num_consumer_threads']

# Start consumer threads
consumer_threads = []
for i in range(num_consumer_threads):
    consumer_thread = threading.Thread(target=consume_and_aggregate, args=(i,))
    consumer_thread.start()
    consumer_threads.append(consumer_thread)

# Main application loop
while True:
    time.sleep(1)
