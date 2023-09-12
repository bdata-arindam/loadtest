import json
import logging
import os
import threading
import time
import psutil
from kafka import KafkaConsumer, KafkaProducer
import avro.schema
from avro.io import DatumReader, DatumWriter

def load_config(config_file_path):
    with open(config_file_path, 'r') as config_file:
        return json.load(config_file)

def write_recovery_file(last_offset, recovery_file):
    with open(recovery_file, 'w') as recovery:
        recovery.write(str(last_offset))

def read_recovery_file(recovery_file):
    try:
        with open(recovery_file, 'r') as recovery:
            return int(recovery.read())
    except FileNotFoundError:
        return None

def configure_logging(log_file_location, log_file_name):
    log_file_path = os.path.join(log_file_location, log_file_name)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file_path),
            logging.StreamHandler()
        ]
    )

def dynamic_config_update(config_file_path):
    while True:
        config = load_config(config_file_path)
        num_consumer_threads = config['num_consumer_threads']
        num_producer_threads = config['num_producer_threads']

        # Calculate memory and CPU limits dynamically
        available_memory = psutil.virtual_memory().available / (1024 * 1024)  # in MB
        available_cpu = psutil.cpu_percent()

        memory_limit = config['memory_limit_percent'] * available_memory / 100
        cpu_limit = config['cpu_limit_percent'] * psutil.cpu_count()

        # Adjust thread counts based on limits
        num_consumer_threads = min(num_consumer_threads, int(cpu_limit / 2))
        num_producer_threads = min(num_producer_threads, int(cpu_limit / 2))

        if config != dynamic_config_update.current_config:
            logging.info("Configuration changed. Applying updates...")

            # Perform any necessary cleanup or updates here
            # For example, you can stop and restart consumer/producer threads

            # Re-calculate and log minimum requirements
            min_memory, min_cpu_threads = calculate_minimum_requirements(config)
            logging.info(f"Minimum Memory Required: {min_memory} MB")
            logging.info(f"Minimum CPU Threads Required: {min_cpu_threads}")

            # Update the current configuration
            dynamic_config_update.current_config = config

        time.sleep(config['config_check_interval'])

def consume_and_replicate(consumer_id, config, avro_schema, consume_and_replicate_config):
    recovery_file = config['source_recovery_file']

    param1 = consume_and_replicate_config.get("param1", "default_value1")
    param2 = consume_and_replicate_config.get("param2", "default_value2")
    param3 = consume_and_replicate_config.get("param3", 0)

    # Create Kafka consumer
    consumer = KafkaConsumer(
        config['source_topic'],
        bootstrap_servers=config['source_bootstrap_servers'],
        security_protocol=config['source_security_protocol'],
        ssl_cafile=config['source_ssl_cafile'],
        ssl_certfile=config['source_ssl_certfile'],
        ssl_keyfile=config['source_ssl_keyfile'],
        sasl_mechanism=config['source_sasl_mechanism'],
        sasl_kerberos_service_name=config['source_sasl_kerberos_service_name'],
        auto_offset_reset='earliest' if last_offset is None else 'latest',
        enable_auto_commit=False,
        value_deserializer=lambda x: deserialize_data(x, config['source_data_format'], avro_schema)
    )

    producer = KafkaProducer(
        bootstrap_servers=config['destination_bootstrap_servers'],
        security_protocol=config['destination_security_protocol'],
        ssl_cafile=config['destination_ssl_cafile'],
        ssl_certfile=config['destination_ssl_certfile'],
        ssl_keyfile=config['destination_ssl_keyfile'],
        sasl_mechanism=config['destination_sasl_mechanism'],
        sasl_kerberos_service_name=config['destination_sasl_kerberos_service_name'],
        value_serializer=lambda x: serialize_data(x, config['destination_data_format'], avro_schema)
    )

    while True:
        try:
            for message in consumer:
                message_value = message.value

                # Optionally, perform data format conversion
                if config['source_data_format'] != config['destination_data_format']:
                    message_value = convert_data_format(message_value, config['source_data_format'], config['destination_data_format'])

                # Replicate the message to the destination topic
                producer.send(config['destination_topic'], value=message_value)

                # Commit the offset to acknowledge message consumption
                consumer.commit()
        except Exception as e:
            logging.error(f"Consumer {consumer_id} Error: {str(e)}")
            write_recovery_file(offset, recovery_file)
            time.sleep(config['source_error_retry_interval'])

def main(config_file_path):
    # Load the configuration
    config = load_config(config_file_path)

    # Configure logging
    configure_logging(config['log_file_location'], config['log_file_name'])

    # Start the dynamic config update thread
    config_update_thread = threading.Thread(target=dynamic_config_update, args=(config_file_path,))
    config_update_thread.daemon = True
    config_update_thread.start()

    # Load Avro schema (if applicable)
    if config['source_data_format'] == 'Avro':
        avro_schema = avro.schema.Parse(open('source_avro_schema.avsc').read())
    else:
        avro_schema = None

    # Start consumer threads
    consumer_threads = []
    for i in range(config['num_consumer_threads']):
        consumer_thread = threading.Thread(target=consume_and_replicate, args=(i, config, avro_schema, config['consume_and_replicate_config']))
        consumer_thread.start()
        consumer_threads.append(consumer_thread)

    # Main application loop
    while True:
        time.sleep(1)

if __name__ == "__main__":
    config_file_path = 'config/src_dstn_config.json'
    main(config_file_path)