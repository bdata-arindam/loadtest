import json
import logging
import argparse
from kafka_consumer import consume_kafka_messages
from kafka_producer import produce_aggregated_data
from message_processing import process_message
from cache import CacheManager
from filesystem_cache import FilesystemCache
from database import DatabaseManager
from log_management import LogManager

# Function to load configuration from a JSON file
def load_config(config_path):
    try:
        with open(config_path, 'r') as config_file:
            return json.load(config_file)
    except Exception as e:
        raise ValueError(f"Error loading configuration from {config_path}: {str(e)}")

# Command-line argument parser
parser = argparse.ArgumentParser(description="Kafka Message Processing Application")
parser.add_argument("--config", type=str, required=True, help="Path to common_config.json")
args = parser.parse_args()

# Load common configuration
common_config = load_config(args.config)

# Configure logging
log_file = common_config.get("log_file", "app.log")
logging.basicConfig(filename=log_file, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialize Kafka consumer
consumer = consume_kafka_messages(common_config)

# Initialize Kafka producer (if needed)
producer = None
if common_config.get("produce_to_topic", False):
    producer = produce_aggregated_data(common_config)

# Initialize cache manager
cache_manager = CacheManager()
if common_config.get("use_inmemory_cache", False):
    cache_manager.add_cache("inmemory", CacheManager.CACHE_INMEMORY)
if common_config.get("use_filesystem_cache", False):
    cache_manager.add_cache("filesystem", CacheManager.CACHE_FILESYSTEM)

# Initialize filesystem cache (optional)
filesystem_cache = None
if common_config.get("use_filesystem_cache", False):
    filesystem_cache = FilesystemCache(common_config.get("filesystem_cache_path", "cache"))

# Initialize database manager (optional)
database_manager = None
if common_config.get("use_mysql_db", False):
    mysql_config = load_config(common_config.get("mysql_config_path", "mysql_config.json"))
    database_manager = DatabaseManager(mysql_config)

# Initialize log manager (optional)
log_manager = None
if common_config.get("use_log_management", False):
    log_manager = LogManager(common_config)

# Process Kafka messages and perform aggregation
for message in consumer:
    process_message(message, common_config, cache_manager, filesystem_cache, database_manager)

# Close resources and perform any necessary cleanup
if producer:
    producer.close()
if filesystem_cache:
    filesystem_cache.close()
if database_manager:
    database_manager.close()
if log_manager:
    log_manager.close()
