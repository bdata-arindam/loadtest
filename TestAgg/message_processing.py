import json
import logging
from datetime import datetime, timedelta

class MessageProcessor:
    def __init__(self, config, cache_manager):
        self.config = config
        self.cache_manager = cache_manager

    def process_message(self, message):
        try:
            data = json.loads(message.value)
            filter_conditions = self.config.get("filter_conditions", {})
            aggregation_config = self.config.get("aggregation_config", {})

            # Apply filter conditions
            if all(data.get(key) == value for key, value in filter_conditions.items()):
                message_timestamp = datetime.strptime(data["timestamp"], '%Y-%m-%dT%H:%M:%SZ')
                current_time = datetime.utcnow()

                # Check if the message timestamp is within the time window
                if (current_time - message_timestamp) <= timedelta(seconds=self.config["time_window"]["interval_seconds"]):
                    # Count messages that meet filter conditions
                    self.increment_count(aggregation_config, message_timestamp)
        except Exception as e:
            # Log the error
            logging.error(f"Error processing message: {str(e)}")

    def increment_count(self, aggregation_config, timestamp):
        key = timestamp.strftime("%Y-%m-%d %H:%M:%S")
        count = 1

        # Get the cached count or initialize to zero
        cached_count = self.cache_manager.get(key)
        if cached_count:
            count += cached_count

        # Set the new count in cache
        self.cache_manager.set(key, count)
