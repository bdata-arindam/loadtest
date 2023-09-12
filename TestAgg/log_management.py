import os
import glob
import logging
import logging.handlers
from datetime import datetime

class LogManager:
    def __init__(self, config):
        self.config = config

    def configure_logging(self):
        log_file = self.config.get("log_file", "app.log")
        log_dir = os.path.dirname(log_file)

        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_level = self.config.get("log_level", "INFO").upper()
        log_format = self.config.get("log_format", "%(asctime)s - %(levelname)s - %(message)s")

        # Create a rotating log handler to manage log files
        log_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.get("log_max_bytes", 1024 * 1024),  # 1 MB
            backupCount=self.config.get("log_backup_count", 10)
        )

        logging.basicConfig(
            level=log_level,
            format=log_format,
            handlers=[log_handler]
        )

    def manage_logs(self):
        if self.config.get("auto_compress_logs", False):
            self.compress_logs()

        if self.config.get("auto_delete_logs", False):
            self.delete_old_logs()

    def compress_logs(self):
        log_files = glob.glob(os.path.join(os.path.dirname(self.config["log_file"]), "*.log"))
        for log_file in log_files:
            if log_file.endswith(".log"):
                compressed_log_file = log_file + ".gz"
                os.system(f"gzip -c {log_file} > {compressed_log_file}")
                os.remove(log_file)
                logging.info(f"Compressed log file: {log_file} -> {compressed_log_file}")

    def delete_old_logs(self):
        log_dir = os.path.dirname(self.config["log_file"])
        log_files = glob.glob(os.path.join(log_dir, "*.log.*"))
        max_age_days = self.config.get("log_max_age_days", 60)

        current_time = datetime.now()
        for log_file in log_files:
            try:
                file_time = datetime.fromtimestamp(os.path.getctime(log_file))
                age = (current_time - file_time).days
                if age >= max_age_days:
                    os.remove(log_file)
                    logging.info(f"Deleted old log file: {log_file}")
            except Exception as e:
                logging.error(f"Error deleting log file: {str(e)}")