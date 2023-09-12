import mysql.connector
import logging

class DatabaseManager:
    def __init__(self, config):
        self.config = config

    def connect_to_database(self):
        try:
            db_config = self.config.get("database_config")
            self.conn = mysql.connector.connect(**db_config)
            self.cursor = self.conn.cursor()
            logging.info("Connected to MySQL database")
        except Exception as e:
            logging.error(f"Database connection error: {str(e)}")

    def save_aggregated_data(self, data):
        try:
            query = self.config.get("save_data_query")
            if not query:
                logging.warning("No save data query specified in the configuration.")
                return

            self.connect_to_database()

            for row in data:
                self.cursor.execute(query, row)

            self.conn.commit()
            logging.info("Aggregated data saved to the database")
        except Exception as e:
            logging.error(f"Error saving aggregated data to the database: {str(e)}")
        finally:
            self.close_database_connection()

    def close_database_connection(self):
        if hasattr(self, 'cursor') and self.cursor:
            self.cursor.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logging.info("Closed database connection")
