import json
from kafka import KafkaProducer
import mysql.connector

def produce_aggregated_data(config, aggregation_data):
    if config.get("produce_to_topic"):
        produce_to_kafka(config, aggregation_data)
    
    if config.get("save_to_mysql"):
        save_to_mysql(config, aggregation_data)

def produce_to_kafka(config, aggregation_data):
    producer = KafkaProducer(
        bootstrap_servers=config["kafka_config"]["bootstrap_servers"],
        security_protocol="SASL_PLAINTEXT",  # or "SASL_SSL" if SSL is enabled
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name=config["kafka_config"]["sasl_kerberos_service_name"],
        sasl_kerberos_domain_name=config["kafka_config"]["sasl_kerberos_domain_name"],
    )

    try:
        # Convert the aggregation_data to a JSON string
        message_value = json.dumps(aggregation_data).encode('utf-8')
        
        # Produce the message to the Kafka topic
        producer.send(config["kafka_topic"], value=message_value)
        producer.flush()
    except Exception as e:
        # Handle and log any exceptions that occur during message production
        print(f"Error producing message to Kafka: {str(e)}")
    finally:
        producer.close()

def save_to_mysql(config, aggregation_data):
    try:
        conn = mysql.connector.connect(
            host=config["mysql_config"]["host"],
            user=config["mysql_config"]["user"],
            password=config["mysql_config"]["password"],
            database=config["mysql_config"]["database"]
        )

        cursor = conn.cursor()

        # Extract data from the aggregation_data dictionary
        start_datetime = aggregation_data.get("startdatetime", None)
        end_datetime = aggregation_data.get("enddatetime", None)
        total_count = aggregation_data.get("totalcount", None)

        # Insert data into the MySQL database
        if start_datetime and end_datetime and total_count:
            cursor.execute(
                "INSERT INTO aggregation_data (start_datetime, end_datetime, total_count) VALUES (%s, %s, %s)",
                (start_datetime, end_datetime, total_count)
            )
            conn.commit()
            print("Data saved to MySQL successfully.")
        else:
            print("Incomplete aggregation data. Data not saved to MySQL.")
    except Exception as e:
        # Handle and log any exceptions that occur during MySQL data insertion
        print(f"Error saving data to MySQL: {str(e)}")
    finally:
        cursor.close()
        conn.close()