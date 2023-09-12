import json
from kafka import KafkaConsumer

def consume_kafka_messages(config):
    consumer = KafkaConsumer(
        config["kafka_topic"],
        bootstrap_servers=config["kafka_config"]["bootstrap_servers"],
        security_protocol="SASL_PLAINTEXT",  # or "SASL_SSL" if SSL is also enabled
        sasl_mechanism="GSSAPI",
        sasl_kerberos_service_name=config["kafka_config"]["sasl_kerberos_service_name"],
        sasl_kerberos_domain_name=config["kafka_config"]["sasl_kerberos_domain_name"],
        sasl_plain_username=config["kafka_config"]["kerberos_principal"],
        sasl_plain_password=config["kafka_config"]["kerberos_keytab"],
        auto_offset_reset=config["kafka_config"]["auto_offset_reset"],
        group_id=config["kafka_config"]["group_id"]
    )
    
    return consumer