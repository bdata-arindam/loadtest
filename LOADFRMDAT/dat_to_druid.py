import time
import json
import os
import datetime
import re
from email_alert import send_email_alert
from druid_operations import append_to_druid, read_column_definitions

# ... (Rest of the code remains the same)

if __name__ == "__main__":
    example_config = {
        "email_alert": {
            "subject_prefix": "Data Processing Report - ",
            "receiver_email": "receiver@example.com",
            "sender_email": "sender@example.com",
            "smtp_server": "<>.<>.com",  # Replace with your SMTP server address
            # "sender_password": "your_sender_password",  # Uncomment and set your sender's password if needed
        }
    }
    
    num_files_processed = 5
    num_records_processed = 100
    
    send_email_alert(example_config, num_files_processed, num_records_processed)