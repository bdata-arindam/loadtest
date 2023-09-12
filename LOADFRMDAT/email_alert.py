import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email_alert(config, num_files_processed, num_records_processed):
    try:
        subject = config['email_alert']['subject_prefix'] + "Data Processing Report"
        receiver_email = config['email_alert']['receiver_email']
        smtp_server = smtplib.SMTP(config['email_alert']['smtp_server'])
        smtp_server.starttls()  # Use starttls() for secure connections

        msg = MIMEMultipart()
        msg['From'] = config['email_alert']['sender_email']
        msg['To'] = receiver_email
        msg['Subject'] = subject

        body = f"Number of files processed: {num_files_processed}\nNumber of records processed: {num_records_processed}"

        msg.attach(MIMEText(body, 'plain'))

        # Authenticate if needed (uncomment and set your sender's password)
        # smtp_server.login(config['email_alert']['sender_email'], config['email_alert']['sender_password'])

        text = msg.as_string()
        smtp_server.sendmail(config['email_alert']['sender_email'], receiver_email, text)
        smtp_server.quit()
        
        print("Email alert sent successfully.")
    except Exception as e:
        print(f"Error sending email alert: {e}")

# Example usage
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
