from flask import Flask, request
import pandas as pd
import paramiko
import os

app = Flask(__name__)

SFTP_HOST = 'sftp.couchdrop.io'
SFTP_USERNAME = 'your_username'
SFTP_PASSWORD = 'your_password'

@app.route('/webhook', methods=['POST'])
def handle_webhook():
    data = request.json
    file_path = data.get('path')

    if not file_path or not file_path.endswith('.csv'):
        return 'Not a CSV file. Skipped.', 200

    local_path = '/tmp/original.csv'
    cleaned_path = '/tmp/cleaned.csv'

    transport = paramiko.Transport((SFTP_HOST, 22))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)

    sftp.get(file_path, local_path)

    df = pd.read_csv(local_path)
    df.columns = [col.strip().replace(" ", "_").upper() for col in df.columns]
    df.to_csv(cleaned_path, index=False)

    cleaned_remote_path = file_path.replace('Five9', 'Five9/Processed')
    try:
        sftp.mkdir('/Outbound/Five9/Processed')
    except:
        pass

    sftp.put(cleaned_path, cleaned_remote_path)

    sftp.close()
    transport.close()

    return 'Cleaned and re-uploaded successfully!', 200
