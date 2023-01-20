import pysftp
import pandas as pd
from google.cloud import storage
from datetime import date, timedelta
import os

def get_file(host,username,password,date):
    # Retrieve file from SFTP server
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
        
    with pysftp.Connection(host, username=username, password=password, cnopts=cnopts) as sftp:

        with sftp.cd(date):
            for filename in sftp.listdir():
                print(f'Connected to server and reading {filename}...')
                with sftp.open(filename) as _file:
                    df  = pd.read_csv(_file)
                    #print(df.head(1))
                    df.to_csv(f'{filename}',index=False)
                    print(f'Saved {filename} to local')
                    return filename

def upload_to_gcs(bucket_name,filename):
    storage_client = storage.Client()
    blob_name = filename
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        blob.upload_from_filename(filename)
        print('file: ',filename,' uploaded to bucket: ',bucket_name,' successfully')
    except Exception as e:
        print(e)

if __name__ == '__main__':
    HOST = os.environ.get('SFTP_HOSTNAME','Specified environment variable is not set.')
    USERNAME = os.environ.get('SFTP_USERNAME','Specified environment variable is not set.')
    PASSWORD = os.environ.get('SFTP_PASSWORD','Specified environment variable is not set.')
    BUCKET_NAME = os.environ.get('STORAGE_BUCKET_NAME','Specified environment variable is not set.')


    today = date.today()
    yesterday = today - timedelta(days = 1)
    yesterday = yesterday.strftime('%Y_%m_%d')
    filename = get_file(HOST,USERNAME,PASSWORD,yesterday)
    upload_to_gcs(BUCKET_NAME,filename)
