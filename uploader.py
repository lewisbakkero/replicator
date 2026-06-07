import os
import io
import json
import boto3
import logging
from google.oauth2 import service_account
from configparser import ConfigParser
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

logging.basicConfig(filename='logfile.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

# Set up Google Drive API
google_drive_credentials_file = './credentials.json'
google_drive_scopes = ['https://www.googleapis.com/auth/drive.readonly']
google_drive_service_account = service_account.Credentials.from_service_account_file(
    google_drive_credentials_file, scopes=google_drive_scopes
)
drive_service = build('drive', 'v3', credentials=google_drive_service_account)

config = ConfigParser()
config.read('config.ini')

aws_access_key_id = config.get('aws_credentials', 'aws_access_key_id')
aws_secret_access_key = config.get('aws_credentials', 'aws_secret_access_key')


# Set up Amazon S3 client
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

def list_files_in_folder(folder_name, parent_id=None, current_path=None, file_dict=None):
    # if current_path is None:
    #     current_path = folder_name
    # else:
    #     current_path = os.path.join(current_path, folder_name)

    if file_dict is None:
        file_dict = {}

    folder_query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    response = drive_service.files().list(q=folder_query).execute()

    folder_id = response.get('files', [])[0].get('id') if response.get('files', []) else None

    current_path = folder_name
    if folder_id:
        all_files = []
        page_token = None

        while True:
            response = drive_service.files().list(
                q=f"'{folder_id}' in parents",
                pageSize=1000,
                pageToken=page_token
            ).execute()

            files = response.get('files', [])
            all_files.extend(files)

            page_token = response.get('nextPageToken')
            if not page_token:
                break

        for file in all_files:
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                # Recursively list files and folders in subdirectories
                list_files_in_folder(file['name'], parent_id=file['id'], current_path=current_path, file_dict=file_dict)
            else:
                file_dict[os.path.join(current_path, file['name'])] = file

    return file_dict

def file_exists_in_s3(s3_bucket, s3_key):
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        return True
    except Exception as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            logging.error(f"An error occurred in file_exists_in_s3: {e}")
            raise

def download_and_upload_to_s3(file, s3_bucket, local_temp_dir):
    retry_count = 3
    while retry_count > 0:
        try:
            file_id = file[1]['id']
            file_name = file[1]['name']
            s3_key = file[0]

            if not file_exists_in_s3(s3_bucket, s3_key):
                if file[1]['mimeType'] == 'application/vnd.google-apps.folder':
                    # If it's a folder, create the corresponding folder in S3
                    s3_key = s3_key + '/'  # Ensure it ends with a '/'
                    s3.put_object(Bucket=s3_bucket, Key=s3_key)
                else:
                    # It's a file, download and upload
                    request = drive_service.files().get_media(fileId=file_id)
                    downloader = io.FileIO(os.path.join(local_temp_dir, file_name), 'wb')
                    downloader.write(request.execute())
                    downloader.close()

                    # Upload the file to S3
                    if ('/' in s3_key):
                        with open(os.path.join(local_temp_dir, file_name), 'rb') as file_data:
                            s3.upload_fileobj(file_data, s3_bucket, s3_key)
                            print(f"File '{file_name}' uploaded to S3 in folder '{s3_key}'.")

                    # Clean up local file
                    os.remove(os.path.join(local_temp_dir, file_name))

            # If upload is successful, break out of the loop
            break
            
        except (NoCredentialsError, PartialCredentialsError) as e:
            logging.error(f"Failed to upload '{file_name}' to S3. Retry {retry_count}...")
            retry_count -= 1
            if retry_count == 0:
                logging.error(f"Maximum retries reached. Moving on to the next file.")
        except Exception as e:
            logging.error(f"An error occurred while uploading '{file_name}' to S3: {e}. Retry {retry_count}...")
            retry_count -= 1
            if retry_count == 0:
                logging.error(f"Maximum retries reached. Moving on to the next file.")
        
    

def main():
    google_drive_folder_name = 'Marta'
    s3_bucket = 'pickbackup'

    # List all files in the Google Drive folder
    files_list = list_files_in_folder(google_drive_folder_name)

    # Create a temporary directory to mirror folder hierarchy
    local_temp_dir = 'local_temp_dir'
    os.makedirs(local_temp_dir, exist_ok=True)

    # Download and upload each file to S3
    for file in files_list.items():
        download_and_upload_to_s3(file, s3_bucket,local_temp_dir)

    # Remove the temporary directory
    os.rmdir(local_temp_dir)

if __name__ == "__main__":
    main()
