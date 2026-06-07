import boto3
from configparser import ConfigParser
def delete_s3_objects_from_file(file_path, bucket_name):
  """
  Deletes S3 objects based on the complete line (including folder path) in each line of a file.

  Args:
      file_path (str): Path to the file containing object keys.
      bucket_name (str): Name of the S3 bucket containing the objects.
  """
  
  config = ConfigParser()
  config.read('config.ini')

  aws_access_key_id = config.get('aws_credentials', 'aws_access_key_id')
  aws_secret_access_key = config.get('aws_credentials', 'aws_secret_access_key')
  s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
  with open(file_path, 'r') as f:
    for line in f:
      # Remove leading/trailing spaces and use the entire line as object key
      parts = line.split("'")
      last_path = parts[-2].strip()
      object_key = last_path.replace(" ", "").replace(".", "") 
      #object_key = line.strip().split(' ')[-1]
      # Delete the object
      s3.delete_object(Bucket=bucket_name, Key=object_key)
      print(f"Deleted object: {object_key}")

# Replace these with your actual values
file_path = "delete_list.txt"
bucket_name = "pickbackup"

delete_s3_objects_from_file(file_path, bucket_name)
