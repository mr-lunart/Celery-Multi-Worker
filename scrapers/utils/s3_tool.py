import json
import os
import boto3
from dotenv import load_dotenv

load_dotenv(dotenv_path="config/.env", override=True) 

s3_resource = boto3.resource(
    service_name='s3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
)

def download_json_file(bucket, file_key):
    """
    Use boto3.resource('s3') for this function

    boto3.client("s3") will allow you to perform Low level API calls
    boto3.resource('s3') will allow you to perform High level API calls
    """
    content_object = s3_resource.Object(bucket, file_key)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content
