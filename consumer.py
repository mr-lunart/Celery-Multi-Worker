import os
import json
import boto3
import datetime

from celery import Celery
from dotenv import load_dotenv

load_dotenv() 

SQS_URL = "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue"

aws_client = boto3.client(
    service_name="sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-west-1"
)

app = Celery()
app.conf.broker_url = 'redis://127.0.0.1:6379/0'
app.conf.result_backend = 'redis://127.0.0.1:6379/0'

@app.task(bind=True)
def start(event_body, receipt_handle:str, message_group_id:str):
    return

def consumer_sqs():
    now = datetime.datetime.now()
    start_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"Start consume::{start_time}")
    response = aws_client.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=['MessageGroupId'],
        WaitTimeSeconds=10 # Long polling
    )
    messages = response.get('Messages', [])
    try:
        if messages:
            print("Found message, sending task to worker...")
            message = messages[0]
            group_id = message['Attributes']['MessageGroupId']
            event_body = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']  
            start.apply_async(kwargs={
                'event_body':event_body,
                'receipt_handle':receipt_handle,
                'message_group_id':group_id}
            )
            return
        else:
            now = datetime.datetime.now()
            end_time = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"Found No Messages, Finish::{end_time}")
            raise Exception("Point Failure")

    except Exception as err:
        raise err

while True:
    consumer_sqs()