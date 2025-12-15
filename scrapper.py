import os
import json
import boto3
import time
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

app = Celery("scrapper")
app.conf.broker_url = 'redis://localhost:6379/0'
app.conf.result_backend = 'redis://localhost:6379/0'
app.conf.task_routes = {
    'tasks.consumer_sqs': {'queue': 'consumer_sqs'},
    'tasks.delete_sqs': {'queue': 'consumer_sqs'}
}

@app.task()
def consumer_sqs():
    now = datetime.datetime.now()
    start = now.strftime("%Y-%m-%d %H:%M:%S")

    response = aws_client.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=['MessageGroupId'],
        WaitTimeSeconds=10 # Long polling
    )
    messages = response.get('Messages', [])
    try:
        if messages:
            message = messages[0]
            receipt_handle = message['ReceiptHandle']
            group_id = message['Attributes']['MessageGroupId']
            event_body = json.loads(message['Body'])
            now = datetime.datetime.now()
            finish = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"process data::start: {start} | finish: {finish}")
            receipt_handle = message['ReceiptHandle']  
            process_data.apply_async(kwargs={'filename':event_body["filename"],'receipt_handle':receipt_handle})
            return
        else:
            now = datetime.datetime.now()
            finish = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"found no messages:: start: {start} | finish: {finish}")
            return
    except:
        consumer_sqs.apply_async(
            routing_key="tasks.consumer_sqs",
            queue="consumer_sqs"
        )
        return

@app.task()
def process_data(filename:str, receipt_handle:str):
    now = datetime.datetime.now()
    start = now.strftime("%Y-%m-%d %H:%M:%S")
    data_list = []
    for i in range(100):
        time.sleep(0.01)
        item_data = {
            "item_name": f"item_{i:03d}",
            "quantity": 10,
            "date": "2025-12-12"
        }
        data_list.append(item_data)
    try:
        with open(filename, 'w') as file:
            json.dump(data_list, file, indent=2) 
        now = datetime.datetime.now()
        finish = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"start: {start} | finish: {finish}")
        print(f"✅ Finish. write '{filename}' : start: {start} | finish: {finish}")
        delete_sqs.apply_async(
            routing_key="tasks.delete_sqs",
            queue="consumer_sqs",
            kwargs={'receipt_handle':receipt_handle}
        )
        return
    except:
        now = datetime.datetime.now()
        finish = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"failed writing file: {filename} start: {start} | finish: {finish}")
        delete_sqs.apply_async(
            routing_key="tasks.delete_sqs",
            queue="consumer_sqs",
            kwargs={'receipt_handle':receipt_handle}
        )
        return

@app.task()
def delete_sqs(receipt_handle:str):
    try:
        aws_client.delete_message(
            QueueUrl=SQS_URL,
            ReceiptHandle=receipt_handle
        )
        consumer_sqs.apply_async(
            routing_key="tasks.consumer_sqs",
            queue="consumer_sqs"
        )
    except:
        aws_client.delete_message(
            QueueUrl=SQS_URL,
            ReceiptHandle=receipt_handle
        )
        consumer_sqs.apply_async(
            routing_key="tasks.consumer_sqs",
            queue="consumer_sqs"
        )