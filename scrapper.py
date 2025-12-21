import os
import json
import boto3
import time
import datetime

from celery import Celery
from dotenv import load_dotenv

from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

load_dotenv(dotenv_path="config/.env") 

SQS_URL = "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue"

aws_client = boto3.client(
    service_name="sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-west-1"
)

app = Celery("scrapper")
app.conf.broker_url = os.getenv("CELERY_REDIS")
app.conf.result_backend = os.getenv("CELERY_REDIS")
app.conf.task_routes = {}

@app.task(name='start', bind=True)
def start(self, event_body:dict):
    filename=event_body["filename"]
    receipt_handle=event_body["receipt_handle"]
    message_group_id=event_body["message_group_id"]
    message_pathfile=event_body["message_pathfile"]
    process_data.apply_async(kwargs={'filename':event_body["filename"]})

@app.task(bind=True)
def process_data(self, filename:str):
    now = datetime.datetime.now()
    start = now.strftime("%Y-%m-%d %H:%M:%S")
    data_list = []
    quantity = 0
    for i in range(100):
        time.sleep(0.03)
        quantity += i
        item_data = {
            "item_name": f"item_{i:03d}",
            "quantity": quantity,
            "date": "2025-12-12"
        }
        data_list.append(item_data)
    try:
        with open(filename, 'w') as file:
            json.dump(data_list, file, indent=2) 
        now = datetime.datetime.now()
        finish = now.strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"✅ Finish. write '{filename}' : start: {start} | finish: {finish}")
        return
    except:
        now = datetime.datetime.now()
        finish = now.strftime("%Y-%m-%d %H:%M:%S")
        logger.error(f"failed writing file: {filename} start: {start} | finish: {finish}")
        return