import os
import json
import boto3
import time
import datetime

from celery import Celery
from dotenv import load_dotenv

load_dotenv() 

# SQS_URL = "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue"

# aws_client = boto3.client(
#     service_name="sqs",
#     aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
#     aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
#     region_name="eu-west-1"
# )

app = Celery("scrapper")
app.conf.broker_url = 'redis://localhost:6379/0'
app.conf.result_backend = 'redis://localhost:6379/0'

stop_task = False

@app.task()
def producer(event:dict, group_id:str):
    now = datetime.datetime.now()
    start = now.strftime("%Y-%m-%d %H:%M:%S")
    print("start: " + start)
    time.sleep(5)
    now = datetime.datetime.now()
    finish = now.strftime("%Y-%m-%d %H:%M:%S")
    print("finish: " + finish)
    # result = consumer_start.apply_async((event, group_id))
    return

@app.task()
def consumer_start(event:dict, group_id:str):
    time.sleep(5)
    start_producer.apply_async((event, group_id))
    return

@app.task()
def start_producer(event:dict, group_id:str):
    if stop_task:
        pass
    else:
        producer.apply_async((event, group_id))
    return

@app.task()
def stop_queue():
    global stop_task
    stop_app = True
    return