import boto3
import asyncio
import redis

import os
import json
import logging

from celery import Celery
from dotenv import load_dotenv

load_dotenv(dotenv_path="config/.env")

aws_client = boto3.client(
    service_name="sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-west-1"
)

app = Celery()
app.conf.broker_url=os.getenv("CELERY_REDIS")
app.conf.result_backend=os.getenv("CELERY_REDIS")

# create entry point for celery worker
@app.task(name='start',bind=True)
def gateway(self, event_body:dict):
    return

def setup_logger():
    logs_path = "logs/consumer"
    logger = logging.getLogger("logger-consumer")
    logger.setLevel(logging.DEBUG)
    log_console_handler = logging.StreamHandler()
    log_file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=os.path.join(logs_path, 'consumer-history.log'),
        when='midnight',
        interval=1,
        backupCount=3, # Akan menyimpan log 7 hari terakhir
        encoding='utf-8',
    )
    logger.addHandler(log_console_handler)
    logger.addHandler(log_file_handler)
    formatter = logging.Formatter(
        "{asctime} - {levelname} - {message}",
        style="{",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    log_console_handler.setFormatter(formatter)
    log_file_handler.setFormatter(formatter)
    return logger

def consumer_sqs(foldername:str):
    response = aws_client.receive_message(
        QueueUrl=os.getenv("SQS_URL"),
        MaxNumberOfMessages=1,
        MessageSystemAttributeNames=['MessageGroupId'],
        WaitTimeSeconds=10 # Long polling
    )
    messages = response.get('Messages', [])
    try:
        if messages:
            message = messages[0]
            event_body = json.loads(message['Body'])
            event_body["receipt_handle"]=message['ReceiptHandle']
            event_body["message_group_id"]=message['Attributes']['MessageGroupId']
            message_pathfile = add_sqs_message(foldername=foldername,data=event_body)
            logger.info(f"Found message, sending task {message_pathfile} to worker...")
            if message_pathfile:
                event_body["message_pathfile"]=message_pathfile
                gateway.apply_async(kwargs={'event_body':event_body})
                return
            else:
                raise Exception("Failed adding message")
        else:
            logger.info(f"Found No Messages, repeat process")

    except Exception as err:
        raise err

def add_sqs_message(foldername:str, data:dict) -> str:
    # adding file to indicate message still on process
    try:
        if not os.path.exists(foldername):
            os.makedirs(foldername)
        file_count = count_active_message(foldername=foldername)
        filename = f"message-{file_count}.json"
        path_file = os.path.join(foldername, filename)
        with open(path_file, "w") as f:
            json.dump(data, f, indent=4)
        return path_file
    except Exception as err:
        return ""

def count_active_message(foldername:str) -> int:
    # flat file limit system
    if not os.path.exists(foldername):
        os.makedirs(foldername)
    file_list = os.listdir(foldername)
    return len(file_list)

def check_redis_connection():
    try:
        r = redis.Redis(
            host=os.getenv("REDIS_HOST"), 
            port=os.getenv("REDIS_PORT"), 
            db=os.getenv("REDIS_DB"), 
            username=os.getenv("REDIS_USERNAME"), 
            password=os.getenv("REDIS_PASSWORD"),
            socket_connect_timeout=5
        )
        if r.ping():
            return True
        else:
            return False
    except redis.ConnectionError as err:
            logger.error("ERROR connection failed to redis")
            return False

async def run():
    max_message=int(os.getenv("MAX_MESSAGE"))
    foldername=os.getenv("ACTIVE_MESSAGE_PATH")
    while True:
        await asyncio.sleep(1)
        # count maximum allowed active message
        if count_active_message(foldername=foldername) < max_message:
             # check redis first
            redis_status = check_redis_connection()
            if redis_status:
                pass
            else:
                continue
            # init consume process
            consumer_sqs(foldername=foldername)
        else:
            continue

if __name__ == "__main__":
    logger = setup_logger()
    asyncio.run(run())