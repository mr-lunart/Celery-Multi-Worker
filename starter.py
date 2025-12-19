from scrapper import consumer_sqs
import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv() 

SQS_URL = "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue"

aws_client = boto3.client(
    service_name="sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-west-1"
)

message_id = "001"
total_club = 100
for i in range(total_club):
    filename = f"json_file_{i}.json"
    message_body_dict = {
        "filename": filename,
        "id": message_id,
    }
    message_body_json = json.dumps(message_body_dict)
    response = aws_client.send_message(
        QueueUrl=SQS_URL,
        MessageBody=message_body_json,
        MessageGroupId="test" 
    )

total_kick = 10
for i in range(total_kick):
    consumer_sqs.apply_async(
        routing_key="tasks.consumer_sqs",
        queue="consumer_sqs"
    )