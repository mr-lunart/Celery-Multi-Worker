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

message_body = json.dumps({
            "param1" : "hello world",
            "param2" : "nice to meet you",
            "param3" : "goodnight",
        })

response = aws_client.send_message(
            QueueUrl=SQS_URL,
            MessageBody=message_body,
            MessageGroupId="test"
        )
        
message_id = response['MessageId']