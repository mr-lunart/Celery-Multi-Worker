import os
import json
import boto3
import random
from dotenv import load_dotenv

load_dotenv() 

SQS_URL = "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue"

aws_client = boto3.client(
    service_name="sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-west-1"
)

# send message
def add_message(message_body):
    try:
        response = aws_client.send_message(
                    QueueUrl=SQS_URL,
                    MessageBody=message_body,
                    MessageGroupId="asia"
        )
    except Exception as err:
        print(err)

# delete message
def delete_message():
    response = aws_client.receive_message(
        QueueUrl=SQS_URL,
        MaxNumberOfMessages=10,
        MessageSystemAttributeNames=['MessageGroupId','VisibilityTimeout'],
        WaitTimeSeconds=10 # Long polling
    )
    messages = response.get('Messages', [])
    if messages:
        for message in messages:
            receipt_handle = message['ReceiptHandle']
            group_id = message['Attributes']['MessageGroupId']
            message_body = message['Body']   
            delete_response = aws_client.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle
            )
    else:
        print("found no messages")

# delete_message()
organizations = ["Liverpool FC"]
for organization in organizations:
    message_body = json.dumps({
                "num_of_post" : 5,
                "platform":"facebook",
                "organization":organization,
                "start_date":"",
                "end_date":"",
            })
    add_message(message_body)

    message_body = json.dumps({
                "num_of_post" : 5,
                "platform":"instagram",
                "organization":organization,
                "start_date":"",
                "end_date":"",
            })
    add_message(message_body)

    message_body = json.dumps({
                "num_of_post" : 5,
                "platform":"tiktok",
                "organization":organization,
                "start_date":"",
                "end_date":"",
            })
    add_message(message_body)

    message_body = json.dumps({
                "num_of_post" : 5,
                "platform":"youtube",
                "organization":organization,
                "start_date":"",
                "end_date":"",
            })
    add_message(message_body)

    message_body = json.dumps({
                "num_of_post" : 5,
                "platform":"twitter",
                "organization":organization,
                "start_date":"",
                "end_date":"",
            })
    add_message(message_body)