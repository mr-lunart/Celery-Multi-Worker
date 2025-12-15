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

# send message
# message_body = json.dumps({
#             "filename" : "json_file.json",
#             "id" : "001",
#         })
# response = aws_client.send_message(
#             QueueUrl=SQS_URL,
#             MessageBody=message_body,
#             MessageGroupId="test"
#         )
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

delete_message()