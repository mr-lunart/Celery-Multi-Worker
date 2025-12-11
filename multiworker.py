from kombu.utils.url import safequote
from celery import Celery
import os
from dotenv import load_dotenv

load_dotenv() 

# Raw credentials from environment
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# URL-encode ONLY for broker URL
aws_access_key_encoded = safequote(AWS_ACCESS_KEY_ID)
aws_secret_key_encoded = safequote(AWS_SECRET_ACCESS_KEY)

# Use encoded credentials in broker URL
broker_url = f"sqs://"

app = Celery("multiworker", broker=broker_url)
app.conf.broker_transport_options = {
    "region": "eu-west-1",
    "wait_time_seconds": 15,
    "predefined_queues": {
        "phokus-benchmarking-queue": {
            "url": "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue",
            # Use RAW credentials here (NOT encoded)
            "access_key_id": AWS_ACCESS_KEY_ID,
            "secret_access_key": AWS_SECRET_ACCESS_KEY,
        },
    },
}
app.conf.task_default_queue="phokus-benchmarking-queue"
app.conf.task_acks_late=True
app.conf.task_reject_on_worker_lost=True


@app.task()
def generate_response_task(self, param1, param2, param3):
    print(param1)
    print(param2)
    print(param3)
    return