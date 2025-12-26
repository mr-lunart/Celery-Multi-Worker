import os
import boto3
from datetime import datetime, timedelta

from celery import Celery
from dotenv import load_dotenv

from celery.utils.log import get_task_logger

from scrapers.utils.s3_tool import download_json_file
from scrapers.facebook.brightdata_facebook import FacebookScrapper
from scrapers.instagram.brightdata_instagram import InstagramScrapper
from scrapers.linkedin.brightdata_linkedin import LinkedinScrapper
from scrapers.twitter.twitter_scraper_v2 import Scrape_Twitter
from scrapers.tiktok.tiktok_scraper_v2 import Scrape_TikTok
from scrapers.youtube.youtube_scraper_v2 import Scrape_Youtube

logger = get_task_logger(__name__)

load_dotenv(dotenv_path="config/.env") 

SQS_URL = "https://sqs.eu-west-1.amazonaws.com/820866026690/phokus-benchmarking-queue"
BRIGHTDATA_KEY_API = "5a945c61c1dca42391ab2f6b482b4d2f4245121b0452cad02b7414b6d9cf2513"
INGESTION_BUCKET="phokus-ingestion-raw-bucket"
CONFIG_BUCKET = 'phokus-benchmarking-collector-configuration-prod'
CONFIG_FILE_KEY = 'config.json'

config_credential = download_json_file(CONFIG_BUCKET, CONFIG_FILE_KEY)

aws_client = boto3.client(
    service_name="sqs",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="eu-west-1"
)

app = Celery("app_v2")
app.conf.broker_url = os.getenv("CELERY_REDIS")
app.conf.result_backend = os.getenv("CELERY_REDIS")
app.conf.task_routes = {}

@app.task(name='start', bind=True)
def start(self, event_body:dict):
    filename=event_body["filename"]
    receipt_handle=event_body["receipt_handle"]
    message_group_id=event_body["message_group_id"]
    message_pathfile=event_body["message_pathfile"]
    # process_data.apply_async(kwargs={'filename':event_body["filename"]})

@app.task(bind=True)
def remove_task_from_queue(receipt_handle:str, message_pathfile:str):
    delete_response = aws_client.delete_message(
                QueueUrl=SQS_URL,
                ReceiptHandle=receipt_handle
    )

@app.task(bind=True)
def tiktok_scraper(self, channel_name:str, post_limit:int):
    rds_credential = config_credential['pgsql']
    tiktok_credentials = config_credential['tiktok']
    api_key = tiktok_credentials['api_key']
    weekday_index = None
    
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)

    try:
        scrapper = Scrape_TikTok(
            weekday_index=weekday_index, 
            api_key=api_key, 
            conn_params=rds_credential, 
            input_channel=input_channel, 
            start_date=start_date, 
            end_date=end_date, 
            post_limit=num_of_post
        )
        scrapper.start()
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def twitter_scraper(self, channel_name:str, post_limit:int):
    rds_credential = config_credential['pgsql']
    twitter_credentials = config_credential['twitter']
    bearer_token = twitter_credentials['bearer_token1']
    weekday_index = None
    
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)
    
    try:
        scrapper = Scrape_Twitter(
            bearer_token=bearer_token, 
            conn_params=rds_credential, 
            input_channel=input_channel, 
            start_date=start_date, 
            end_date=end_date, 
            post_limit=num_of_post
        )
        scrapper.start()
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def youtube_scraper(self, channel_name:str, post_limit:int):
    rds_credential = config_credential['pgsql']
    youtube_credentials = config_credential['youtube']
    developer_key = youtube_credentials['developer_key']
    weekday_index = None
    
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)


    try:
        scrapper = Scrape_Youtube(
            weekday=weekday_index,
            developer_key=developer_key,
            conn_params=rds_credential, 
            input_channel=input_channel, 
            start_date=start_date, 
            end_date=end_date, 
            post_limit=num_of_post
        )
        scrapper.start()
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def facebook_scraper(self, url:str, post_limit:int):
    num_of_post = post_limit
    input_channel = url
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)
    param_input = [
        {
            "url":input_channel,
            "num_of_posts":num_of_post,
            "start_date":start_date, # MM-DD-YYYY
            "end_date":end_date, # MM-DD-YYYY
        }
    ]

    scrapper = FacebookScrapper(
        bucket_name=INGESTION_BUCKET,
        api_key=BRIGHTDATA_KEY_API,
        param_input=param_input
    )

    try:
        scrapper.start()
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def instagram_scraper(self, url:str, post_limit:int):
    num_of_post = post_limit
    input_channel = url
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)
    param_input = [
        {
            "url":input_channel,
            "num_of_posts":num_of_post,
            "start_date":start_date, # MM-DD-YYYY
            "end_date":end_date, # MM-DD-YYYY
            "post_type":"" # 'Post' / 'Reel'
        }
    ]

    scrapper = InstagramScrapper(
        bucket_name=INGESTION_BUCKET,
        api_key=BRIGHTDATA_KEY_API,
        param_input=param_input
    )

    try:
        scrapper.start()
    except Exception as err:
        logger.error(err)

# @app.task(bind=True)
# def linkedin_scraper(self, url:str, post_limit:int):
#     num_of_post = post_limit
#     input_channel = url
#     today = datetime.today()
#     start_date = today - timedelta(days=6)
#     end_date = today - timedelta(days=0)
#     param_input = [
#         {
#             "url":input_channel,
#             "start_date":start_date, # MM-DD-YYYY
#             "end_date":end_date, # MM-DD-YYYY
#             "post_type":"" # 'Post' / 'Reel'
#         }
#     ]
    
#     scrapper = LinkedinScrapper(
#         bucket_name=INGESTION_BUCKET,
#         api_key=BRIGHTDATA_KEY_API,
#         param_input=param_input
#     )

#     try:
#         scrapper.start()
#     except Exception as err:
#         logger.error(err)