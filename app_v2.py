import os
import boto3
from datetime import datetime, timedelta

from celery import Celery
from dotenv import load_dotenv

from celery.utils.log import get_task_logger
from celery import chain

from scrapers.utils.s3_tool import download_json_file
from scrapers.facebook.brightdata_facebook import FacebookScrapper
from scrapers.instagram.brightdata_instagram import InstagramScrapper
# from scrapers.linkedin.brightdata_linkedin import LinkedinScrapper
from scrapers.twitter.twitter_scraper_v2 import Scrape_Twitter
from scrapers.tiktok.tiktok_scraper_v2 import Scrape_TikTok
from scrapers.youtube.youtube_scraper_v2 import Scrape_Youtube

logger = get_task_logger(__name__)

load_dotenv(dotenv_path="config/.env", override=True) 

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
    # filename=event_body.get("filename","")
    logger.info(event_body)
    receipt_handle=event_body.get("receipt_handle","")
    message_group_id=event_body.get("message_group_id","")
    message_pathfile=event_body.get("message_pathfile","")
    platform=event_body.get("platform","")
    channel_name=event_body.get("channel_name","")
    url=event_body.get("url","")
    start_date=event_body.get("start_date","")
    end_date=event_body.get("end_date","")
    num_of_post=event_body.get("num_of_post","")
    if platform == "facebook":
        facebook_scraper.apply_async(kwargs={
            'url':url,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "instagram":
        instagram_scraper.apply_async(kwargs={
            'url':url,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "tiktok":
        tiktok_scraper.apply_async(kwargs={
            'channel_name':channel_name,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "twitter":
        twitter_scraper.apply_async(kwargs={
            'channel_name':channel_name,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "youtube":
        youtube_scraper.apply_async(kwargs={
            'channel_name':channel_name,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})

@app.task(bind=True)
def remove_task_from_queue(self, receipt_handle:str, message_pathfile:str):
    try:
        delete_response = aws_client.delete_message(
                    QueueUrl=SQS_URL,
                    ReceiptHandle=receipt_handle
        )
        if os.path.exists(message_pathfile):
            os.remove(message_pathfile)
            print(f"file is deleted {message_pathfile}")
        else:
            print(f"Failed to delete file {message_pathfile}")
    except Exception as err:
        logger.error(f"{message_pathfile} {err}")

@app.task(bind=True)
def tiktok_scraper(self, channel_name:str, post_limit:int, receipt_handle:str, message_pathfile:str):
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
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def twitter_scraper(self, channel_name:str, post_limit:int, receipt_handle:str, message_pathfile:str):
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
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def youtube_scraper(self, channel_name:str, post_limit:int, receipt_handle:str, message_pathfile:str):
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
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def facebook_scraper(self, url:str, post_limit:int, receipt_handle:str, message_pathfile:str):
    num_of_post = post_limit
    input_channel = url
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)
    param_input = [
        {
            "url":input_channel,
            "num_of_posts":num_of_post,
            "start_date":start_date.strftime("%Y-%m-%d"), # MM-DD-YYYY
            "end_date":end_date.strftime("%Y-%m-%d"), # MM-DD-YYYY
        }
    ]

    scrapper = FacebookScrapper(
        bucket_name=INGESTION_BUCKET,
        api_key=BRIGHTDATA_KEY_API,
        param_input=param_input
    )

    try:
        scrapper.start()
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile}
        )
    except Exception as err:
        logger.error(err)

@app.task(bind=True)
def instagram_scraper(self, url:str, post_limit:int, receipt_handle:str, message_pathfile:str):
    num_of_post = post_limit
    input_channel = url
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)
    param_input = [
        {
            "url":input_channel,
            "num_of_posts":num_of_post,
            "start_date":start_date.strftime("%Y-%m-%d"), # MM-DD-YYYY
            "end_date":end_date.strftime("%Y-%m-%d"), # MM-DD-YYYY
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
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
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