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
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
app.conf.task_reject_on_worker_lost = True
app.conf.task_routes = {}


@app.task(name='start', bind=True)
def gateway(self, event_body:dict):
    # message_group_id=event_body.get("message_group_id","")
    receipt_handle=event_body.get("receipt_handle","")
    message_pathfile=event_body.get("message_pathfile","")
    platform=event_body.get("platform","")
    organization=event_body.get("organization","")
    start_date=event_body.get("start_date","")
    end_date=event_body.get("end_date","")
    num_of_post=event_body.get("num_of_post","")
    logger.info(f"start_scrapper::{platform}::message_path::{message_pathfile}::organization{organization}")
    if platform == "facebook":
        facebook_scraper.apply_async(kwargs={
            'channel_name':organization,
            'date_start':start_date,
            'date_end':end_date,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "instagram":
        instagram_scraper.apply_async(kwargs={
            'channel_name':organization,
            'date_start':start_date,
            'date_end':end_date,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "tiktok":
        tiktok_scraper.apply_async(kwargs={
            'channel_name':organization,
            'date_start':start_date,
            'date_end':end_date,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "twitter":
        twitter_scraper.apply_async(kwargs={
            'channel_name':organization,
            'date_start':start_date,
            'date_end':end_date,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    elif platform == "youtube":
        youtube_scraper.apply_async(kwargs={
            'channel_name':organization,
            'date_start':start_date,
            'date_end':end_date,
            'post_limit':num_of_post,
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
    return f"start_scrapper::{platform}::message_path::{message_pathfile}::organization{organization}"

@app.task(bind=True)
def remove_task_from_queue(self, receipt_handle:str, message_pathfile:str):
    try:
        aws_client.delete_message(
                    QueueUrl=SQS_URL,
                    ReceiptHandle=receipt_handle
        )
        if os.path.exists(message_pathfile):
            os.remove(message_pathfile)
            print(f"file is deleted {message_pathfile}")
            return f"success delete task {message_pathfile}"
        else:
            print(f"file {message_pathfile} not exist")
            return f"file {message_pathfile} not exist"
        
    except Exception as err:
        logger.error(f"failed removing task::{message_pathfile} {err}")
        raise err

@app.task(bind=True)
def tiktok_scraper(self, channel_name:str, date_start:str, date_end:str, post_limit:int, receipt_handle:str, message_pathfile:str):
    rds_credential = config_credential['pgsql']
    tiktok_credentials = config_credential['tiktok']
    api_key = tiktok_credentials['api_key']
    
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)

    try:
        scrapper = Scrape_TikTok( 
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
        return f"succes_scrapping::{input_channel}::message_path::{message_pathfile}::total_post{post_limit}"
    except Exception as err:
        logger.error(err)
        raise err

@app.task(bind=True)
def twitter_scraper(self, channel_name:str, date_start:str, date_end:str, post_limit:int, receipt_handle:str, message_pathfile:str):
    rds_credential = config_credential['pgsql']
    twitter_credentials = config_credential['twitter']
    bearer_token = twitter_credentials['bearer_token1']
    
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
        return f"succes_scrapping::{input_channel}::message_path::{message_pathfile}::total_post{post_limit}"
    except Exception as err:
        logger.error(err)
        raise err

@app.task(bind=True)
def youtube_scraper(self, channel_name:str, date_start:str, date_end:str, post_limit:int, receipt_handle:str, message_pathfile:str):
    rds_credential = config_credential['pgsql']
    youtube_credentials = config_credential['youtube']
    developer_key = youtube_credentials['developer_key']
    
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    start_date = today - timedelta(days=6)
    end_date = today - timedelta(days=0)
    
    try:
        scrapper = Scrape_Youtube(
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
        return f"succes_scrapping::{input_channel}::message_path::{message_pathfile}::total_post{post_limit}"
    except Exception as err:
        logger.error(err)
        raise err

@app.task(bind=True)
def facebook_scraper(self, channel_name:str, date_start:str, date_end:str, post_limit:int, receipt_handle:str, message_pathfile:str):
    rds_credential = config_credential['pgsql']
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    if date_start:
        pass
    else:
        start_date = today - timedelta(days=6)
        date_start = start_date.strftime("%Y-%m-%d")
    if date_end:
        pass
    else:
        end_date = today - timedelta(days=0)
        date_end = end_date.strftime("%Y-%m-%d")


    try:
        scrapper = FacebookScrapper(
            bucket_name=INGESTION_BUCKET,
            api_key=BRIGHTDATA_KEY_API,
            conn_params=rds_credential,
            input_channel=input_channel,
            start_date=date_start,
            end_date=date_end,
            post_limit=num_of_post,
        )
        scrapper.start()
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
        return f"succes_scrapping::{input_channel}::message_path::{message_pathfile}::total_post{post_limit}"
    except Exception as err:
        logger.error(err)
        raise err

@app.task(bind=True)
def instagram_scraper(self, channel_name:str, date_start:str, date_end:str, receipt_handle:str, message_pathfile:str, post_limit:int=10):
    rds_credential = config_credential['pgsql']
    num_of_post = post_limit
    input_channel = channel_name
    today = datetime.today()
    if date_start:
        pass
    else:
        start_date = today - timedelta(days=6)
        date_start = start_date.strftime("%Y-%m-%d")
    if date_end:
        pass
    else:
        end_date = today - timedelta(days=0)
        date_end = end_date.strftime("%Y-%m-%d")

    try:
        scrapper = InstagramScrapper(
            bucket_name=INGESTION_BUCKET,
            api_key=BRIGHTDATA_KEY_API,
            conn_params=rds_credential,
            input_channel=input_channel,
            start_date=start_date,
            end_date=end_date,
            post_limit=num_of_post)
        scrapper.start()
        remove_task_from_queue.apply_async(kwargs={
            'receipt_handle':receipt_handle,
            'message_pathfile':message_pathfile})
        return f"succes_scrapping::{input_channel}::message_path::{message_pathfile}::total_post{post_limit}"
    except Exception as err:
        logger.error(err)
        raise err
