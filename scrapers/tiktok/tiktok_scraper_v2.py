from tikapi import TikAPI, ValidationException, ResponseException
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
import random
import time
import os

from sqlalchemy import text
from .channel_model import ChannelModel
from .post_model import PostModel
from scrapers.utils.get_engine import get_engine
from scrapers.utils.logger import generate_log_object

class Scrape_TikTok():

    ROOT_URL = "https://www.tiktok.com"
    BUCKET_NAME = "phokus-ingestion-raw-bucket"
    PROFILE_PICTURE_OBJ_URL = "https://social-external-tracking.s3.eu-west-1.amazonaws.com/tiktok/profile_pictures/{}.png"
    PLATFORM = "tiktok"
    WEEKDAYS = {}
    
    def __init__(
            self,
            api_key, 
            conn_params, 
            input_channel, 
            start_date, 
            end_date, 
            post_limit
        ):
        """ Scraper uses TikApi, TikTok's unofficial API https://tikapi.io/developer
            Separate requests made for fetching channel data and posts data.
            
            Intended to scrape TikTok on a weekly basis, posts tracked for 4 weeks (28 days) only.
        """
        self.log = generate_log_object(self.PLATFORM, input_channel)

        self.input_channel = input_channel
        self.start_date = start_date
        self.end_date = end_date
        self.post_limit = post_limit
        
        self.today = datetime.today()
        delta = timedelta(days=28)
        self.last_date = (self.today - delta)
        self.counter = 0

        # Settings
        self.today_date_string = self.today.strftime("%d%m%Y")
        self.primary_keys = {}
        
        self.api_key = api_key
        
        # Instanciate TikAPI
        self.api = TikAPI(api_key)
        self.session = get_engine(conn_params)
        
        # Get S3 bucket
        boto3_session = boto3.Session()
        s3_resource = boto3_session.resource(
            service_name='s3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket = s3_resource.Bucket(self.BUCKET_NAME)
        
    def start(self):
        channel_credential = self.get_channel_name(self.input_channel)
        self.platform_channel_name = channel_credential["channel_name"]
        self.channel_input_id = channel_credential["id"]
        # earlier scrapped data
        earlier_data_channel, earlier_data_post = self.get_earlier_data(self.input_channel)

        # scrape channel and post
        data_channel = self.scrape_channels(earlier_data_channel)
        if data_channel == None and not isinstance(data_channel, dict):
            self.log.error('failed scraping channel information')
            raise Exception('failed scraping channel information')

        status_posts = self.scrape_posts(data_channel, earlier_data_post)
        if status_posts == None or status_posts == False :
            self.log.error('failed scraping channel posts')
            raise Exception('failed scraping channel posts')
        
        current_date_path = datetime.now().strftime("%Y/%m/%d")
        s3_key = f"tiktok/{current_date_path}/"
        self.save_to_s3(s3_key=s3_key)
        self.log.info('Scraping is finished')

    def save_to_s3(self, s3_key:str):
        channel_name = self.platform_channel_name
        timestamp_data = datetime.now().strftime("%Y%m%d")
        profile_name = f"tiktok-profile-{channel_name}-{timestamp_data}.parquet"
        post_name = f"tiktok-post-{channel_name}-{timestamp_data}.parquet"
       
        try:
            profile = pd.DataFrame(self.channel_profile_metric.get_dataset())
            profile.to_parquet(profile_name, index=False)
            self.bucket.upload_file(profile_name, f"{s3_key}{profile_name}")

            post = pd.DataFrame(self.channel_posts_metric.get_dataset())
            post.to_parquet(post_name, index=False)
            self.bucket.upload_file(post_name, f"{s3_key}{post_name}")
        except Exception as err:
            self.log.error(f"Error uploading data to S3:{err}")
            raise err
        
        try:
            for filepath in (profile_name, post_name):
                if os.path.exists(filepath):
                    os.remove(filepath)
                else:
                    print(f'filepath {filepath} not found')
            self.log.info(f'file tiktok {channel_name} parquet is deleted')
        except Exception as err:
            self.log.error(f"Error deleting data:{err}")
            raise err

    def scrape_channels(self, earlier_data_channel):
        # Scraping Channel
        channel = self.platform_channel_name
        channel_input_id = self.channel_input_id
        self.log.info(f"Scraping channel {channel}")
        first_scraped_at = None
        try:
            response = self.api.public.check(username=channel)
            json_obj = response.json()
            if json_obj.get('status') == 'success':
                self.log.info(f'Status API Channel {channel} success')
            else:
                self.log.info(f'Status API Channel {channel} error')
                return
        except ValidationException as e:
            self.log.error(f"Error in {channel} Message: {e}, Error Field: {e.field}")
            return
        except ResponseException as e:
            self.log.error(f"Error in {channel} Message: {e}, Error Code: {e.response.status_code}")
            if '403' in str(e) or 'try again' in str(e).lower():
                raise Exception(e)
            return
        
        try:
            channel_id = json_obj.get('userInfo', {}).get('user', {}).get('id', None)
            channel_name = json_obj.get('userInfo', {}).get('user', {}).get('nickname', None) 
            if int(channel_id) in earlier_data_channel["channel_id"].values:
                first_scraped_at = earlier_data_channel[earlier_data_channel["channel_id"] == int(channel_id)]["first_scraped_at"].values[0]
                first_scraped_at = datetime.fromtimestamp(first_scraped_at.tolist()/1e9).strftime("%Y-%m-%d %H:%M:%S")
            
            uniqueId = json_obj.get('userInfo', {}).get('user', {}).get('uniqueId', None)
            avatarLarger = json_obj.get('userInfo', {}).get('user', {}).get('avatarLarger', None)
            avatarMedium = json_obj.get('userInfo', {}).get('user', {}).get('avatarMedium', None)
            avatarThumb = json_obj.get('userInfo', {}).get('user', {}).get('avatarThumb', None)
            channel_data = {
                "channelId": channel_id,
                "channelName": uniqueId,
                "channelUrl": self.ROOT_URL + "/@{}".format(channel),
                "nickname": channel_name,
                "followerCount": json_obj.get('userInfo', {}).get('stats', {}).get('followerCount', None),
                "followingCount": json_obj.get('userInfo', {}).get('stats', {}).get('followingCount', None),
                "heartCount": json_obj.get('userInfo', {}).get('stats', {}).get('heartCount', None),
                "videoCount": json_obj.get('userInfo', {}).get('stats', {}).get('videoCount', None),
                "secUid": json_obj.get('userInfo', {}).get('user', {}).get('secUid', None),
                "bioLink": json_obj.get('userInfo', {}).get('user', {}).get('bioLink', None),  
                "avatarLarger": avatarLarger,
                "avatarMedium":avatarMedium,
                "avatarThumb": avatarThumb,
                "signature": json_obj.get('userInfo', {}).get('user', {}).get('signature', None),
                "createdAt": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                "firstScrapedAt": first_scraped_at if first_scraped_at else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                "date": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                "channel_input_id": channel_input_id,
                "profile_picture_s3_obj_url": self.PROFILE_PICTURE_OBJ_URL.format(uniqueId),
            }
            channel_model = ChannelModel()
            channel_model.add_item(channel_data)
            self.channel_profile_metric = channel_model
            self.primary_keys[channel_id] = {'channel_input_id':channel_input_id}
            self.log.info(f"Channel {channel_model} saved")
            return channel_data
        
        except Exception as ex:
            self.log.error(f"Exception in {channel} parsing channel data: {ex}")
            return
        
    def scrape_posts(self, channel_data, earlier_data_post):
        # Scraping Posts
        secUid = channel_data.get("secUid", None)
        if secUid is None:
            self.log.info("Error: secUid is NULL")
            return False
        try:
            random_delay = random.choice([60, 120, 180])
            self.log.info(f"Delay {random_delay} sec before calling API Post endpoint")
            time.sleep(random_delay)
            self.log.info(f"Scraping posts from channel {channel_data['channelName']}")
            # create post_models, post model handle all data post from parse post response
            self.posts_model = PostModel()
            try:
                response = self.api.public.posts(secUid=secUid, count=self.post_limit)
                while(response):
                    is_data = self.parse_post_response(response.json(), earlier_data_post)
                    if not is_data:
                        break
                    cursor = response.json().get('cursor', None)
                    response = response.next_items()
                self.channel_posts_metric = self.posts_model
                return True

            except Exception as e:
                if '403' in str(e) or 'try again' in str(e).lower():
                    random_delay = random.choice([300, 310, 320])
                    self.log.info(f"Delay {random_delay} sec before re-calling API Post endpoint")
                    time.sleep(random_delay)
                    response = self.api.public.posts(secUid=secUid, count=self.post_limit)
                    while(response):
                        is_data = self.parse_post_response(response.json(), earlier_data_post)
                        if not is_data:
                            break
                        cursor = response.json().get('cursor', None)
                        response = response.next_items()
                    self.channel_posts_metric = self.posts_model
                    return True
        
            self.log.info(f"Saved {self.counter} post(s) for channel {channel_data['channelName']}")
            
            
        except ValidationException as e:
            error_message = f"Error in validation {channel_data['channelName']} Message: {e}, Error Field: {e.field}"
            self.log.error(error_message)
            return False

        except ResponseException as e:
            error_message = f"Error in response {channel_data['channelName']} Message: {e}, Error Code: {e.response.status_code}"
            self.log.error(error_message)
            return False

    def parse_post_response(self, json_obj, earlier_data):
        """ 
        Function responsible for parsing post data
        """
        is_data = False
        if json_obj.get('itemList', None) is None:
            return is_data
        
        for item in json_obj['itemList']:
            try:
                count, first_scraped_at = None, None
                post_id = item.get('id', None)
                channel_id = item.get('author', {}).get('id', None)
                
                createTime = item.get('createTime', None)
                # Skip if createTime older than last post date
                if createTime < self.last_date.timestamp():
                    continue

                is_data = True

                codecType = item.get('video', {}).get('bitrateInfo', [])
                if codecType:
                    codecType = codecType[0].get('CodecType', None)

                textExtra = item.get('contents', [])
                if textExtra:
                    textExtra = textExtra[0].get('textExtra', [])[0] if textExtra[0].get('textExtra', []) else None
                    
                if int(post_id) in earlier_data["post_id"].values:
                    count = int(earlier_data[earlier_data["post_id"] == int(post_id)]["count"].values[0]) + 1
                    first_scraped_at = earlier_data[earlier_data["post_id"] == int(post_id)]["first_scraped_at"].values[0]
                    first_scraped_at = datetime.fromtimestamp(first_scraped_at.tolist()/1e9).strftime("%Y-%m-%d %H:%M:%S")
                    
                uniqueId = item.get('author', {}).get('uniqueId', None)
                cover = item.get('music', {}).get('coverThumb', None)
                origin_cover = item.get('video', {}).get('originCover', None)
                dynamic_cover = item.get('video', {}).get('dynamicCover', None)

                post_data = {
                    "postId": post_id,
                    "desc": item.get('desc', None),
                    "createTime": datetime.fromtimestamp(createTime).strftime("%Y-%m-%d %H:%M:%S"),
                    "height": item.get('video', {}).get('height', None),
                    "width": item.get('video', {}).get('width', None),
                    "duration": item.get('video', {}).get('duration', None),
                    "format": item.get('video', {}).get('format', None),
                    "ratio": item.get('video', {}).get('ratio', None),
                    "videoQuality": item.get('video', {}).get('videoQuality', None),
                    "definition": item.get('video', {}).get('definition', None),
                    "originCover": origin_cover,
                    "reflowCover": item.get('video', {}).get('reflowCover', None),
                    "dynamicCover": dynamic_cover,
                    "downloadAddr": item.get('video', {}).get('downloadAddr', None),
                    "playAddr": item.get('video', {}).get('playAddr', None),
                    "codecType": codecType,
                    "musicId": item.get('music', {}).get('id', None),
                    "musicPlayUrl": item.get('music', {}).get('playUrl', None),
                    "musicDuration": item.get('music', {}).get('duration', None),
                    "musicTitle": item.get('music', {}).get('title', None),
                    "musicAlbum": item.get('music', {}).get('album', None),
                    "cover": cover,
                    "commentCount": item.get('stats', {}).get('commentCount', None),
                    "diggCount": item.get('stats', {}).get('diggCount', None),
                    "playCount": item.get('stats', {}).get('playCount', None),
                    "shareCount": item.get('stats', {}).get('shareCount', None),
                    "textExtra": textExtra,
                    "digged": item.get('digged', None),
                    "isAd": item.get('isAd', None),
                    "forFriend": item.get('forFriend', None),
                    "duetEnabled": item.get('duetEnabled', None),
                    "duetDisplay": item.get('duetDisplay', None),
                    "itemCommentStatus": item.get('itemCommentStatus', None),
                    "itemMute": item.get('itemMute', None),
                    "privateItem": item.get('privateItem', None),
                    "stitchDisplay": item.get('stitchDisplay', None),
                    "stitchEnabled": item.get('stitchEnabled', None),
                    "shareEnabled": item.get('shareEnabled', None),
                    "showNotPass": item.get('showNotPass', None),
                    "vl1": item.get('vl1', None),
                    "challenges": item.get('challenges', None),
                    "stickersOnItem": item.get('stickersOnItem', None),
                    "channelName": uniqueId,
                    "channel_id": channel_id,
                    "createdAt": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                    "firstScrapedAt": first_scraped_at if first_scraped_at else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                    "count": count if count else 1,
                    "channel_input_id": self.primary_keys[channel_id]['channel_input_id']
                }
                self.posts_model.add_item(post_data)
                self.counter += 1
                if self.counter >= self.post_limit:
                    is_data = False
                    break
                
            except Exception as ex:
                self.log.error(f"Exception in parsing post data: {ex}")
            
        return is_data
    
    def get_earlier_data(self, input_channel:str):
        sql_query_channel = f"""
        SELECT channel_id, min(first_scraped_at) as first_scraped_at
        FROM postgres.scraping.tiktok_channels
        LEFT JOIN postgres.scraping.benchmarking_channel_input ON tiktok_channels.channel_input_id = benchmarking_channel_input.id
        WHERE organisation = '{input_channel}'
        GROUP BY channel_id"""

        sql_query_post = f"""
        SELECT post_id, max(count) as count, min(first_scraped_at) as first_scraped_at
        FROM postgres.scraping.tiktok_posts
        LEFT JOIN postgres.scraping.benchmarking_channel_input ON tiktok_posts.channel_input_id = benchmarking_channel_input.id
        WHERE organisation = '{input_channel}'
        GROUP BY post_id"""
        
        result = self.session.execute(text(sql_query_channel))
        rows = result.fetchall()
        earlier_data_channel = pd.DataFrame(rows, columns=result.keys())
        
        result = self.session.execute(text(sql_query_post))
        rows = result.fetchall()
        earlier_data_post = pd.DataFrame(rows, columns=result.keys())

        # TO DO
        # close the connection, this is last sql transaction, another sql transaction is forbidden after this
        # if there need of another transaction, first refactor the SQL session maker engine
        self.session.close()
            
        return earlier_data_channel, earlier_data_post
    
    def get_channel_name(self, input_channel):
        sql_query = f"""
        SELECT id, organisation, organisation_id, scrape_media, tiktok_channel AS channel_name
        FROM postgres.scraping.benchmarking_channel_input
        WHERE organisation = '{input_channel}';
        """
        result = self.session.execute(text(sql_query))
        rows = result.fetchone()
        keys = result.keys()
        data = self._fetchone_to_dict(keys, rows)
        return data
    
    def _fetchone_to_dict(self, keys, rows):
        """for internal use only."""
        if rows:
            return dict(zip(keys, rows))
        return rows
    