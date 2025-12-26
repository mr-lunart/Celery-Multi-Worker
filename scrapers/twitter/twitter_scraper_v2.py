from time import sleep
from datetime import datetime, timedelta
from collections import Counter
import pandas as pd
import requests
import boto3
import random
import time
import os

from sqlalchemy import text
from .channel_model import ChannelModel
from .post_model import PostModel
from scrapers.engine import get_engine
from scrapers.logger import generate_log_object
from scrapers.weekdays import calculate_weekdays


class Scrape_Twitter:
    
    BUCKET_NAME = "social-external-tracking"
    PROFILE_PICTURE_OBJ_URL = "https://social-external-tracking.s3.eu-west-1.amazonaws.com/twitter/profile_pictures/{}.png"
    PLATFORM = "twitter"
    POST_URL = "https://twitter.com/{}/status/{}"
    WEEKDAYS = {}

    def __init__(self, bearer_token, conn_params, input_channel, start_date, end_date, post_limit):
        """ This scraper uses Twitter's API v2 endpoints
            https://developer.twitter.com/en/docs/twitter-api
        """

        self.logger = generate_log_object(self.PLATFORM)
        
        # Settings
        self.primary_keys = {}
        
        self.bearer_token = bearer_token
        self.input_channel = input_channel
        self.start_date = start_date
        self.end_date = end_date
        self.platform_channel_name = None
        self.channel_input_id = None
        
        # DB connection
        self.session = get_engine(conn_params)
        self.post_limit = post_limit
        
        # Get S3 bucket
        session = boto3.Session()
        s3 = session.resource('s3')
        self.bucket = s3.Bucket(self.BUCKET_NAME)
        
    def start(self):
        channel = self.get_channel_name(self.input_channel)
        self.platform_channel_name = channel[3]
        self.channel_input_id = channel[0]
        # Get earlier scraped data
        earlier_data_channel, earlier_data_post = self.get_earlier_data(self.input_channel)
        # Scraping Channels and Posts
        status = self.scrape_channels_posts(earlier_data_channel, earlier_data_post)
        if status == None or status == False :
            self.logger.error('failed scraping channel posts')
            return

        self.save_to_s3(s3_key="twitter/test_scrapper/")
        self.logger.info('Scraping is finished')

    def save_to_s3(self, s3_key:str):
        channel_name = self.platform_channel_name
        timestamp_data = datetime.now().strftime("%Y%m%d%H%M%S")
        profile_name = f"twitter-profile-{channel_name}-{timestamp_data}.parquet"
        post_name = f"twitter-post-{channel_name}-{timestamp_data}.parquet"
       
        try:
            profile = pd.DataFrame(self.channel_profile_metric.get_dataset())
            profile.to_parquet(profile_name, index=False)
            self.bucket.upload_file(profile_name, f"{s3_key}{profile_name}")

            post = pd.DataFrame(self.channel_posts_metric.get_dataset())
            post.to_parquet(post_name, index=False)
            self.bucket.upload_file(post_name, f"{s3_key}{post_name}")
        except Exception as err:
            self.logger.error(f"Error uploading data to S3:{err}")
            return
        
        try:
            for filepath in (profile_name, post_name):
                if os.path.exists(filepath):
                    os.remove(filepath)
                else:
                    print(f'filepath {filepath} not found')
            self.logger.info(f'file twitter {channel_name} parquet is deleted')
        except Exception as err:
            self.logger.error(f"Error deleting data:{err}")
            return

    def scrape_channels_posts(self, earlier_data_channel, earlier_data_post):
        try:
            # Parse Channel data
            url = self.create_url_channels(self.platform_channel_name)
            json_response = self.connect_to_endpoint(url=url, params=None, channel_name=None)
            items = json_response['data']
            
            for item in items:
                first_scraped_at = None
                channel_id = item.get('id', None)

                if int(channel_id) in earlier_data_channel["channel_id"].values:
                    first_scraped_at = earlier_data_channel[earlier_data_channel["channel_id"] == int(channel_id)]["first_scraped_at"].values[0]
                    first_scraped_at = first_scraped_at.strftime("%Y-%m-%d")

                profile_img = item.get('profile_image_url', None)
                profile_img = profile_img.replace("normal", "400x400") if profile_img else profile_img
                website = item.get('entities', {}).get('url', {}).get('urls', [])
                website = website[0].get('expanded_url', None) if website else None
                screen_name = item.get('username', None)
                # scrape_media = channels[channels["twitter_channel"].str.lower() == screen_name.lower()]["scrape_media"].values[0]
                channel_input_id = int(self.channel_input_id)
                
                channel_data = {
                    "ChannelID": channel_id,
                    "ChannelName": self.platform_channel_name,
                    "Description": item.get('description', None),
                    "FollowersCount": item.get('public_metrics', {}).get('followers_count', None),
                    "FollowingCount": item.get('public_metrics', {}).get('following_count', None),
                    "ListedCount": item.get('public_metrics', {}).get('listed_count', None),
                    "TweetCount": item.get('public_metrics', {}).get('tweet_count', None),
                    "Location": item.get('location', None),
                    "Website": website,
                    "ScreenName": screen_name,
                    "ProfileImageURLHttps": profile_img,
                    "Verified": item.get('verified', None),
                    "DateCreated": item.get('first_scraped_at', None).split('T')[0] if item.get('first_scraped_at', None) else None, 
                    "CreatedAt": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                    "FirstScrapedAt": first_scraped_at if first_scraped_at else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                    "channel_input_id": channel_input_id,
                    "profile_picture_s3_obj_url": self.PROFILE_PICTURE_OBJ_URL.format(screen_name),
                }

                channel_model = ChannelModel()
                channel_model.add_item(channel_data)
                self.channel_profile_metric = channel_model

                self.primary_keys[channel_id] = {'channel_input_id': channel_input_id}
                self.logger.info(f"Channel {item.get('username', None)} - {item.get('name', None)} saved")
                break # Only one channel expected

        except Exception as ex:
            self.session.rollback()
            message = f"Exception in parsing channel data: {ex}"
            self.logger.error(message)
            raise Exception(message)


        # post scrapper ############# 
        self.logger.info(f"Scraping posts from channel {self.platform_channel_name}")
        url = self.create_url_posts(channel_id)
        
        next_token = "token"
        iteration = 0
        counter_media = 0
        self.posts_model = PostModel()

        while next_token is not None: # Looping API Twitter using pagination
            random_delay = random.choice([45, 50, 55])
            self.logger.info(f"Delay {random_delay} sec before calling API Paging endpoint")
            time.sleep(random_delay)

            if iteration == 0:
                next_token = None
            iteration += 1
            self.logger.info(f"Fetching posts in channel {self.platform_channel_name} with page iteration number {iteration}")
            
            params = self.get_params(next_token)
            json_response = self.connect_to_endpoint(url=url, params=params, channel_name=self.platform_channel_name)
            if 'error' in str(json_response) and 'usage cap exceeded' in str(json_response).lower(): # ERROR USAGE CAP MONTLY LIMIT
                error_message = f"License Error when get posts from channel ID {channel_id} with {json_response}"
                self.logger.error(error_message)
                raise Exception(error_message)
            elif 'error' in str(json_response) and 'usage cap exceeded' not in str(json_response).lower(): # OTHER ERRORS
                error_message = f"Error get posts from channel ID {channel_id} with {json_response}"
                self.logger.error(error_message)
                continue
            elif not json_response.get("data", None): # NO DATA IN RESPONSE JSON
                self.logger.info(f"No response received for channel ID {channel_id}")
                break

            items = json_response['data']
            medias = json_response.get('includes', {}).get('media', [])
            next_token = json_response.get('meta', {}).get('next_token', None)

            for item in items:
                try:
                    first_scraped_at, count, media_type,  = None, None, None
                    media_urls = []
                    post_id = item.get('id', None)

                    if int(post_id) in earlier_data_post["post_id"].values:
                        count = int(earlier_data_post[earlier_data_post["post_id"] == int(post_id)]["count"].values[0])

                        first_scraped_at = earlier_data_post[earlier_data_post["post_id"] == int(post_id)]["first_scraped_at"].values[0]
                        first_scraped_at = first_scraped_at.strftime("%Y-%m-%d")

                    mentions = item.get('entities', {}).get('mentions', None)
                    retweeted = item.get('referenced_tweets', [])
                    retweeted = retweeted[0].get('type', None) == 'retweeted' if retweeted else False
                    post_media_keys = item.get('attachments', {}).get('media_keys', [])
                    date_created = item.get('first_scraped_at', None)

                    for post_media_key in post_media_keys:
                        for media in medias:
                            if media.get('media_key', None) == post_media_key:
                                media_type = media.get('type', None)
                                if media_type == "photo":
                                    media_urls.append(media.get('url', None))
                                elif media_type == "video":
                                    media_urls.append(media.get('preview_image_url', None))
                                    
                    post_data = {
                        "PostID": post_id,
                        "ChannelName": self.platform_channel_name,
                        "Text": item.get('text', None),
                        "CommentCount": item.get('public_metrics', {}).get('reply_count', None),
                        "RetweetCount": item.get('public_metrics', {}).get('retweet_count', None),
                        "FavoriteCount": item.get('public_metrics', {}).get('like_count', None),
                        "QuoteCount": item.get('public_metrics', {}).get('quote_count', None),
                        "BookmarkCount": item.get('public_metrics', {}).get('bookmark_count', None),
                        "ViewCount": item.get('public_metrics', {}).get('impression_count', None),
                        "Hashtags": item.get('entities', {}).get('hashtags', None),
                        "IsRetweet": retweeted,
                        "UserMentions": mentions,
                        "UserMentionsCount": len(mentions) if mentions else None,
                        "Media": media_urls if media_urls else None,
                        "MediaType": media_type if media_type else None,
                        "PostURL": self.POST_URL.format(self.platform_channel_name, post_id),
                        # "DateCreated": item.get('first_scraped_at', None).replace("T", " ").replace(".000Z", ""), # Commented out by Dmitri 2024-05-03 due to error
                        "DateCreated": date_created.replace("T", " ").replace(".000Z", "") if date_created else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                        "ChannelID": channel_id,
                        "Count": count + 1 if count else 1,
                        "CreatedAt": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                        "FirstScrapedAt": first_scraped_at if first_scraped_at else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                        "channel_input_id": self.primary_keys[channel_id]['channel_input_id']
                    }
                    
                    self.posts_model.add_item(post_data)
                    counter_media += 1
                    self.logger.info(f"Media Number: {counter_media} Post ID: {post_id} from Channel: {self.platform_channel_name}")

                except Exception as ex:
                    self.logger.error(f"Exception in {self.platform_channel_name} parsing posts data: {ex}")

                if counter_media >= self.post_limit: # Breaking from token pagination iteration if media_count is greater equal to threshold
                    break
            if counter_media >= self.post_limit: # Breaking from while true pagination iteration
                break
        
        self.channel_posts_metric = self.posts_model
        self.logger.info(f"Saved {counter_media} post(s) for channel {self.platform_channel_name}")
        return True
        
    def create_url_channels(self, channel_name):
        """ Method that wraps up the URL creation
            User fields are adjustable, options include: created_at, description, entities, id, location, name, pinned_tweet_id, 
            profile_image_url, protected, public_metrics, url, username, verified, and withheld
        """
        
        usernames = "usernames={}".format(channel_name)
        user_fields = "user.fields=description,created_at,entities,location,url,profile_image_url,username,public_metrics,verified"
        url = "https://api.twitter.com/2/users/by?{}&{}".format(usernames, user_fields)

        return url
    
    def create_url_posts(self, channel_id):
        return "https://api.twitter.com/2/users/{}/tweets".format(channel_id)
    
    def get_params(self, next_token):
        """ Tweet fields are adjustable.
            Options include:
            attachments, author_id, context_annotations, conversation_id, created_at, entities, geo, id, in_reply_to_user_id, lang, non_public_metrics, 
            organic_metrics, possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets, source, text, and withheld
        """
        return {
            "tweet.fields": "created_at,entities,attachments,author_id,source,public_metrics,referenced_tweets",
            "expansions": [
              "attachments.media_keys"
            ],
            "media.fields": [
                "url,preview_image_url"
            ],
            # "start_time": self.last_month.strftime("%Y-%m-%dT%H:%M:%SZ"), # Disabled 2024-07-23
            "start_time": self.start_date.strftime("%Y-%m-%dT%H:%M:%SZ"), # Added 2024-07-30
            "end_time": self.end_date.strftime("%Y-%m-%dT%H:%M:%SZ"), # Added 2024-07-30
            "max_results": 5, # Modified 2024-07-25 to decrease the limit post number result
            "pagination_token": next_token
           }
    
    def bearer_oauth(self, r):
        """
        Method required by bearer token authentication.
        """

        r.headers["Authorization"] = f"Bearer {self.bearer_token}"
        r.headers["User-Agent"] = "v2UserTweetsPython"

        return r

    def connect_to_endpoint(self, url, params=None, channel_name=None):
        """ Method for creating a connection to Twitter API v2 endpoint.
        """
        response = requests.request("GET", url, params=params, auth=self.bearer_oauth)
        # self.logger.info(f"Status Code: {response.status_code}{' channel: '+channel_name if channel_name else ''}")
        
        if response.status_code == 429:
                self.logger.info(f"Waiting 300 seconds before re-calling the API endpoint due to error: {response.status_code} {response.text}")
                sleep(300)
                response = requests.request("GET", url, params=params, auth=self.bearer_oauth)
                # self.logger.info(f"Second Attempt Status Code: {response.status_code}{' channel: '+channel_name if channel_name else ''} Message: {response.text}")
            
        if response.status_code != 200:
            return {'error_code': response.status_code, 'error_message': response.text}
        
        return response.json()

    def get_channel_name(self, input_channel):
        sql_query = f"""
        SELECT id, organisation, organisation_id, twitter_channel AS channel_name
        FROM postgres.scraping.benchmarking_channel_input
        WHERE organisation = '{input_channel}'"""
        result = self.session.execute(text(sql_query))
        row = result.fetchone()

        return row

    def get_earlier_data(self, input_channel):
        sql_query = f"""
        SELECT channel_id, min(first_scraped_at) as first_scraped_at
        FROM postgres.scraping.twitter_channels
        LEFT JOIN postgres.scraping.benchmarking_channel_input ON twitter_channels.channel_input_id = benchmarking_channel_input.id
        WHERE organisation = '{input_channel}'
        GROUP BY channel_id"""
        result = self.session.execute(text(sql_query))
        rows = result.fetchall()
        earlier_data_channel = pd.DataFrame(rows, columns=result.keys())
        
        sql_query = f"""
        SELECT post_id, max(count) as count, min(first_scraped_at) as first_scraped_at
        FROM postgres.scraping.twitter_posts
        LEFT JOIN postgres.scraping.benchmarking_channel_input ON twitter_posts.channel_input_id = benchmarking_channel_input.id
        WHERE organisation = '{input_channel}'
        GROUP BY post_id"""
        result = self.session.execute(text(sql_query))
        rows = result.fetchall()
        earlier_data_post = pd.DataFrame(rows, columns=result.keys())
            
        return earlier_data_channel, earlier_data_post