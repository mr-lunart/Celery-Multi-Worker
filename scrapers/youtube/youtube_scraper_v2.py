from pyyoutube import Api
from datetime import datetime, timedelta
from fuzzywuzzy import fuzz
import googleapiclient.discovery
import requests
import boto3
import pandas as pd
import warnings
import os

from sqlalchemy import text
from .channel_model import ChannelModel
from .post_model import PostModel
from scrapers.utils.get_engine import get_engine
from scrapers.utils.logger import generate_log_object

warnings.filterwarnings('ignore')


class Scrape_Youtube:
    
    BUCKET_NAME = "phokus-ingestion-raw-bucket"
    PROFILE_PICTURE_OBJ_URL = "https://social-external-tracking.s3.eu-west-1.amazonaws.com/youtube/profile_pictures/{}.png"
    PLATFORM = "youtube"
    YOUTUBE_ROOT_URL = "https://www.youtube.com/"
    YOUTUBE_VIDEO_URL = "https://www.youtube.com/watch?v={}&ab_channel={}"
    GOOGLE_API_URL = "https://www.googleapis.com/youtube/v3/search?key={}&channelId={}&part=snippet,id&order=date&maxResults={}&publishedAfter={}T00:00:00.000Z"
    WEEKDAYS = {}
    # WEEKDAYS = {
    #     0: {'from_id': 1, 'to_id': 13},  # Monday
    #     1: {'from_id': 14, 'to_id': 26},  # Tuesday
    #     2: {'from_id': 27, 'to_id': 39},  # Wednesday
    #     3: {'from_id': 40, 'to_id': 52},  # Thursday
    #     4: {'from_id': 53, 'to_id': 65},  # Friday
    #     5: {'from_id': 66, 'to_id': 78},  # Saturday
    #     6: {'from_id': 79, 'to_id': 92}  # Sunday
    # } # Commented out by Dmitri 2024-05-14

    def __init__(self, weekday_index, developer_key, conn_params, input_channel, start_date, end_date, post_limit):
        """Scraper uses Google's Youtube Data API V3
        """
        
        self.logger = generate_log_object(self.PLATFORM)
        
        self.today = datetime.today()
        # weekday_index = datetime.weekday(self.today) # Commented out by Dmitri 2024-05-14
        self.today_date_string = self.today.strftime("%d%m%Y")
        self.primary_keys = {}
        
        self.input_channel = input_channel
        self.start_date = start_date
        self.end_date = end_date
        self.post_limit = post_limit
        # self.organisation_id_start = self.WEEKDAYS[weekday_index]["from_id"] # Commented out by Dmitri 2024-05-14
        # self.organisation_id_end = self.WEEKDAYS[weekday_index]["to_id"] # Commented out by Dmitri 2024-05-14
        
        delta = timedelta(days=28)
        self.last_date = (self.today - delta)
        
        api_service_name = "youtube"
        api_version = "v3"
        
        self.weekday_index = weekday_index
        self.developer_key = developer_key

        # Instanciate Google API Client
        self.youtube_client = googleapiclient.discovery.build(api_service_name, api_version, developerKey=developer_key)
        # Instanciate Youtube API Client
        self.api = Api(api_key=developer_key)
        self.session = get_engine(conn_params)
        # Get S3 bucket
        boto3_session = boto3.Session()
        s3_resource = boto3_session.resource('s3')
        self.bucket = s3_resource.Bucket(self.BUCKET_NAME)
        
    def start(self):
        channel = self.get_channel_name(self.input_channel)
        self.platform_channel_name = channel["channel_name"]
        self.platform_channel_title = channel["youtube_channel_title"]
        self.channel_input_id = channel["id"]
        # earlier scrapped data
        earlier_data_channel, earlier_data_post = self.get_earlier_data(self.input_channel)
        # scrape channel and post
        scraping_status = self.scrape_channels_and_posts(earlier_data_channel, earlier_data_post)
        if scraping_status == None or scraping_status == False :
            self.logger.error('failed scraping channel and posts')
            return
        current_date_path = datetime.now().strftime("%Y/%m/%d")
        s3_key = f"youtube/{current_date_path}/"
        self.save_to_s3(s3_key=s3_key)
        self.logger.info('Scraping is finished')

    def save_to_s3(self, s3_key:str):
        channel_name = self.platform_channel_name
        timestamp_data = datetime.now().strftime("%Y%m%d%H%M%S")
        profile_name = f"youtube-profile-{channel_name}-{timestamp_data}.parquet"
        post_name = f"youtube-post-{channel_name}-{timestamp_data}.parquet"
       
        try:
            profile = pd.DataFrame(self.channel_profile_metric.get_dataset())
            profile.to_parquet(profile_name, index=False)
            self.bucket.upload_file(profile_name, f"{s3_key}{profile_name}")

            post = pd.DataFrame(self.channel_posts_metric.get_dataset())
            post.to_parquet(post_name, index=False)
            self.bucket.upload_file(post_name, f"{s3_key}{post_name}")
        except Exception as err:
            self.logger.error(f"Error uploading data to S3:{err}")
            raise err
        
        try:
            for filepath in (profile_name, post_name):
                if os.path.exists(filepath):
                    os.remove(filepath)
                else:
                    print(f'filepath {filepath} not found')
            self.logger.info(f'file youtube {channel_name} parquet is deleted')
        except Exception as err:
            self.logger.error(f"Error deleting data:{err}")
            raise err

    def scrape_channels_and_posts(self, earlier_data_channel, earlier_data_post):
        try:
            channel_name = self.platform_channel_name
            youtube_channel_title = self.platform_channel_title 
            self.logger.info(f"Scraping channels and videos from channel {channel_name}")
            response = self.youtube_client.search().list(
                    part="id,snippet",
                    type='video',
                    q=channel_name,
                    maxResults=50
            ).execute()

            # Get channel ID
            channel_id = None
            for item in response.get('items', []):
                if fuzz.ratio(item.get('snippet', {}).get('channelTitle', '').lower(), channel_name.lower()) >= 90:
                    channel_id = item.get('snippet', {}).get('channelId', '')
                    break
                    
            if channel_id is None:
                channel_title = youtube_channel_title
                for item in response.get('items', []):
                    if fuzz.ratio(item.get('snippet', {}).get('channelTitle', '').lower(), channel_title.lower()) >= 90:
                        channel_id = item.get('snippet', {}).get('channelId', '')
                        break
                        
            if channel_id is None:
                self.logger.info(f"No Channel ID found for {channel_name}")
                return
            
            # Get channel date
            channel_by_id = self.api.get_channel_info(channel_id=channel_id)
            channel_info = channel_by_id.items[0].to_dict()

            first_scraped_at = None
            channel_url = channel_info.get('snippet', {}).get('customUrl', None)
            if channel_id in earlier_data_channel["channel_id"].values:
                first_scraped_at = earlier_data_channel[earlier_data_channel["channel_id"] == channel_id]["first_scraped_at"].values[0]
                first_scraped_at = first_scraped_at.strftime("%Y-%m-%d")
                
            thumbnail_url = channel_info.get('snippet', {}).get('thumbnails', {}).get('high', {}).get('url', None)
            
            channel_data = {
                "ChannelID": channel_id,
                "ChannelName": channel_name,
                "Description": channel_info.get('snippet', {}).get('description', None),
                "ThumbnailUrl": thumbnail_url,
                "ChannelUrl": self.YOUTUBE_ROOT_URL + channel_url if channel_url else '',
                "ChannelTotalVideos": channel_info.get('statistics', {}).get('videoCount', None),
                "ChannelLocation": channel_info.get('snippet', {}).get('country', None),
                "ChannelJoinedDate": channel_info.get('snippet', {}).get('publishedAt', None).split('T')[0] if channel_info.get('snippet', {}).get('publishedAt', None) else '',
                "ChannelTotalViews": channel_info.get('statistics', {}).get('viewCount', None),
                "NumberOfSubscribers": channel_info.get('statistics', {}).get('subscriberCount', None),
                "CreatedAt": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                "FirstScrapedAt": first_scraped_at if first_scraped_at else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                "channel_input_id": self.channel_input_id,
                "profile_picture_s3_obj_url": self.PROFILE_PICTURE_OBJ_URL.format(channel_name),
            }

            channel_model = ChannelModel()
            channel_model.add_item(channel_data)
            self.channel_profile_metric = channel_model
            self.primary_keys[channel_id] = {'channel_input_id': self.channel_input_id}
            self.logger.info(f"Channel {channel_name} saved")


            # Get Video IDs  ################ this is the post scraper request
            api_url = self.GOOGLE_API_URL.format(self.developer_key, channel_id, self.post_limit, self.last_date.strftime("%Y-%m-%d"))
            video_items = requests.get(api_url).json().get('items', [])
            if not video_items:
                self.logger.info("No videos found")
                return

            # Get Video data ################ this is the post scraper process
            counter = 0
            for video_item in video_items:
                video_id = video_item.get('id', {}).get('videoId', None)
                if video_id is None:
                    self.logger.info("No Video ID found")
                    continue
                    
                video_info = self.api.get_video_by_id(video_id=video_id).items[0].to_dict()
                view_count = video_info.get('statistics', {}).get('viewCount', None)
                date_of_post = video_info.get('snippet', {}).get('publishedAt', None).replace('T', ' ').replace('Z', '')
                if view_count == 0 or view_count is None:
                    continue
                    
                if self.last_date.timestamp() > datetime.strptime(date_of_post, "%Y-%m-%d %H:%M:%S").timestamp():
                    continue
                    
                first_scraped_at, count = None, None

                if video_id in earlier_data_post["post_id"].values:
                    # Scraping posts for only 4 times (4 weeks)
                    count = int(earlier_data_post[earlier_data_post["post_id"] == video_id]["count"].values[0])
                    if count >= 4:
                        continue

                    first_scraped_at = earlier_data_post[earlier_data_post["post_id"] == video_id]["first_scraped_at"].values[0]
                    first_scraped_at = first_scraped_at.strftime("%Y-%m-%d")
                    
                duration = video_info.get('contentDetails', {}).get('duration', None)
                duration = ":".join([i.zfill(2) for i in duration.replace("H", ":").replace("M", ":").replace("S", "").replace("PT", "").split(':')])
                duration = duration if duration != "P0D" else None
                
                thumbnail = video_info.get('snippet', {}).get('thumbnails', {}).get('maxres', None)
                thumbnail = thumbnail.get('url', None) if thumbnail is not None else video_info.get('snippet', {}).get('thumbnails', {}).get('standard', {}).get('url', None)
                
                post_data = {
                    "PostID": video_id,
                    "ChannelName": channel_name,
                    "ChannelId": video_info.get('snippet', {}).get('channelId', None),
                    "Title": video_info.get('snippet', {}).get('title', None),
                    "Description": video_info.get('snippet', {}).get('description', None),
                    "PostUrl": self.YOUTUBE_VIDEO_URL.format(video_id, channel_name.replace(' ', '')),
                    "Thumbnail": thumbnail,
                    "ViewCount": view_count,
                    "DateOfPost": video_info.get('snippet', {}).get('publishedAt', None).replace('T', ' ').replace('Z', '') if item.get('snippet', {}).get('publishedAt', None) else '', 
                    "Likes": video_info.get('statistics', {}).get('likeCount', None),
                    "Dislikes": video_info.get('statistics', {}).get('dislikeCount', None),
                    "Duration": duration,
                    "Dimension": video_info.get('contentDetails', {}).get('dimension', None),
                    "Definition": video_info.get('contentDetails', {}).get('definition', None),
                    "CommentsCount": video_info.get('statistics', {}).get('commentCount', None),
                    "Tags": video_info.get('snippet', {}).get('tags', None),
                    "Count": count + 1 if count else 1,
                    "CreatedAt": datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                    "FirstScrapedAt": first_scraped_at if first_scraped_at else datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                    "channel_input_id": self.primary_keys[channel_id]['channel_input_id']
                }
                
                posts_model = PostModel()
                posts_model.add_item(post_data)
                self.channel_posts_metric = posts_model
                
                counter += 1
                
            self.logger.info(f"Saved {counter} video(s) for channel {channel_name}")
            return True
        except Exception as ex:
            self.logger.error(f"Exception in {self.platform_channel_name} parsing channel/posts data: {ex}")

    def get_earlier_data(self, input_channel:str):
        sql_query_channel = f"""
        SELECT channel_id, min(first_scraped_at) as first_scraped_at
        FROM postgres.scraping.youtube_channels
        LEFT JOIN postgres.scraping.benchmarking_channel_input ON youtube_channels.channel_input_id = benchmarking_channel_input.id
        WHERE organisation = '{input_channel}'
        GROUP BY channel_id"""

        sql_query_post = f"""
        SELECT post_id, max(count) as count, min(first_scraped_at) as first_scraped_at
        FROM postgres.scraping.youtube_posts
        LEFT JOIN postgres.scraping.benchmarking_channel_input ON youtube_posts.channel_input_id = benchmarking_channel_input.id
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
        SELECT id, organisation, organisation_id, scrape_media, youtube_channel AS channel_name, youtube_channel_title
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