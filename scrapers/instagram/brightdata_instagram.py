import requests
import json
import time

from datetime import datetime
from scrapers.logger import generate_log_object
import boto3
import pandas as pd
import os

class InstagramScrapper:

    def __init__(self, bucket_name:str, api_key:str, param_input:dict) -> None:
        self.api_key = api_key
        self.request_input = {'input':[]}

        self.param_input = param_input
        self.platform_channel_name = "instagram"
        self.logger = generate_log_object(self.platform_channel_name)
        boto3_session = boto3.Session()
        s3_resource = boto3_session.resource('s3')
        self.bucket = s3_resource.Bucket(bucket_name)
        pass

    def start(self):
        data_input = self.param_input
        transformed_param = self.transform_input(param_input=data_input)
        _, result = self.sync_facebook_profile(transformed_param)
        if isinstance(result, dict):
            self.data_kind = "profile"
            self.event_loop(result=result,filename=None)
        else:
            return
        _, result = self.sync_facebook_post_by_url_profile(data_input)
        if isinstance(result, dict):
            self.data_kind = "post"
            self.event_loop(result=result,filename=None)
        else:
            return
        self.save_upload_s3(s3_key="instagram/test_scrapper/")

    def sync_instagram_post(self, input_data:list[dict]):
        self.request_input['input'] = input_data
        input_post = json.dumps(self.request_input)
        url =  "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_lk5ns7kz21pck8jpis&notify=false&include_errors=true&type=discover_new&discover_by=url"
        headers = {
        "Authorization": f"Bearer {self.api_key}",
        "Content-Type": "application/json",
        }
        response = requests.post(url=url, headers=headers, data=input_post)
        status = response.status_code
        if status == 200:
            full_dataset = []
            full_dataset = self.transform_to_pandas(response=response)
            self.posts_metric = full_dataset
            return status, full_dataset
        elif status == 202:
            data = response.json()
            snapshot_id = data.get("snapshot_id", None)
            if snapshot_id:
                return status, data
            else:
                print(f"Snapshot is not Ready, Wait 10 seconds")
                time.sleep(10)
                return self.sync_instagram_post(input_data)
        else:
            return status, None
        
    def sync_instagram_profile(self, input_data:list[dict]):
        self.request_input['input'] = input_data
        input_post = json.dumps(self.request_input)
        url =  "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_l1vikfch901nx3by4&notify=false&include_errors=true"
        headers = {
        "Authorization": f"Bearer {self.api_key}",
        "Content-Type": "application/json",
        }
        response = requests.post(url=url, headers=headers, data=input_post)
        status = response.status_code
        if status == 200:
            full_dataset = []
            full_dataset = self.transform_to_pandas(response=response)
            self.profile_metric = full_dataset
            return status, full_dataset
        elif status == 202:
            data = response.json()
            snapshot_id = data.get("snapshot_id", None)
            if snapshot_id:
                return status, data
            else:
                print(f"Snapshot is not Ready, Wait 10 seconds")
                time.sleep(10)
                return self.sync_instagram_profile(input_data)
        else:
            return status, None
        
    def write_json_file(self, status, data, filename):
        if status == 200:
            if data:
                with open(filename, 'w') as file:
                    json.dump(data, file, indent=4)
            else:
                print("found 0 row on dataset")
                json_string = json.dumps(data, indent=4)
                with open(filename, 'w') as file:
                    json.dump(data, file, indent=4)

        if status == 202:
            snapshot_id = data.get("snapshot_id", None)
            message = data.get("message", None)
            status = data.get("status", None)
            if snapshot_id:
                print(f"snapshot id :{snapshot_id}")
                print(message)

                json_string = json.dumps(data, indent=4)
                with open(filename, 'w') as file:
                    json.dump(data, file, indent=4)
            else:
                print(status)
                print(message)
        else:
            print(f"Error: {status}::{data}")

    def transform_input(self, param_input:list[dict]) -> list[dict]:
        list_param_input = []
        for input_item in param_input:
            new_param_input = {}
            new_param_input["url"] = input_item["url"]
            list_param_input.append(new_param_input)
        return list_param_input
    
    def event_loop(self, result, filename):
        print("start monitor snapshot...")
        snapshot_id = result.get("snapshot_id", None)
        if snapshot_id:
            while True:
                monitor_snapshot_id = self.monitor_api(snapshot_id=snapshot_id)
                if monitor_snapshot_id:
                    self.snapshot_downloader(snapshot_id=monitor_snapshot_id, filename=filename)
                    print("end monitor snapshot...")
                    break
                else:
                    print("reload monitor 15s")
                    time.sleep(15)
        else:
            raise Exception("Error snapshot id not found")

    def monitor_api(self, snapshot_id:str):
        url = f'https://api.brightdata.com/datasets/v3/progress/{snapshot_id}'
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = json.loads(response.content.decode('utf-8'))
            if data["status"] == "ready":
                return snapshot_id
            else:
                return ""
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")
            return ""
        
    def snapshot_downloader(self, snapshot_id:str, filename:str):
        url = f'https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}'
        headers = {
            'Authorization': f'Bearer {self.api_key}'
        }
        try:
            response = requests.get(url, headers=headers, stream=True)
            response.raise_for_status()
            full_dataset = self.transform_to_pandas(response=response)
            if self.data_kind == "profile":
                self.profile_metric = full_dataset
            elif self.data_kind == "post":
                self.posts_metric = full_dataset
            print("Download snapshot completed successfully")
            
        except requests.exceptions.RequestException as e:
            print(f"Error: {e}")

    def save_upload_s3(self, s3_key:str):
        channel_name = self.platform_channel_name
        timestamp_data = datetime.now().strftime("%Y%m%d%H%M%S")
        profile_name = f"instagram-profile-{channel_name}-{timestamp_data}.parquet"
        post_name = f"instagram-post-{channel_name}-{timestamp_data}.parquet"
       
        try:            
            self.profile_metric.to_parquet(profile_name, index=False)
            self.bucket.upload_file(profile_name, f"{s3_key}{profile_name}")

            self.posts_metric.to_parquet(post_name, index=False)
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
            self.logger.info(f'file instagram {channel_name} parquet is deleted')
        except Exception as err:
            self.logger.error(f"Error deleting data:{err}")
            return

    def transform_to_pandas(self, response):
        full_dataset = []
        for i, line in enumerate(response.iter_lines()):
            decoded_line = line.decode('utf-8')
            full_dataset.append(json.loads(decoded_line))
        df = pd.DataFrame(full_dataset)
        return df


# ig_scrapper = InstagramScrapper(api_key=api_key)

# football_club = [
#     {
#         "url":"https://www.instagram.com/realmadrid/",
#         "num_of_posts":5,
#         "start_date":"12-01-2025", # MM-DD-YYYY
#         "end_date":"12-31-2025", # MM-DD-YYYY
#         "post_type":"" # 'Post' / 'Reel'
#     },
#     {
#         "url":"https://www.instagram.com/fcbarcelona/",
#         "num_of_posts":5,
#         "start_date":"12-01-2025", # MM-DD-YYYY
#         "end_date":"12-31-2025", # MM-DD-YYYY
#         "post_type":"" # 'Post' / 'Reel'
#     }
# ]

# status, result = ig_scrapper.sync_instagram_post(football_club)
# ig_scrapper.write_json_file(status, result, "instagram_post.json")

# football_club = [
#     {
#         "url":"https://www.instagram.com/realmadrid/",
#     },
#     {
#         "url":"https://www.instagram.com/fcbarcelona/",
#     }
# ]

# status, result = ig_scrapper.sync_instagram_profile(football_club)
# ig_scrapper.write_json_file(status, result, "instagram_profile.json")