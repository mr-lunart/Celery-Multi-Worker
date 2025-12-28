import requests
import json
import time

from datetime import datetime
from scrapers.utils.logger import generate_log_object
from scrapers.utils.get_engine import get_engine
from sqlalchemy import text
import boto3
import pandas as pd
import os


class FacebookScrapper:

    def __init__(
            self, 
            bucket_name:str, 
            api_key:str, 
            conn_params:dict,
            input_channel:str,
            start_date:str, 
            end_date:str,
            post_limit:int 
        ) -> None:
        self.api_key = api_key
       
        self.input_channel = input_channel
        self.start_date = start_date
        self.end_date = end_date
        self.post_limit = post_limit
        self.platform_channel_name = "facebook"
        self.log = generate_log_object(self.platform_channel_name, input_channel)
        self.session = get_engine(conn_params)
        boto3_session = boto3.Session()
        s3_resource = boto3_session.resource(
            service_name='s3',
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket = s3_resource.Bucket(bucket_name)
        pass

    def start(self):
        channel_credential = self.get_channel_name(self.input_channel)
        self.platform_channel_name = channel_credential["channel_name"]

        data_input = [{
            "url":f"https://www.facebook.com/{self.platform_channel_name}/",
            "num_of_posts":self.post_limit,
            "start_date":self.start_date, # MM-DD-YYYY
            "end_date":self.end_date # MM-DD-YYYY
        }]
        
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
        current_date_path = datetime.now().strftime("%Y/%m/%d")
        s3_key = f"facebook/{current_date_path}/"
        self.save_upload_s3(s3_key=s3_key)

    def get_channel_name(self, input_channel):
        sql_query = f"""
        SELECT id, organisation, organisation_id, scrape_media, facebook_channel AS channel_name
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
            self.log.error("Error snapshot id not found")
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
        except requests.exceptions.RequestException as err:
            self.log.error(err)
            raise err
        
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
            
        except requests.exceptions.RequestException as err:
            self.log.error(err)
            raise err

    def sync_facebook_post_by_url_profile(self, input_data:list[dict]):
        self.request_input['input'] = input_data
        input_post = json.dumps(self.request_input)
        url =  "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_lkaxegm826bjpoo9m5&notify=false&include_errors=true"
        headers = {
        "Authorization": f"Bearer {self.api_key}",
        "Content-Type": "application/json",
        }
        response = requests.post(url=url, headers=headers, data=input_post)
        status = response.status_code
        if status == 200:
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
                return self.sync_facebook_post_by_url_profile(input_data)
        else:
            return status, None
        
    def sync_facebook_profile(self, input_data:list[dict]):
        self.request_input['input'] = input_data
        input_post = json.dumps(self.request_input)
        url =  "https://api.brightdata.com/datasets/v3/scrape?dataset_id=gd_mf124a0511bauquyow&notify=false&include_errors=true"
        headers = {
        "Authorization": f"Bearer {self.api_key}",
        "Content-Type": "application/json",
        }
        response = requests.post(url=url, headers=headers, data=input_post)
        status = response.status_code
        if status == 200:
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
                return self.sync_facebook_profile(input_data)
        else:
            return status, None
    
    def save_upload_s3(self, s3_key:str):
        channel_name = self.platform_channel_name
        timestamp_data = datetime.now().strftime("%Y%m%d")
        profile_name = f"facebook-profile-{channel_name}-{timestamp_data}.parquet"
        post_name = f"facebook-post-{channel_name}-{timestamp_data}.parquet"
       
        try:            
            self.profile_metric.to_parquet(profile_name, index=False)
            self.bucket.upload_file(profile_name, f"{s3_key}{profile_name}")

            self.posts_metric.to_parquet(post_name, index=False)
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
            self.log.info(f'file facebook {channel_name} parquet is deleted')
        except Exception as err:
            self.log.error(f"Error deleting data:{err}")
            raise err

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

    def transform_to_pandas(self, response):
        full_dataset = []
        for i, line in enumerate(response.iter_lines()):
            decoded_line = line.decode('utf-8')
            full_dataset.append(json.loads(decoded_line))
        df = pd.DataFrame(full_dataset)
        return df

# if __name__ == '__main__':
#     fb_scrapper = FacebookScrapper(
#         bucket_name="social-external-tracking",
#         api_key=
#     )
#     football_club = [
#         {
#             "url":"https://www.facebook.com/fcbarcelona/",
#             "num_of_posts":5,
#             "start_date":"12-01-2025", # MM-DD-YYYY
#             "end_date":"12-31-2025", # MM-DD-YYYY
#         }
#     ]

#     fb_scrapper.run(football_club)

# status, result = fb_scrapper.sync_facebook_post_by_url_profile(football_club)
# fb_scrapper.write_json_file(status, result, "facebook_post.json")

# football_club = [
#     {
#         "url":"https://www.facebook.com/fcbarcelona/",
#     },
#     {
#         "url":"https://www.facebook.com/RealMadrid/",
#     }
# ]

# status, result = fb_scrapper.sync_facebook_profile(football_club)
# fb_scrapper.write_json_file(status, result, "real_madrid_facebook_profile.json")