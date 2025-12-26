class ChannelModel():
    pandas_dict:dict[str, list] = {
        "channel_id":[],
        "channel_name":[],
        "description":[],
        "followers_count":[],
        "following_count":[],
        "listed_count":[],
        "tweet_count":[],
        "location":[],
        "website":[],
        "screen_name":[],
        "profile_image_url_https":[],
        "verified":[],
        "date_created":[],
        "created_at":[],
        "first_scraped_at":[],
        "channel_input_id":[],
        "profile_picture_s3_obj_url":[],
    }
    
    def add_item(self, data:dict):
        """create manual mapping""" 
        self.pandas_dict["channel_id"].append(data.get("ChannelID", None))
        self.pandas_dict["channel_name"].append(data.get("ChannelName", None))
        self.pandas_dict["description"].append(data.get("Description", None))
        self.pandas_dict["followers_count"].append(data.get("FollowersCount", None))
        self.pandas_dict["following_count"].append(data.get("FollowingCount", None))
        self.pandas_dict["listed_count"].append(data.get("ListedCount", None))
        self.pandas_dict["tweet_count"].append(data.get("TweetCount", None))
        self.pandas_dict["location"].append(data.get("Location", None))
        self.pandas_dict["website"].append(data.get("Website", None))
        self.pandas_dict["screen_name"].append(data.get("ScreenName", None))
        self.pandas_dict["profile_image_url_https"].append(data.get("ProfileImageURLHttps", None))
        self.pandas_dict["verified"].append(data.get("Verified", None))
        self.pandas_dict["date_created"].append(data.get("DateCreated", None))
        self.pandas_dict["created_at"].append(data.get("CreatedAt", None))
        self.pandas_dict["first_scraped_at"].append(data.get("FirstScrapedAt", None))
        self.pandas_dict["channel_input_id"].append(data.get("channel_input_id", None))
        self.pandas_dict["profile_picture_s3_obj_url"].append(data.get("profile_picture_s3_obj_url", None))
    
    def get_dataset(self):
        """get the pandas compatible dataset"""
        return self.pandas_dict