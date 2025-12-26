class ChannelModel():
    pandas_dict:dict[str, list] = {
        "channel_id":[],
        "channel_name":[],
        "thumbnail_url":[],
        "description":[],
        "channel_url":[],
        "channel_total_videos":[],
        "channel_location":[],
        "channel_joined_date":[],
        "channel_total_views":[],
        "number_of_subscribers":[],
        "created_at":[],
        "first_scraped_at":[],
        "channel_input_id":[],
        "profile_picture_s3_obj_url":[],
    }
    
    def add_item(self, data:dict):
        """create manual mapping"""  
        self.pandas_dict["channel_id"].append(data.get("ChannelID", None))
        self.pandas_dict["channel_name"].append(data.get("ChannelName", None))
        self.pandas_dict["thumbnail_url"].append(data.get("ThumbnailUrl", None))
        self.pandas_dict["description"].append(data.get("Description", None))
        self.pandas_dict["channel_url"].append(data.get("ChannelUrl", None))
        self.pandas_dict["channel_total_videos"].append(data.get("ChannelTotalVideos", None))
        self.pandas_dict["channel_location"].append(data.get("ChannelLocation", None))
        self.pandas_dict["channel_joined_date"].append(data.get("ChannelJoinedDate", None))
        self.pandas_dict["channel_total_views"].append(data.get("ChannelTotalViews", None))
        self.pandas_dict["number_of_subscribers"].append(data.get("NumberOfSubscribers", None))
        self.pandas_dict["created_at"].append(data.get("CreatedAt", None))
        self.pandas_dict["first_scraped_at"].append(data.get("FirstScrapedAt", None))
        self.pandas_dict["channel_input_id"].append(data.get("channel_input_id", None))
        self.pandas_dict["profile_picture_s3_obj_url"].append(data.get("profile_picture_s3_obj_url", None))
    
    def get_dataset(self):
        """get the pandas compatible dataset"""
        return self.pandas_dict