class ChannelModel():
    pandas_dict:dict[str, list] = {
        "channel_id":[],
        "channel_name":[],
        "channel_url":[],
        "nickname":[],
        "avatar_larger":[],
        "avatar_medium":[],
        "avatar_small":[],
        "avatar_thumb":[],
        "signature":[],
        "sec_uid":[],
        "bio_link":[],
        "room_id":[],
        "follower_count":[],
        "following_count":[],
        "heart_count":[],
        "video_count":[],
        "created_at":[],
        "first_scraped_at":[],
        "date":[],
        "channel_input_id":[],
        "profile_picture_s3_obj_url":[],
    }

    def add_item(self, data:dict):
        """create manual mapping"""
        self.pandas_dict["channel_id"].append(data.get("channelId", None))
        self.pandas_dict["channel_name"].append(data.get("channelName", None))
        self.pandas_dict["channel_url"].append(data.get("channelUrl", None))
        self.pandas_dict["nickname"].append(data.get("nickname", None))
        self.pandas_dict["avatar_larger"].append(data.get("avatarLarger", None))
        self.pandas_dict["avatar_medium"].append(data.get("avatarMedium", None))
        self.pandas_dict["avatar_small"].append(data.get("avatarSmall", None))
        self.pandas_dict["avatar_thumb"].append(data.get("avatarThumb", None))
        self.pandas_dict["signature"].append(data.get("signature", None))
        self.pandas_dict["sec_uid"].append(data.get("secUid", None))
        self.pandas_dict["bio_link"].append(data.get("bioLink", None))
        self.pandas_dict["room_id"].append(data.get("roomId", None))
        self.pandas_dict["follower_count"].append(data.get("followerCount", None))
        self.pandas_dict["following_count"].append(data.get("followingCount", None))
        self.pandas_dict["heart_count"].append(data.get("heartCount", None)  )
        self.pandas_dict["video_count"].append(data.get("videoCount", None))
        self.pandas_dict["created_at"].append(data.get("createdAt", None))
        self.pandas_dict["first_scraped_at"].append(data.get("firstScrapedAt", None))
        self.pandas_dict["date"].append(data.get("date", None))
        self.pandas_dict["channel_input_id"].append(data.get("channel_input_id", None))
        self.pandas_dict["profile_picture_s3_obj_url"].append(data.get("profile_picture_s3_obj_url", None))
    
    def get_dataset(self):
        """get the pandas compatible dataset"""
        return self.pandas_dict