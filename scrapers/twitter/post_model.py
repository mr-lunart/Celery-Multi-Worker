class PostModel():
    pandas_dict:dict[str, list] = {
        "post_id":[],
        "channel_name":[],
        "screen_name":[],
        "text":[],
        "comments_count":[],
        "retweet_count":[],
        "favorite_count":[],
        "quote_count":[],
        "bookmark_count":[],
        "view_count":[],
        "hashtags":[],
        "is_retweet":[],
        "user_mentions":[],
        "user_mentions_count":[],
        "media":[],
        "media_type":[],
        "post_url":[],
        "date_created":[],
        "channel_id":[],
        "count":[],
        "created_at":[],
        "first_scraped_at":[],
        "channel_input_id":[],
    }
    
    def add_item(self, data:dict):
        """create manual mapping""" 
        
        self.pandas_dict["post_id"].append(data.get("PostID", None))
        self.pandas_dict["channel_name"].append(data.get("ChannelName", None))
        self.pandas_dict["screen_name"].append(data.get("ScreenName", None))
        self.pandas_dict["text"].append(data.get("Text", None))
        self.pandas_dict["comments_count"].append(data.get("CommentCount", None))
        self.pandas_dict["retweet_count"].append(data.get("RetweetCount", None))
        self.pandas_dict["favorite_count"].append(data.get("FavoriteCount", None))
        self.pandas_dict["quote_count"].append(data.get("QuoteCount", None))
        self.pandas_dict["bookmark_count"].append(data.get("BookmarkCount", None))
        self.pandas_dict["view_count"].append(data.get("ViewCount", None))
        self.pandas_dict["hashtags"].append(data.get("Hashtags", None))
        self.pandas_dict["is_retweet"].append(data.get("IsRetweet", None))
        self.pandas_dict["user_mentions"].append(data.get("UserMentions", None))
        self.pandas_dict["user_mentions_count"].append(data.get("UserMentionsCount", None))
        self.pandas_dict["media"].append(data.get("Media", None))
        self.pandas_dict["media_type"].append(data.get("MediaType", None))
        self.pandas_dict["post_url"].append(data.get("PostURL", None))
        self.pandas_dict["date_created"].append(data.get("DateCreated", None))
        self.pandas_dict["channel_id"].append(data.get("ChannelID", None))
        self.pandas_dict["count"].append(data.get("Count", None))
        self.pandas_dict["created_at"].append(data.get("CreatedAt", None))
        self.pandas_dict["first_scraped_at"].append(data.get("FirstScrapedAt", None))
        self.pandas_dict["channel_input_id"].append(data.get("channel_input_id", None))

    def get_dataset(self):
        """get the pandas compatible dataset"""
        return self.pandas_dict