class PostModel():
    pandas_dict:dict[str, list] = {
        "post_id":[],
        "channel_name":[],
        "title":[],
        "description":[],
        "post_url":[],
        "thumbnail_url":[],
        "view_count":[],
        "date_of_post":[],
        "likes":[],
        "dislikes":[],
        "duration":[],
        "dimension":[],
        "definition":[], 
        "comments_count":[], 
        "tags":[],
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
        self.pandas_dict["title"].append(data.get("Title", None))
        self.pandas_dict["description"].append(data.get("Description", None))
        self.pandas_dict["post_url"].append(data.get("PostUrl", None))
        self.pandas_dict["thumbnail_url"].append(data.get("Thumbnail", None))
        self.pandas_dict["view_count"].append(data.get("ViewCount", None))
        self.pandas_dict["date_of_post"].append(data.get("DateOfPost", None))
        self.pandas_dict["likes"].append(data.get("Likes", None))
        self.pandas_dict["dislikes"].append(data.get("Dislikes", None))
        self.pandas_dict["duration"].append(data.get("Duration", None))
        self.pandas_dict["dimension"].append(data.get("Dimension", None))
        self.pandas_dict["definition"].append(data.get("Definition", None))
        self.pandas_dict["comments_count"].append(data.get("CommentsCount", None))
        self.pandas_dict["tags"].append(data.get("Tags", None))
        self.pandas_dict["channel_id"].append(data.get("ChannelId", None))
        self.pandas_dict["count"].append(data.get("Count", None))
        self.pandas_dict["created_at"].append(data.get("CreatedAt", None))
        self.pandas_dict["first_scraped_at"].append(data.get("FirstScrapedAt", None))
        self.pandas_dict["channel_input_id"].append(data.get("channel_input_id", None))

    def get_dataset(self):
        """get the pandas compatible dataset"""
        return self.pandas_dict