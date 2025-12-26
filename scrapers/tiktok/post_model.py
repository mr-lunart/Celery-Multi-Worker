class PostModel():
    pandas_dict:dict[str, list] = {
        "create_time":[],
        "post_id":[],
        "description":[],
        "height":[],
        "width":[],
        "duration":[],
        "ratio":[],
        "cover":[],
        "orgin_cover":[],
        "dynamic_cover":[],
        "download_addr":[],
        "play_addr":[],
        "reflow_cover":[],
        "format":[],
        "video_quality":[],
        "codec_type":[],
        "definition":[],
        "music_id":[],
        "music_title":[],
        "music_duration":[],
        "music_album":[],
        "challenges":[],
        "digg_count":[],
        "share_count":[],
        "comment_count":[],
        "play_count":[],
        "text_extra":[],
        "for_friend":[],
        "digged":[],
        "item_comment_status":[],
        "show_not_pass":[],
        "vl1":[],
        "item_mute":[],
        "private_item":[],
        "duet_enabled":[],
        "stitch_enabled":[],
        "share_enabled":[],
        "stickers_on_item":[],
        "is_ad":[],
        "duet_display":[],
        "stitch_display":[],
        "channel_name":[],
        "channel_id":[],
        "created_at":[],
        "first_scraped_at":[],
        "count":[],
        "musicplayurl":[],
        "channel_input_id":[],
    }
    
    def add_item(self, data:dict):
        """create manual mapping"""    
        self.pandas_dict["create_time"].append(data.get("createTime", None))
        self.pandas_dict["post_id"].append(data.get("postId", None))
        self.pandas_dict["description"].append(data.get("desc", None))
        self.pandas_dict["height"].append(data.get("height", None))
        self.pandas_dict["width"].append(data.get("width", None))
        self.pandas_dict["duration"].append(data.get("duration", None))
        self.pandas_dict["ratio"].append(data.get("ratio", None))
        self.pandas_dict["cover"].append(data.get("cover", None))
        self.pandas_dict["orgin_cover"] .append(data.get("originCover", None))
        self.pandas_dict["dynamic_cover"].append(data.get("dynamicCover", None))
        self.pandas_dict["download_addr"].append(data.get("downloadAddr", None))
        self.pandas_dict["play_addr"].append(data.get("playAddr", None))
        self.pandas_dict["reflow_cover"].append(data.get("reflowCover", None))
        self.pandas_dict["format"].append(data.get("format", None))
        self.pandas_dict["video_quality"].append(data.get("videoQuality", None))
        self.pandas_dict["codec_type"].append(data.get("codecType", None))
        self.pandas_dict["definition"].append(data.get("definition", None))
        self.pandas_dict["music_id"].append(data.get("musicId", None))
        self.pandas_dict["music_title"].append(data.get("musicTitle", None))
        self.pandas_dict["music_duration"].append(data.get("musicDuration", None))
        self.pandas_dict["music_album"].append(data.get("musicAlbum", None))
        self.pandas_dict["challenges"].append(data.get("challenges", None))
        self.pandas_dict["digg_count"].append(data.get("diggCount", None))
        self.pandas_dict["share_count"].append(data.get("shareCount", None))
        self.pandas_dict["comment_count"].append(data.get("commentCount", None))
        self.pandas_dict["play_count"].append(data.get("playCount", None))
        self.pandas_dict["text_extra"].append(data.get("textExtra", None))     
        self.pandas_dict["for_friend"].append(data.get("forFriend", None))
        self.pandas_dict["digged"].append(data.get("digged", None))
        self.pandas_dict["item_comment_status"].append(data.get("itemCommentStatus", None))
        self.pandas_dict["show_not_pass"].append(data.get("showNotPass", None))
        self.pandas_dict["vl1"].append(data.get("vl1", None))
        self.pandas_dict["item_mute"].append(data.get("itemMute", None))
        self.pandas_dict["private_item"].append(data.get("privateItem", None))
        self.pandas_dict["duet_enabled"].append(data.get("duetEnabled", None))
        self.pandas_dict["stitch_enabled"].append(data.get("stitchEnabled", None))
        self.pandas_dict["share_enabled"].append(data.get("shareEnabled", None))
        self.pandas_dict["stickers_on_item"].append(data.get("stickersOnItem", None))
        self.pandas_dict["is_ad"].append(data.get("isAd", None))
        self.pandas_dict["duet_display"].append(data.get("duetDisplay", None))
        self.pandas_dict["stitch_display"].append(data.get("stitchDisplay", None))
        self.pandas_dict["channel_name"].append(data.get("channelName", None))
        self.pandas_dict["channel_id"].append(data.get("channel_id", None))
        self.pandas_dict["created_at"].append(data.get("createdAt", None))
        self.pandas_dict["first_scraped_at"].append(data.get("firstScrapedAt", None))
        self.pandas_dict["count"].append(data.get("count", None))
        self.pandas_dict["musicplayurl"].append(data.get("musicPlayUrl", None))
        self.pandas_dict["channel_input_id"].append(data.get("channel_input_id", None))
    
    def get_dataset(self):
        """get the pandas compatible dataset"""
        return self.pandas_dict