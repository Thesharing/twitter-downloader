import os
import sys

from spiderutil.typing import MediaType

if sys.version_info[0] > 2:
    import urllib.parse as urlparse
else:
    import urlparse


class Tweet:
    def __init__(self, tweet: dict):
        self.dict = tweet
        self.source = Tweet(tweet['retweeted_status']) if 'retweeted_status' in tweet else self
        self.id = tweet['id']
        self.user = User(tweet['user'])
        self.medias = list(Media(media) for media in tweet['extended_entities']['media']) \
            if 'extended_entities' in tweet else []
        self.text = tweet['text']


class Media:
    def __init__(self, media: dict):
        self.type = MediaType[media['type']]
        self.id = media['id']
        if self.type == MediaType.video:
            bitrate = 0
            self.url = ''
            for variant in media['video_info']['variants']:
                if 'bitrate' in variant and variant['bitrate'] > bitrate:
                    self.url = variant['url']
        else:
            self.url = media['media_url']
        self.file_name = os.path.basename(urlparse.urlparse(self.url).path)


class User:
    def __init__(self, user: dict):
        self.name = user['name']
        self.nickname = user['screen_name']
        self.id = user['id']
