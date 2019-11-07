import os
import sys

from spiderutil.typing import MediaType

if sys.version_info[0] > 2:
    import urllib.parse as urlparse
else:
    import urlparse

media_type = {
    'photo': MediaType.image,
    'video': MediaType.video,
    'animated_gif': MediaType.video
}


class Tweet:
    def __init__(self, tweet: dict):
        self.dict = tweet
        self.source = Tweet(tweet['retweeted_status']) if 'retweeted_status' in tweet else self
        self.id = tweet['id']
        self.user = User(tweet['user'])
        self.media = list(Media(medium) for medium in tweet['extended_entities']['media']) \
            if 'extended_entities' in tweet else []
        self.text = tweet['text']


class Media:
    def __init__(self, media: dict):
        self.type = media_type[media['type']]
        self.id = media['id']
        if self.type == MediaType.image:
            self.url = media['media_url']
        else:
            bitrate = 0
            self.url = ''
            for variant in media['video_info']['variants']:
                if 'bitrate' in variant and variant['bitrate'] > bitrate:
                    self.url = variant['url']
        self.file_name = os.path.basename(urlparse.urlparse(self.url).path)


class User:
    def __init__(self, user: dict):
        self.name = user['name']
        self.nickname = user['screen_name']
        self.id = user['id']
