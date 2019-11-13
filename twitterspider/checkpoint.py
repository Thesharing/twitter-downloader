import json
import os

from spiderutil.structure import Dict


class Checkpoint(Dict):

    def __init__(self, source=None, cursor=None, user_id=None, tweet_id=None):
        super(Checkpoint, self).__init__(source=source, cursor=cursor, user_id=user_id, tweet_id=tweet_id)

    @property
    def start(self):
        return self.user_id is None

    def save(self, path):
        path = os.path.abspath(path)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self, f, ensure_ascii=False, indent=2)

    def update(self, cursor=None, user_id=None, tweet_id=None):
        if self.cursor is None or (cursor is not None and cursor > self.cursor):
            self.cursor = cursor
        if self.user_id is None or (user_id is not None and user_id > self.user_id):
            self.user_id = user_id
        if self.tweet_id is None or (tweet_id is not None and tweet_id > self.tweet_id):
            self.tweet_id = tweet_id

    @staticmethod
    def load_file(path):
        path = os.path.abspath(path)
        if os.path.isfile(path):
            with open(path, 'r', encoding='utf-8') as f:
                return Checkpoint(json.load(f))
        else:
            return Checkpoint()
