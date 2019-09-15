class Checkpoint:

    def __init__(self, cursor=None, user_id=None, tweet_id=None):
        self.cursor = cursor
        self.user_id = user_id
        self.tweet_id = tweet_id

    @property
    def start(self):
        return self.user_id is None
