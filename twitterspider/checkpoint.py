import json, os

from spiderutil.structure import Dict


class Checkpoint(Dict):

    def __init__(self, cursor=None, user_id=None, tweet_id=None, structure=None):
        if structure is None:
            structure = {
                'cursor': cursor,
                'user_id': user_id,
                'tweet_id': tweet_id
            }
        super(Checkpoint, self).__init__(structure)

    @property
    def start(self):
        return self.user_id is None

    def save(self, path):
        path = os.path.abspath(path)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.dict, f, ensure_ascii=False, indent=2)

    @staticmethod
    def load(path):
        path = os.path.abspath(path)
        if os.path.isfile(path):
            with open(path, 'r', encoding='utf-8') as f:
                obj = json.load(f)
                return Checkpoint(structure=obj)
        else:
            return Checkpoint()
