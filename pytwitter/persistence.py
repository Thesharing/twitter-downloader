import os
from abc import abstractmethod
import redis
import pymongo


class Database:
    def __init__(self, name: str, db_type):
        self.name = name
        self.type = db_type

    @abstractmethod
    def count(self):
        pass


class Redis(Database):
    def __init__(self, name: str,
                 host='localhost',
                 port=6379):
        super(Redis, self).__init__(name, 'Redis')
        self.conn = redis.StrictRedis(host=host,
                                      port=port,
                                      decode_responses=True)

    @abstractmethod
    def count(self):
        pass


class RedisSet(Redis):

    def add(self, values):
        return self.conn.sadd(self.name, values)

    def count(self):
        return self.conn.scard(self.name)

    def empty(self):
        return self.conn.scard(self.name) <= 0

    def pop(self):
        return self.conn.spop(self.name)

    def remove(self, values):
        return self.conn.srem(self.name, values)

    def rand(self, number=None):
        if number:
            return self.conn.srandmember(self.name, number)
        else:
            return self.conn.srandmember(self.name)

    def is_member(self, value):
        return self.conn.sismember(self.name, value)

    def all(self):
        return self.conn.smembers(self.name)

    def flush_all(self):
        return self.conn.delete(self.name)


class RedisHash(Redis):

    def add(self, key):
        return self.conn.hsetnx(self.name, key, 0)

    def count(self):
        return self.conn.hlen(self.name)

    def empty(self):
        return self.conn.hlen(self.name) <= 0

    def remove(self, keys):
        return self.conn.hdel(self.name, keys)

    def exists(self, key):
        return self.conn.hexists(self.name, key)

    def all(self):
        return self.conn.hgetall(self.name)

    def get(self, keys):
        """
        :param keys: a single key or a list of keys
        :return: a string, or a list of string correspondingly
        """
        if type(keys) is list:
            return self.conn.hmget(self.name, keys)
        else:
            return self.conn.hget(self.name, keys)

    def set(self, mapping: dict):
        if len(mapping) > 1:
            return self.conn.hmset(self.name, mapping)
        elif len(mapping) == 1:
            (key, value), = mapping.items()
            return self.conn.hset(self.name, key, value)

    def increment(self, key, value: int = 1):
        return self.conn.hincrby(self.name, key, value)


class MongoDB(Database):

    def __init__(self, collection: str,
                 host='localhost',
                 port=27017,
                 db='spider'):
        super(MongoDB, self).__init__(collection, 'MongoDB')
        client = pymongo.MongoClient(host=host, port=port)
        database = client[db]
        self.conn = database[collection]

    def insert(self, documents):
        if type(documents) is list:
            return self.conn.insert_many(documents)
        else:
            return self.conn.insert_one(documents)

    def remove(self, filter, all=False):
        if all:
            return self.conn.delete_many(filter=filter)
        else:
            return self.conn.delete_one(filter=filter)

    def update(self, filter, update, all=False):
        if all:
            return self.conn.update_many(filter=filter, update=update)
        else:
            return self.conn.update_one(filter=filter, update=update)

    def replace(self, filter, replacement):
        return self.conn.replace_one(filter=filter, replacement=replacement)

    def find(self, filter, all=False, **kwargs):
        if all:
            return self.conn.find(filter=filter, **kwargs)
        else:
            return self.conn.find_one(filter=filter, **kwargs)

    def all(self):
        return self.conn.find()

    def count(self, filter=None, **kwargs):
        if filter is None:
            return self.conn.count_documents(filter={}, **kwargs)
        else:
            return self.conn.count_documents(filter=filter, **kwargs)

    def create_index(self, index):
        """
        :param index: [('key', pymongo.HASHED)]
        :return: Index Name
        """
        return self.conn.create_index(index)


class LocalFile(Database):
    def __init__(self, file_path: str, file_type=None):
        super(LocalFile, self).__init__(os.path.basename(file_path), 'LocalFile')
        self.file_path = file_path
        self.file_type = file_type

    def count(self):
        if os.path.isdir(self.file_path):
            if self.file_type:
                return len([name for name in os.listdir(self.file_path) if
                            os.path.isfile(os.path.join(self.file_path, name)) and os.path.splitext(
                                name) == self.file_type])
            else:
                return len([name for name in os.listdir(self.file_path)
                            if os.path.isfile(os.path.join(self.file_path, name))])
        else:
            return 0


def test_redis(host='localhost',
               port=6379):
    conn = redis.StrictRedis(host=host, port=port,
                             decode_responses=True)
    conn.client_list()


def test_mongodb(host='localhost',
                 port=27017):
    client = pymongo.MongoClient(host=host, port=port,
                                 serverSelectionTimeoutMS=3000, connectTimeoutMS=3000)
    client.admin.command('ismaster')
