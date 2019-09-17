import re
import os
from abc import abstractmethod

from spiderutil.typing import MediaType


class PathGenerator:

    def __init__(self, folder_path):
        folder_path = os.path.abspath(folder_path)
        self.check(folder_path)
        self.folder_path = folder_path

    @abstractmethod
    def path(self, **kwargs):
        pass

    @staticmethod
    def check(path):
        if not os.path.isdir(path):
            os.makedirs(path)

    @staticmethod
    def ext(media_type):
        return 'mp4' if media_type == MediaType.video else 'jpg'


class StoreSimply(PathGenerator):

    def path(self, media_type, file_name, **kwargs):
        return os.path.join(self.folder_path, '{0}.{1}'.format(file_name, self.ext(media_type)))


class StoreByUserName(PathGenerator):

    def __init__(self, folder_path):
        PathGenerator.__init__(self, folder_path)
        self.users = {}
        self.init_all_users()

    def path(self, media_type, user_name, **kwargs):
        if user_name not in self.users:
            # If not appeared before
            self.users[user_name] = 1
        count = self.users[user_name]
        # The count is always the index of next media file
        self.users[user_name] += 1
        return os.path.join(self.folder_path, '{0}-{1}.{2}'.format(user_name, count, self.ext(media_type)))

    def init_all_users(self):
        # Traverse all the files, the file name should be '[Username]-[Index].[Ext]]'
        for file_name in os.listdir(self.folder_path):
            match = re.search(r'(.+)-(\d+)(?!.+)', os.path.splitext(file_name)[0])
            if match is not None:
                user_name = match.group(1)
                count = int(match.group(2))
                self.users[user_name] = max(self.users[user_name], count + 1) if user_name in self.users else count + 1


class StoreByUserNamePerFolder(PathGenerator):

    def __init__(self, folder_path):
        PathGenerator.__init__(self, folder_path)
        self.users = {}

    def path(self, media_type, user_name, **kwargs):
        # Check if the folder named by username exists
        self.check(os.path.join(self.folder_path, user_name))
        if user_name not in self.users:
            self.init_user(user_name)
        count = self.users[user_name]
        self.users[user_name] += 1
        return os.path.join(self.folder_path,
                            os.path.join(user_name, '{1}.{2}'.format(user_name, count, self.ext(media_type))))

    def init_user(self, user_name):
        # Traverse the sub folder of specific user, the file name should be '[Username]/[Index].[Ext]'
        path = os.path.join(self.folder_path, user_name)
        count = 0
        for file_name in os.listdir(path):
            if os.path.isfile(os.path.join(path, file_name)):
                match = re.search(r'\d+', os.path.splitext(file_name)[0])
                if match is not None:
                    self.users[user_name] = max(count, int(match.group(0)))
        self.users[user_name] = count + 1
