from enum import Enum


class MediaType(Enum):
    video = 1
    photo = 2
    other = 3

    @staticmethod
    def convert(name: str):
        return MediaType(name.lower())
