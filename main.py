from time import sleep

from pytwitter.twitter import TwitterSpider, TwitterDownloader
from pytwitter.paths import StoreByUserName
from pytwitter.util import TokenReader
from pytwitter.log import Log

if __name__ == '__main__':
    # First, get a developer api token from local file.
    token = TokenReader.from_local_file('./token')
    # Then if you need to use proxy, define it
    proxies = {'https': 'http://127.0.0.1:1080'}
    # Init a spider to get tweet objects
    spider = TwitterSpider(token, proxies=proxies)
    # Init a downloader to download tweet images and videos
    downloader = TwitterDownloader(StoreByUserName('./download'), proxies=proxies)
    # Init a logger if you want to print logs
    logger = Log.create_logger()
    # `screen_name` is the nickname of a user
    for tweet in spider.crawl_timeline(screen_name='Twitter'):
        logger.info(tweet.id)
        # Download the media
        downloader.download(tweet.source)
        # Since downloader has no delays, you need to add delay manually
        sleep(5)
