from time import sleep

from spiderutil.log import Log
from spiderutil.connector import MongoDB
from spiderutil.exceptions import SpiderException
from spiderutil.path import StoreByUserName

from twitterspider.twitter import TwitterSpider, TwitterDownloader
from twitterspider.util import TokenReader
from twitterspider.tweet import Tweet
from twitterspider.checkpoint import Checkpoint

if __name__ == '__main__':
    # First, get a developer api token from local file.
    token = TokenReader.from_local_file('./token')

    # If you need to use proxy, define it
    proxies = {
        'http': 'http://127.0.0.1:1080',
        'https': 'http://127.0.0.1:1080'
    }

    # Init a spider to get tweet objects
    spider = TwitterSpider(token, proxies=proxies)

    # Init a downloader to download tweet images and videos
    downloader = TwitterDownloader(StoreByUserName('./download'),
                                   proxies=proxies)

    # Init a logger if you want to print logs in the main function
    logger = Log.create_logger('TwitterSpider', './twitter.log')

    # Init the mongoDB to persist data,
    mongo = MongoDB('Twitter')
    # Check the connection and drop data in former session
    mongo.check_connection()
    mongo.drop()

    # Save failed tweets into another collection
    failed = MongoDB('Twitter-Failed')

    # Use local file to save checkpoint
    checkpoint = Checkpoint.load('./checkpoint.txt')
    since_id = checkpoint.tweet_id

    # Crawl the timeline and save to mongoDB
    # `screen_name` is the nickname of a user
    for tweet in spider.crawl_timeline(screen_name='twitter', since_id=since_id):
        # If you don't have mongoDB, you can use `downloader.download` download it directly
        mongo.insert(tweet.dict)

    # Download all the tweets
    for data in mongo.all():
        tweet = Tweet(data)
        logger.info(tweet.id)

        # Download the media
        try:
            downloader.download(tweet.source)
        except SpiderException:
            logger.error('Cannot download %s', tweet.id)
            # If failed to download the tweet, log it in another collection for future
            failed.insert(tweet.dict)
        finally:
            mongo.remove({'id': tweet.id})

        # Save the checkpoint
        checkpoint.update(tweet_id=tweet.id)
        checkpoint.save('./checkpoint.txt')

        # Since downloader has no delays, you need to add delay manually
        sleep(2)
