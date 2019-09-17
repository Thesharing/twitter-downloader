from time import sleep

from spiderutil.log import Log
from spiderutil.connector import MongoDB
from spiderutil.exceptions import SpiderException

from twitterspider.twitter import TwitterSpider, TwitterDownloader
from twitterspider.paths import StoreByUserName
from twitterspider.util import TokenReader
from twitterspider.tweet import Tweet

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
    # (Loggers are automatically enabled in spider and download,
    # however, you can replace it with customized one)
    logger = Log.create_logger('TwitterSpider', './twitter.log')

    # Init the mongoDB to persist data,
    # check the connection and drop data in former session
    mongo = MongoDB('Twitter')
    mongo.check_connection()
    mongo.drop()

    # Save failed tweets into another collection
    failed = MongoDB('Twitter-Failed')

    # Use mongoDB to save checkpoint
    since_id = None
    checkpoint = MongoDB('Checkpoint')
    item = checkpoint.find({'name': 'Twitter'})
    if item is None:
        # If there is no checkpoint you need to manually add one
        checkpoint.insert({
            'name': 'Twitter',
            'id': 0
        })
    else:
        since_id = item['id']

    # Crawl the timeline and save to mongoDB
    # If you don't have mongoDB, you can download it directly
    # `screen_name` is the nickname of a user
    for tweet in spider.crawl_timeline(screen_name='zhlongh', since_id=since_id):
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
        checkpoint.update({'name': 'Twitter'}, {'$set': {'id': tweet.id}})

        # Since downloader has no delays, you need to add delay manually
        sleep(2)
