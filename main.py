from time import sleep

from twitterspider.twitter import TwitterSpider, TwitterDownloader
from twitterspider.paths import StoreByUserName
from twitterspider.util import TokenReader
from twitterspider.log import Log
from twitterspider.connector import MongoDB
from twitterspider.tweet import Tweet
from twitterspider.exceptions import SpiderException

if __name__ == '__main__':
    # First, get a developer api token from local file.
    token = TokenReader.from_local_file('./token')
    # Then if you need to use proxy, define it
    proxies = {
        'http': 'http://127.0.0.1:1080',
        'https': 'http://127.0.0.1:1080'
    }
    # Init a spider to get tweet objects
    spider = TwitterSpider(token, proxies=proxies)
    # Init a downloader to download tweet images and videos
    downloader = TwitterDownloader(StoreByUserName('./download'),
                                   proxies=proxies)
    # Init a logger if you want to print logs in main function
    logger = Log.create_logger('TwitterSpider', './twitter.log')
    # Init the mongoDB to persist data
    mongo = MongoDB('Twitter')
    mongo.check_connection()
    mongo.drop()
    failed = MongoDB('Twitter-Failed')
    # Use mongoDB to save checkpoint
    since_id = None
    checkpoint = MongoDB('Checkpoint')
    # If there is no checkpoint you may need to manually add
    checkpoint_item = checkpoint.find({'name': 'Twitter'})
    if checkpoint_item is None:
        checkpoint.insert({
            'name': 'Twitter',
            'id': 0
        })
    else:
        since_id = checkpoint_item['id']

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
            failed.insert(tweet.dict)
        finally:
            mongo.remove({'id': tweet.id})

        # Save the checkpoint
        checkpoint.update({'name': 'Twitter'}, {'$set': {'id': tweet.id}})

        # Since downloader has no delays, you need to add delay manually
        sleep(2)
