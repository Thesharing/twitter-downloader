import json
import os
import requests
import sys
from time import sleep
from typing import Iterable

from .checkpoint import Checkpoint
from .exceptions import NetworkException, RetryLimitExceededException, UnauthorizedException
from .log import Log
from .paths import PathGenerator
from .tweet import Tweet

if sys.version_info[0] > 2:
    import urllib.parse as urlparse
else:
    import urlparse

DELAY = 5
RETRY = 5


class TwitterSpider:

    def __init__(self, token: str, proxies: dict = None, logger=None, delay=DELAY, retry=RETRY):
        self.base_url = 'https://api.twitter.com/1.1/'
        self.proxies = proxies
        self.headers = {'Authorization': token}
        self.logger = logger if logger is not None else Log.create_logger('TwitterSpider', './twitter.log')
        self.delay = delay
        self.retry = retry

    def crawl_timeline(self, screen_name: str = None, user_id: str = None,
                       include_retweets: bool = True, exclude_replies: bool = True,
                       start_id=None, since_id=None, delay: float = None) -> Iterable[Tweet]:
        """

        :param screen_name:
        :param user_id:
        :param include_retweets:
        :param exclude_replies:
        :param start_id:
        :param since_id:
        :param delay:
        :return:
        """
        if delay is None:
            delay = self.delay

        self.logger.info('Crawling timeline: %s', locals())

        tweets = self.timeline(screen_name=screen_name, user_id=user_id, include_rts=include_retweets,
                               exclude_replies=exclude_replies, max_id=start_id, since_id=since_id)
        if len(tweets) <= 0:
            return
        tweet_id = start_id
        for tweet in tweets:
            tweet_id = tweet['id']
            yield Tweet(tweet)

        while len(tweets) > 0:
            sleep(delay)
            tweets = self.timeline(screen_name=screen_name, user_id=user_id, include_rts=include_retweets,
                                   exclude_replies=exclude_replies, max_id=tweet_id - 1, since_id=since_id)
            for tweet in tweets:
                tweet_id = tweet['id']
                yield Tweet(tweet)

    def crawl_likes(self, screen_name: str = None, user_id: str = None,
                    start_id=None, since_id=None, delay: float = None) -> Iterable[Tweet]:
        if delay is None:
            delay = self.delay

        self.logger.info('Crawling likes: %s', locals())

        tweets = self.likes(screen_name=screen_name, user_id=user_id, max_id=start_id, since_id=since_id)
        if len(tweets) <= 0:
            return
        tweet_id = start_id
        for tweet in tweets:
            tweet_id = tweet['id']
            yield Tweet(tweet)

        while len(tweets) > 0:
            sleep(delay)
            tweets = self.likes(screen_name=screen_name, user_id=user_id, max_id=tweet_id - 1, since_id=since_id)
            for tweet in tweets:
                tweet_id = tweet['id']
                yield Tweet(tweet)

    def crawl_following(self, screen_name: str = None, user_id: str = None,
                        include_retweets: bool = True, exclude_replies: bool = True,
                        checkpoint: Checkpoint = None, delay: float = None) -> Iterable[Tweet]:
        if delay is None:
            delay = self.delay
        cursor = checkpoint.cursor
        start = checkpoint is None or checkpoint.start

        self.logger.info('Crawling following: %s', locals())

        users = self.following(screen_name=screen_name, user_id=user_id, cursor=cursor)

        for user in users['users']:
            if not start:
                if checkpoint.user_id is None or user['id'] == checkpoint.user_id:
                    start = True
                    sleep(delay)
                    for tweet in self.crawl_timeline(user_id=user['id'], include_retweets=include_retweets,
                                                     exclude_replies=exclude_replies, start_id=checkpoint.tweet_id,
                                                     delay=delay):
                        yield tweet
                else:
                    continue
            else:
                sleep(delay)
                for tweet in self.crawl_timeline(user_id=user['id'], include_retweets=include_retweets,
                                                 exclude_replies=exclude_replies, delay=delay):
                    yield tweet
        cursor = users['next_cursor']

        while len(users['users']) > 0:
            sleep(delay)
            users = self.following(screen_name=screen_name, user_id=user_id, cursor=cursor)
            for user in users['users']:
                sleep(delay)
                for tweet in self.crawl_timeline(user_id=user['id'], include_retweets=include_retweets,
                                                 exclude_replies=exclude_replies, delay=delay):
                    yield tweet
            cursor = users['next_cursor']

    def _get(self, url, params):
        """
        Access API with requests and return the result with the format of json.
        """
        retry = self.retry
        while retry > 0:
            try:
                r = requests.get(url=url, params=params, headers=self.headers, proxies=self.proxies)
                if r.status_code == 200:
                    return json.loads(r.text)
                elif r.status_code == 401:
                    raise UnauthorizedException()
                else:
                    raise NetworkException('Met error code {} when visiting {}.'.format(r.status_code, r.url))
            except requests.exceptions.RequestException as e:
                self.logger.error(e)
                retry -= 1
        raise RetryLimitExceededException('Max retry limit exceeded when visiting {}.'.format(url))

    def _url(self, url):
        return urlparse.urljoin(self.base_url, url)

    def timeline(self, user_id: str = None, screen_name: str = None, count: int = 200,
                 exclude_replies: bool = None, include_rts: bool = None, trim_user: bool = None,
                 since_id=None, max_id=None):
        """
        Returns a collection of the most recent Tweets posted by the user indicated
        by the screen_name or user_id parameters.

        User timelines belonging to protected users may only be requested when the
        authenticated user either "owns" the timeline or is an approved follower of the owner.

        The timeline returned is the equivalent of the one seen as a user's profile on Twitter.

        This method can only return up to 3,200 of a user's most recent Tweets. Native retweets
        of other statuses by the user is included in this total, regardless of whether
        include_rts is set to false when requesting this resource.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 900
        Requests / 15-min window (app auth): 1500
        Requests / 24-hour window: 100,000

        Check https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-show
        for more information.

        :param user_id: The ID of the user for whom to return results.
        :param screen_name: The screen name of the user for whom to return results.
        :param since_id: Returns results with an ID greater than (that is, more recent than) the specified ID.
                    There are limits to the number of Tweets that can be accessed through the API.
                    If the limit of Tweets has occured since the since_id, the since_id will be forced
                    to the oldest ID available.
        :param count: Specifies the number of Tweets to try and retrieve, up to a maximum of 200
                per distinct request. The value of count is best thought of as a limit to the number
                of Tweets to return because suspended or deleted content is removed after the count
                has been applied. We include retweets in the count, even if include_rts is not supplied.
                It is recommended you always send include_rts=1 when using this API method.
        :param max_id: Returns results with an ID less than (that is, older than) or equal to the specified ID.
        :param trim_user: When set to either true , t or 1 , each Tweet returned in a timeline will
                        include a user object including only the status authors numerical ID.
                        Omit this parameter to receive the complete user object.
        :param exclude_replies: This parameter will prevent replies from appearing in the returned timeline.
                                Using exclude_replies with the count parameter will mean you will receive up-to
                                count tweets — this is because the count parameter retrieves that many Tweets before
                                filtering out retweets and replies.
        :param include_rts: When set to false , the timeline will strip any native retweets
                            (though they will still count toward both the maximal length of the timeline
                            and the slice selected by the count parameter).
                            Note: If you're using the trim_user parameter in conjunction with include_rts,
                            the retweets will still contain a full user object.
        :return: List of tweets= objects.
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get timeline: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required.')
        return self._get(self._url('statuses/user_timeline.json'), params)

    def user(self, user_id: str = None, screen_name: str = None, include_entitles: bool = None):
        """
        Returns a variety of information about the user specified by the required user_id
        or screen_name parameter. The author's most recent Tweet will be returned inline when possible.

        You must be following a protected user to be able to see their most recent Tweet.
        If you don't follow a protected user, the user's Tweet will be removed.
        A Tweet will not always be returned in the current_status field.

        Check https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-users-show
        for more information.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 900
        Requests / 15-min window (app auth): 900

        :param user_id: The ID of the user for whom to return results.
                        Either an id or screen_name is required for this method.
        :param screen_name: The screen name of the user for whom to return results.
                            Either a id or screen_name is required for this method.
        :param include_entitles: The entities node will not be included when set to false.
        :return: User-object, see https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object .
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get user: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required')
        return self._get(self._url('user/show.json'), params)

    def followers(self, user_id: str = None, screen_name: str = None, cursor=None,
                  count: int = 200, skip_status: bool = None, include_user_entitles: bool = None):
        """
        Returns a cursored collection of user objects for users following the specified user.

        At this time, results are ordered with the most recent following first — however, this ordering
        is subject to unannounced change and eventual consistency issues. Results are given in groups
        of 20 users and multiple "pages" of results can be navigated through using the next_cursor value
        in subsequent requests. See Using cursors to navigate collections for more information.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 15
        Requests / 15-min window (app auth): 15

        Check https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-followers-list
        for more information.

        :param user_id: The ID of the user for whom to return results.
        :param screen_name: The screen name of the user for whom to return results.
        :param cursor: Causes the results to be broken into pages. If no cursor is provided,
                        a value of -1 will be assumed, which is the first "page."
                        The response from the API will include a previous_cursor and next_cursor to allow
                        paging back and forth. See Using cursors to navigate collections for more information.
        :param count: The number of users to return per page, up to a maximum of 200.
        :param skip_status: When set to either true, t or 1 statuses will not be included in the returned user objects.
        :param include_user_entitles: The user object entities node will not be included when set to false.
        :return: {
                    "users": [
                          {user-object},
                          {user-object},
                          {user-object}
                    ],
                    "previous_cursor": 0,
                    "previous_cursor_str": "0",
                    "next_cursor": 1333504313713126852,
                    "next_cursor_str": "1333504313713126852"
                }
                For user-object, see https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object .
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get followers: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required')
        return self._get(self._url('followers/list.json'), params)

    def follower_ids(self, user_id: str = None, screen_name: str = None, cursor=None,
                     count: int = 200, skip_status: bool = None, include_user_entitles: bool = None):
        """
        Returns a cursored collection of user IDs for every user following the specified user.

        At this time, results are ordered with the most recent following first — however, this ordering
        is subject to unannounced change and eventual consistency issues. Results are given in groups
        of 20 users and multiple "pages" of results can be navigated through using the next_cursor value
        in subsequent requests. See Using cursors to navigate collections for more information.

        This method is especially powerful when used in conjunction with GET users / lookup,
        a method that allows you to convert user IDs into full user objects in bulk.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 15
        Requests / 15-min window (app auth): 15

        Check https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-followers-ids
        for more information.

        :param user_id: The ID of the user for whom to return results.
        :param screen_name: The screen name of the user for whom to return results.
        :param cursor: Causes the results to be broken into pages. If no cursor is provided,
                        a value of -1 will be assumed, which is the first "page."
                        The response from the API will include a previous_cursor and next_cursor to allow
                        paging back and forth. See Using cursors to navigate collections for more information.
        :param count: The number of users to return per page, up to a maximum of 200.
        :param skip_status: When set to either true, t or 1 statuses will not be included in the returned user objects.
        :param include_user_entitles: The user object entities node will not be included when set to false.
        :return: {
                    "ids": [],
                    "previous_cursor": 0,
                    "previous_cursor_str": "0",
                    "next_cursor": 1333504313713126852,
                    "next_cursor_str": "1333504313713126852"
                }
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get follower IDs: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required')
        return self._get(self._url('followers/ids.json'), params)

    def following(self, user_id: str = None, screen_name: str = None, cursor=None,
                  count: int = 200, stringify_ids: bool = None):
        """
        Returns a cursored collection of user IDs for every user the specified user is following
        (otherwise known as their "friends").

        At this time, results are ordered with the most recent following first — however, this
        ordering is subject to unannounced change and eventual consistency issues. Results are
        given in groups of 20 users and multiple "pages" of results can be navigated through using
        the next_cursor value in subsequent requests. See Using cursors to navigate collections
        for more information.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 15
        Requests / 15-min window (app auth): 15

        Check https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-friends-list
        for more information.

        :param user_id: The ID of the user for whom to return results.
        :param screen_name: The screen name of the user for whom to return results.
        :param cursor: Causes the results to be broken into pages. If no cursor is provided,
                        a value of -1 will be assumed, which is the first "page." The response
                        from the API will include a previous_cursor and next_cursor to allow
                        paging back and forth. See Using cursors to navigate collections for
                        more information.
        :param count: The number of users to return per page, up to a maximum of 200. Defaults to 20.
        :param stringify_ids:
        :return: {
                    "users": [
                          {user-object},
                          {user-object},
                          {user-object}
                    ],
                    "previous_cursor": 0,
                    "previous_cursor_str": "0",
                    "next_cursor": 1333504313713126852,
                    "next_cursor_str": "1333504313713126852"
                }
                For user-object, see https://developer.twitter.com/en/docs/tweets/data-dictionary/overview/user-object .
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get followings: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required')
        return self._get(self._url('friends/list.json'), params)

    def following_ids(self, user_id: str = None, screen_name: str = None, cursor=None,
                      count: int = 200, stringify_ids: bool = None):
        """
        Returns a cursored collection of user IDs for every user the specified user is following
        (otherwise known as their "friends").

        At this time, results are ordered with the most recent following first — however, this
        ordering is subject to unannounced change and eventual consistency issues. Results are
        given in groups of 20 users and multiple "pages" of results can be navigated through using
        the next_cursor value in subsequent requests. See Using cursors to navigate collections
        for more information.

        This method is especially powerful when used in conjunction with GET users / lookup,
        a method that allows you to convert user IDs into full user objects in bulk.

        Check https://developer.twitter.com/en/docs/accounts-and-users/follow-search-get-users/api-reference/get-friends-ids
        for more information.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 15
        Requests / 15-min window (app auth): 15

        :param user_id: The ID of the user for whom to return results.
        :param screen_name: The screen name of the user for whom to return results.
        :param cursor: Causes the results to be broken into pages. If no cursor is provided,
                        a value of -1 will be assumed, which is the first "page." The response
                        from the API will include a previous_cursor and next_cursor to allow
                        paging back and forth. See Using cursors to navigate collections for
                        more information.
        :param count: The number of users to return per page, up to a maximum of 200. Defaults to 20.
        :param stringify_ids:
        :return: {
                    "ids": [],
                    "previous_cursor": 0,
                    "previous_cursor_str": "0",
                    "next_cursor": 1333504313713126852,
                    "next_cursor_str": "1333504313713126852"
                }
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get following IDs: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required')
        return self._get(self._url('friends/ids.json'), params)

    def likes(self, user_id: str = None, screen_name: str = None, count: int = 200,
              since_id=None, max_id=None, include_entitles: bool = None):
        """
        Note: favorites are now known as likes.

        Returns the 20 most recent Tweets liked by the authenticating or specified user.

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 75
        Requests / 15-min window (app auth): 75

        Check https://developer.twitter.com/en/docs/tweets/post-and-engage/api-reference/get-favorites-list
        for more information.

        :param user_id: The ID of the user for whom to return results.
        :param screen_name: The screen name of the user for whom to return results.
        :param count: Specifies the number of records to retrieve. Must be less than or equal to 200; defaults to 20.
                      The value of count is best thought of as a limit to the number of Tweets to return because
                      suspended or deleted content is removed after the count has been applied.
        :param since_id: Returns results with an ID greater than (that is, more recent than) the specified ID.
                         There are limits to the number of Tweets which can be accessed through the API. If the
                         limit of Tweets has occured since the since_id, the since_id will be forced to the
                         oldest ID available.
        :param max_id: Returns results with an ID less than (that is, older than) or equal to the specified ID.
        :param include_entitles: The entities node will be omitted when set to false.
        :return: List of tweet objects.
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get likes: %s', params)
        if user_id is None and screen_name is None:
            raise ValueError('User ID or username is required')
        return self._get(self._url('favorites/list.json'), params)

    def tweet(self, tweet_id: str, trim_user: bool = None,
              include_my_retweet: bool = None, include_entitles: bool = None,
              include_ext_alt_text: bool = None, include_card_uri: bool = None):
        """
        Returns a single Tweet, specified by the id parameter.
        The Tweet's author will also be embedded within the Tweet.

        See GET statuses / lookup for getting Tweets in bulk (up to 100 per call).
        See also Embedded Timelines, Embedded Tweets, and GET statuses/oembed for tools
        to render Tweets according to Display Requirements.

        About Geo

        If there is no geotag for a status, then there will be an empty <geo></geo> or "geo" : {}.
        This can only be populated if the user has used the Geotagging API to send a statuses/update.

        The JSON response mostly uses conventions laid out in GeoJSON. The coordinates that
        Twitter renders are reversed from the GeoJSON specification (GeoJSON specifies a
        longitude then a latitude, whereas Twitter represents it as a latitude then a longitude),
        eg: "geo": { "type":"Point", "coordinates":[37.78029, -122.39697] }

        Response formats: JSON
        Requires authentication? Yes
        Rate limited? Yes
        Requests / 15-min window (user auth): 900
        Requests / 15-min window (app auth): 900

        :param tweet_id: The numerical ID of the desired Tweet.
        :param trim_user: When set to either true , t or 1 , each Tweet returned in a timeline will
                          include a user object including only the status authors numerical ID.
                          Omit this parameter to receive the complete user object.
        :param include_my_retweet: When set to either true , t or 1 , any Tweets returned that have
                                   been retweeted by the authenticating user will include an additional
                                   current_user_retweet node, containing the ID of the source status
                                   for the retweet.
        :param include_entitles: The entities node will not be included when set to false.
        :param include_ext_alt_text: If alt text has been added to any attached media entities, this
                                     parameter will return an ext_alt_text value in the top-level key
                                     for the media entity. If no value has been set, this will be
                                     returned as null.
        :param include_card_uri: When set to either true , t or 1 , the retrieved Tweet will include
                                 a card_uri attribute when there is an ads card attached to the Tweet
                                 and when that card was attached using the card_uri value.
        :return: The tweet object.
        """
        params = locals()
        del (params['self'])
        self.logger.info('Get tweet: %s', params)
        if tweet_id is None:
            raise ValueError('Tweet ID is required')
        return self._get(self._url('statuses/show.json'), params)


class TwitterDownloader:

    def __init__(self, path: PathGenerator, proxies: dict = None, logger=None, retry=RETRY):
        self.path = path
        self.proxies = proxies
        self.retry = retry
        self.logger = logger if logger is not None else Log.create_logger('TwitterSpider', './twitter.log')

    def _get(self, url):
        retry = self.retry
        while retry > 0:
            try:
                r = requests.get(url=url, proxies=self.proxies)
                if r.status_code == 200:
                    return r.content
                else:
                    raise NetworkException('Met error code {} when visiting {}.'.format(r.status_code, r.url))
            except requests.exceptions.RequestException as e:
                self.logger.error(e)
                retry -= 1
        raise RetryLimitExceededException('Max retry limit exceeded when visiting {}.'.format(url))

    def _save(self, content, path):
        if os.path.exists(path):
            self.logger.warning('File %s exists.', path)
            return False
        with open(path, 'wb') as f:
            f.write(content)
        return True

    def download(self, tweet: Tweet):
        user = tweet.user
        for media in tweet.medias:
            # def path(self, file_name, media_type, media_id, media_url, user_id, user_name, screen_name)
            path = self.path.path(file_name=media.file_name, media_type=media.type, media_id=media.id,
                                  media_url=media.url, user_id=user.id, user_name=user.name, screen_name=user.nickname)
            self._save(self._get(media.url), path)
