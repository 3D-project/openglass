import re
import json
import time
import uuid
import http
import math
import random
import requests
import tweepy


class RotateKeys(Exception):
    '''
    exception used for repacing the cursor or stream and
    creating it again with new credentials
    '''
    pass


class StreamListener(tweepy.StreamListener):
    def __init__(self, twitter, callback):
        self.twitter = twitter
        self.callback = callback
        self.search_id = twitter.search_id
        self.current_url = twitter.current_url
        self.type = twitter.type
        self.last_rotation = time.time()
        super().__init__()

    def on_status(self, status):
        self.callback(self, status._json)
        MINS_15 = 15 * 60
        if time.time() - self.last_rotation > MINS_15:
            self.last_rotation = time.time()
            self.rotate_apikey()

    def on_error(self, status_code):
        self.rotate_apikey()
        return True

    def on_timeout(self):
        self.rotate_apikey()
        return True

    def rotate_apikey(self):
        '''raise a RotateKeys exception if there is more than one key'''
        if len(self.twitter.twitter_apis) > 1:
            raise RotateKeys()


class Twitter:

    def __init__(self, twitter_apis):
        self.twitter_apis = twitter_apis
        if len(self.twitter_apis) == 0:
            exit('Provide at least one Twitter API key')
        for twitter_api in self.twitter_apis:
            twitter_api['expired_at'] = {}
        self.api_in_use = random.choice(self.twitter_apis)
        self.api = self.__authenticate(self.api_in_use)
        self.current_url = ''
        self.search_id = str(uuid.uuid4())
        self.type = ''

    def get_retweeters(self, tweet_id, entry_handler):
        '''returns up to 100 user IDs that have retweeted the tweet'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/post-and-engage/api-reference/get-statuses-retweeters-ids
        count = 100
        request_per_window = 300
        if self.type == '':
            self.type = 'get_retweeters'

        cursor = tweepy.Cursor(self.api.retweeters, id=tweet_id, count=count)
        while True:
            self.current_url = '/statuses/retweeters/ids'
            try:
                for retweeter in self.__limit_handled(cursor.items()):
                    entry_handler(self, {'tweet_id': tweet_id, 'retweeter_id': str(retweeter)})
                return
            except RotateKeys:
                self.__handle_time_limit()
                old_cursor = cursor
                cursor = tweepy.Cursor(self.api.retweeters, id=tweet_id, count=count)
                cursor.iterator.next_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.prev_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.num_tweets = old_cursor.iterator.num_tweets
                continue
            except http.client.IncompleteRead:
                time.sleep(5)
                continue
            except requests.exceptions.ConnectionError:
                time.sleep(5)
                continue
            except tweepy.error.TweepError as e:
                print('got unknown error: {}'.format(str(e)))
                time.sleep(5)
                continue

    def statuses_lookup(self, tweet_ids):
        '''returns detail data from a list of tweet ids'''
        # https://developer.twitter.com/en/docs/tweets/post-and-engage/api-reference/get-statuses-lookup
        print('\nstatuses_lookup')
        max_per_request = 100
        request_per_window = 300
        if self.type == '':
            self.type = 'statuses_lookup'
        tweets_data = []

        while True:
            self.current_url = '/statuses/lookup'
            try:
                if len(tweet_ids) == 0:
                    return tweets_data
                batch = tweet_ids[:max_per_request]
                tweet_ids = tweet_ids[max_per_request:]
                tweets_data += [tweet._json for tweet in self.api.statuses_lookup(batch)]
            except tweepy.error.TweepError as e:
                if 'status code = 429' in str(e):
                    self.__handle_time_limit()
                    continue
                elif 'Failed to send request' in str(e):
                    time.sleep(5)
                    continue
                else:
                    print('got unknown error: {}'.format(str(e)))
                    time.sleep(5)
                    continue

    def get_retweeters_new(self, tweet_ids, entry_handler):
        '''returns the new retweeters from a given tweet'''
        if self.type == '':
            self.type = 'get_retweeters_new'

        user_ids = [tweet['user']['id_str'] for tweet in self.statuses_lookup(tweet_ids)]

        def callback(obj, tweet):
            if 'retweeted_status' not in tweet:
                return
            if tweet['retweeted_status']['id_str'] not in tweet_ids:
                return
            entry_handler(self, tweet)

        self.get_timeline_new(user_ids, callback)

    def get_followers(self, user, entry_handler):
        '''returns the followers of a user, ordered from new to old'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-followers-list
        count = 200
        request_per_window = 15
        if self.type == '':
            self.type = 'get_followers'
        profile = self.get_profile(user)
        followers_count = profile['followers_count']
        self.__show_running_time(followers_count, count, request_per_window)

        cursor = tweepy.Cursor(self.api.followers, id=user, count=count)
        while True:
            self.current_url = '/followers/list'
            try:
                for follower in self.__limit_handled(cursor.items()):
                    entry_handler(self, follower._json)
                return
            except RotateKeys:
                self.__handle_time_limit()
                old_cursor = cursor
                cursor = tweepy.Cursor(self.api.followers, id=user, count=count)
                cursor.iterator.next_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.prev_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.num_tweets = old_cursor.iterator.num_tweets
                continue
            except http.client.IncompleteRead:
                time.sleep(5)
                continue
            except requests.exceptions.ConnectionError:
                time.sleep(5)
                continue
            except tweepy.error.TweepError as e:
                print('got unknown error: {}'.format(str(e)))
                time.sleep(5)
                continue

    def get_profile(self, user):
        '''returns the profile information of a user'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-users-show
        request_per_window = 900
        if self.type == '':
            self.type = 'get_profile'

        while True:
            self.current_url = '/users/show'
            try:
                return self.api.get_user(id=user)._json
            except tweepy.error.TweepError as e:
                if 'status code = 429' in str(e):
                    self.__handle_time_limit()
                    continue
                elif 'Failed to send request' in str(e):
                    time.sleep(5)
                    continue
                else:
                    print('got unknown error: {}'.format(str(e)))
                    time.sleep(5)
                    continue

    def get_timeline(self, user, entry_handler):
        '''returns up to 3.200 of a user's most recent tweets'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/api-reference/get-statuses-user_timeline
        count = 200
        request_per_window = 1500
        profile = self.get_profile(user)
        statuses_count = min(3200, profile['statuses_count'])
        self.__show_running_time(statuses_count, count, request_per_window)
        if self.type == '':
            self.type = 'get_timeline'

        cursor = tweepy.Cursor(self.api.user_timeline, id=user, include_rts=True, count=count)
        while True:
            self.current_url = '/statuses/user_timeline'
            try:
                for tweet in self.__limit_handled(cursor.items()):
                    entry_handler(self, tweet._json)
                return
            except RotateKeys:
                self.__handle_time_limit()
                old_cursor = cursor
                cursor = tweepy.Cursor(self.api.user_timeline, id=user, include_rts=True, count=count)
                cursor.iterator.next_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.prev_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.num_tweets = old_cursor.iterator.num_tweets
                continue
            except http.client.IncompleteRead:
                time.sleep(5)
                continue
            except requests.exceptions.ConnectionError:
                time.sleep(5)
                continue
            except tweepy.error.TweepError as e:
                print('got unknown error: {}'.format(str(e)))
                time.sleep(5)
                continue

    def get_timeline_new(self, users, entry_handler):
        '''returns new tweets of a list of users'''
        if self.type == '':
            self.type = 'get_timeline_new'

        while True:
            self.current_url = '/statuses/user_timeline'
            try:
                stream_listener = StreamListener(self, entry_handler)
                stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
                stream.filter(follow=users)
                return
            except RotateKeys:
                self.rotate_apikey()
                continue
            except tweepy.error.TweepError as e:
                print('got unknown error: {}'.format(str(e)))
                time.sleep(5)
                continue

    def search(self, q, entry_handler):
        '''searches for already published tweets that match the search'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        count = 100
        request_per_window = 450
        if self.type == '':
            self.type = 'search'

        cursor = tweepy.Cursor(self.api.search, q=q, count=count)
        while True:
            self.current_url = '/search/tweets'
            try:
                for tweet in self.__limit_handled(cursor.items()):
                    entry_handler(self, tweet._json)
                return
            except RotateKeys:
                self.__handle_time_limit()
                old_cursor = cursor
                cursor = tweepy.Cursor(self.api.search, q=q, count=count)
                cursor.iterator.next_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.prev_cursor = old_cursor.iterator.next_cursor
                cursor.iterator.num_tweets = old_cursor.iterator.num_tweets
                continue
            except http.client.IncompleteRead:
                time.sleep(5)
                continue
            except requests.exceptions.ConnectionError:
                time.sleep(5)
                continue
            except tweepy.error.TweepError as e:
                print('got unknown error: {}'.format(str(e)))
                time.sleep(5)
                continue

    def search_new(self, q, entry_handler):
        '''returns new tweets that match the search'''
        if self.type == '':
            self.type = 'search_new'

        while True:
            self.current_url = '/search/tweets'
            try:
                stream_listener = StreamListener(self, entry_handler)
                stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
                stream.filter(track=q.split(' '))
                return
            except requests.exceptions.ConnectionError:
                time.sleep(5)
                continue
            except RotateKeys:
                self.rotate_apikey()
                continue
            except tweepy.error.TweepError as e:
                print('got unknown error: {}'.format(str(e)))
                time.sleep(5)
                continue

    def __name_to_id(self, id_name_list):
        id_list = []
        for id_name in id_name_list:
            if re.search(r'^\d+$', id_name):
                id_list.append(id_name)
            else:
                profile = self.get_profile(id_name)
                id_list.append(str(profile['id']))
        return id_list

    def watch_users(self, user_ids, entry_handler):
        '''saves all the tweets and its retweets for a list of users'''
        if self.type == '':
            self.type = 'watch_users'
        user_ids = self.__name_to_id(user_ids.split(' '))
        start_time = time.time()
        collected_data = []
        tweets_by_user = {}
        for user_id in user_ids:
            tweets_by_user[int(user_id)] = []

        def callback(obj, tweet):
            is_own_tweet = str(tweet['user']['id']) in user_ids
            is_retweet = 'retweeted_status' in tweet
            if is_retweet and tweet['retweeted_status']['user']['id_str'] not in user_ids:
                return  # the user was probably tagged
            if not is_own_tweet and is_retweet:
                entry = {}
                entry['type'] = 'retweet'
                entry['retweeted_user_id'] = tweet['retweeted_status']['user']['id']
                entry['retweeter_user_id'] = tweet['retweeted_status']['user']['id']
                entry['tweet_id'] = tweet['retweeted_status']['id']
                entry['retweet_id'] = tweet['id']
                entry['tweet'] = tweet
                entry_handler(self, entry)

                if tweet['id'] not in tweets_by_user[tweet['retweeted_status']['user']['id']]:
                    entry = {}
                    entry['type'] = 'old_tweet'
                    entry['user_id'] = tweet['retweeted_status']['user']['id']
                    entry['tweet_id'] = tweet['retweeted_status']['id']
                    entry['tweet'] = self.statuses_lookup([tweet['retweeted_status']['id']])[0]
                    entry_handler(self, entry)
                    tweets_by_user[tweet['retweeted_status']['user']['id']].append(tweet['id'])
            elif is_own_tweet:
                entry = {}
                entry['type'] = 'new_tweet'
                entry['user_id'] = tweet['user']['id']
                entry['tweet_id'] = tweet['id']
                entry['tweet'] = tweet
                entry_handler(self, entry)
                tweets_by_user[user_id].append(tweet['id'])
            else:
                pass  # somebody responded a tweet

        self.get_timeline_new(user_ids, callback)

    def __show_running_time(self, records_amount, count, request_per_window):
        apis_amount = len(self.twitter_apis)
        records_per_round = count * request_per_window * apis_amount
        rounds_needed = math.ceil(records_amount / records_per_round)
        minutes = (rounds_needed - 1) * 15

        if minutes == 0:
            return
        hours = int((minutes // 60) % 24)
        days = int(minutes // 1440)
        minutes = int(minutes % 60)
        msg = ''
        if days > 0:
            s = '' if days == 1 else 's'
            msg += '{} day{} '.format(days, s)
        if hours > 0:
            s = '' if hours == 1 else 's'
            msg += '{} hour{} '.format(hours, s)
        if minutes > 0:
            s = '' if minutes == 1 else 's'
            msg += '{} minute{} '.format(minutes, s)
        msg = msg[:-1]
        print('this will take approximately {} to finish'.format(msg))

    def __authenticate(self, credentials):
        '''authenticates to twitter with the given credentials'''
        auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
        auth.set_access_token(credentials['ACCESS_KEY'], credentials['ACCESS_SECRET'])
        api = tweepy.API(auth)
        return api

    def rotate_apikey(self):
        '''rotates the api key being used'''
        if len(self.twitter_apis) == 1:
            return
        for i, twitter_api in enumerate(self.twitter_apis):
            if twitter_api == self.api_in_use:
                self.api_in_use = self.twitter_apis[(i + 1) % len(self.twitter_apis)]
                self.api = self.__authenticate(self.api_in_use)
                return
        raise Exception('API Key not found')

    def __limit_handled(self, cursor):
        '''used by the cursors, handles rate limit'''
        while True:
            try:
                yield cursor.next()
            except StopIteration:
                return
            except tweepy.RateLimitError:
                raise RotateKeys()
            except tweepy.error.TweepError as e:
                if 'status code = 429' in str(e):
                    raise RotateKeys()
                elif 'Failed to send request' in str(e):
                    time.sleep(5)
                    continue
                else:
                    print('got unknown error: {}'.format(str(e)))
                    time.sleep(5)
                    continue

    def __handle_time_limit(self):
        '''
        changes the API key being used based on their expiration status
        if all keys are expired, it waits the smallest amount of time possible
        '''
        now = time.time()
        self.api_in_use['expired_at'][self.current_url] = now

        if len(self.twitter_apis) == 1:
            time.sleep(15 * 60)
            return

        valid_apis = [twitter_api for twitter_api in self.twitter_apis if self.current_url not in twitter_api['expired_at'] or now - twitter_api['expired_at'][self.current_url] >= 15 * 60]
        if len(valid_apis) > 0:
            self.api_in_use = valid_apis[0]
            self.api = self.__authenticate(self.api_in_use)
        else:
            self.twitter_apis.sort(reverse=False, key=lambda api: api['expired_at'][self.current_url])
            self.api_in_use = self.twitter_apis[0]
            time_expired = now - self.api_in_use['expired_at'][self.current_url]
            time.sleep(15 * 60 - time_expired + 3)
            self.api = self.__authenticate(self.api_in_use)
