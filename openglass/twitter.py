import re
import json
import time
import uuid
import math
import random
import tweepy


class RotateKeys(Exception):
    '''
    exception used for killing the stream and
    creating it again with new credentials each 15 minutes
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
        self.callback(standarize_entry(self, status._json))
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
        self.current_url = '/statuses/retweeters/ids'
        if self.type == '':
            self.type = 'get_retweeters'
        try:
            cursor = tweepy.Cursor(self.api.retweeters, id=tweet_id, count=count)
            for retweeter in self.__limit_handled(cursor.items()):
                entry_handler(standarize_entry(self, {'tweet_id': tweet_id, 'retweeter_id': str(retweeter)}))
        except KeyboardInterrupt:
            pass

    def get_retweeters_new(self, tweet_ids, entry_handler):
        '''returns the new retweeters from a given tweet'''
        self.current_url = '/statuses/retweeters/ids'
        if self.type == '':
            self.type = 'get_retweeters_new'

        user_ids = [tweet._json['user']['id_str'] for tweet in self.api.statuses_lookup(tweet_ids)]

        def callback(tweet):
            if 'retweeted_status' not in tweet:
                return
            if tweet['retweeted_status']['id_str'] not in tweet_ids:
                return
            entry_handler(standarize_entry(self, tweet))

        self.get_timeline_new(user_ids, callback)

    def get_followers(self, user, entry_handler):
        '''returns the followers of a user, ordered from new to old'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-followers-list
        count = 200
        request_per_window = 15
        profile = self.get_profile(user)
        followers_count = profile['followers_count']
        self.__show_running_time(followers_count, count, request_per_window)
        self.current_url = '/followers/list'
        self.type = 'get_followers'

        cursor = tweepy.Cursor(self.api.followers, id=user, count=count)
        for follower in self.__limit_handled(cursor.items()):
            entry_handler(standarize_entry(self, follower._json))

    def get_profile(self, user):
        '''returns the profile information of a user'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/accounts-and-users/follow-search-get-users/api-reference/get-users-show
        request_per_window = 900
        self.current_url = '/users/show'
        self.type = 'get_profile'
        return standarize_entry(self, self.api.get_user(id=user)._json)

    def get_timeline(self, user, entry_handler):
        '''returns up to 3.200 of a user's most recent tweets'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/timelines/api-reference/get-statuses-user_timeline
        count = 200
        request_per_window = 1500
        profile = self.get_profile(user)
        statuses_count = min(3200, profile['statuses_count'])
        self.__show_running_time(statuses_count, count, request_per_window)
        self.current_url = '/statuses/user_timeline'
        self.type = 'get_timeline'

        cursor = tweepy.Cursor(self.api.user_timeline, id=user, include_rts=True, count=count)
        for tweet in self.__limit_handled(cursor.items()):
            entry_handler(standarize_entry(self, tweet._json))

    def get_timeline_new(self, users, entry_handler):
        '''returns new tweets of a list of users'''
        self.current_url = '/statuses/user_timeline'
        if self.type == '':
            self.type = 'get_timeline_new'

        while True:
            try:
                stream_listener = StreamListener(self, entry_handler)
                stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
                stream.filter(follow=users)
                return
            except RotateKeys:
                self.rotate_apikey()
                continue

    def search(self, q, entry_handler):
        '''searches for already published tweets that match the search'''
        # https://developer.twitter.com/en/docs/twitter-api/v1/tweets/search/api-reference/get-search-tweets
        count = 100
        request_per_window = 450
        self.current_url = '/search/tweets'
        self.type = 'search'

        cursor = tweepy.Cursor(self.api.search, q=q, count=count)
        for tweet in self.__limit_handled(cursor.items()):
            entry_handler(standarize_entry(self, tweet._json))

    def search_new(self, q, entry_handler):
        '''returns new tweets that match the search'''
        self.current_url = '/search/tweets'
        self.type = 'search_new'

        while True:
            try:
                stream_listener = StreamListener(self, entry_handler)
                stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
                stream.filter(track=q.split(' '))
                return
            except RotateKeys:
                self.rotate_apikey()
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
        self.type = 'watch_users'
        self.current_url = '/statuses/user_timeline'
        user_ids = self.__name_to_id(user_ids.split(' '))
        start_time = time.time()
        collected_data = []
        tweets_by_user = {}
        for user_id in user_ids:
            tweets_by_user[int(user_id)] = []

        def callback(tweet):
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
                entry_handler(standarize_entry(self, entry))

                if tweet['id'] not in tweets_by_user[tweet['retweeted_status']['user']['id']]:

                    entry = {}
                    entry['type'] = 'old_tweet'
                    entry['user_id'] = tweet['retweeted_status']['user']['id']
                    entry['tweet_id'] = tweet['retweeted_status']['id']
                    try:
                        old_tweet = self.api.statuses_lookup([tweet['retweeted_status']['id']])
                        if len(old_tweet) == 1:
                            old_tweet = old_tweet[0]._json
                            entry['tweet'] = old_tweet
                            entry_handler(standarize_entry(self, entry))
                            tweets_by_user[tweet['retweeted_status']['user']['id']].append(tweet_id)
                    except Exception:
                        pass
            elif is_own_tweet:
                entry = {}
                entry['type'] = 'new_tweet'
                entry['user_id'] = tweet['user']['id']
                entry['tweet_id'] = tweet['id']
                entry['tweet'] = tweet
                entry_handler(standarize_entry(self, entry))
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
                return None
            except tweepy.RateLimitError:
                self.__handle_time_limit()
            except tweepy.error.TweepError as e:
                if 'status code = 429' in str(e):
                    self.__handle_time_limit()
                else:
                    raise e

    def __handle_time_limit(self):
        '''
        changes the API key being used based on their expiration status
        if all keys are expired, it waits the smallest amount of time possible
        '''
        now = time.time()
        self.api_in_use['expired_at'][self.current_url] = now

        valid_apis = [twitter_api for twitter_api in self.twitter_apis if self.current_url not in twitter_api['expired_at'] or now - twitter_api['expired_at'][self.current_url] >= 15 * 60]
        if len(valid_apis) > 0:
            self.api_in_use = valid_apis[0]
            self.api = self.__authenticate(self.api_in_use)
        else:
            self.twitter_apis.sort(reverse=False, key=lambda api: api['expired_at'][self.current_url])
            self.api_in_use = self.twitter_apis[0]
            time_expired = now - self.api_in_use['expired_at'][self.current_url]
            self.api = self.__authenticate(self.api_in_use)
            time.sleep(15 * 60 - time_expired)


def standarize_entry(obj, entry):
    '''
    remove unused entrys and add important fields
    a unique id 'og_id', a search id 'og_search_id'
    a timestamp 'og_timestamp' and a type 'og_type'
    '''
    keys_to_delete = ['id_str', 'profile_background_color', 'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color', 'profile_text_color', 'favorited', 'filter_level']
    for key_to_delete in keys_to_delete:
        if key_to_delete in entry:
            del entry[key_to_delete]
    entry['og_id'] = str(uuid.uuid4())
    entry['og_search_id'] = obj.search_id
    entry['og_timestamp'] = int(time.time())
    entry['og_type'] = obj.type
    entry['og_endpoint'] = obj.current_url
    return entry
