import re
import json
import time
import uuid
import random
import tweepy


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
            self.rotate_apikey()
            self.last_rotation = time.time()

    def on_error(self, status_code):
        return True  # keep stream alive

    def on_timeout(self):
        return False  # restart streaming

    def rotate_apikey(self):
        self.twitter.rotate_apikey()
        self.auth = self.twitter.api.auth


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

    def get_retweeters(self, tweet_id, run_for):
        '''returns up to 100 user IDs that have retweeted the tweet'''
        self.current_url = '/statuses/retweeters/ids'
        if self.type == '':
            self.type = 'get_retweeters'
        retweeters = []
        start_time = time.time()
        print('Number of results: 0', end='\r')
        try:
            cursor = tweepy.Cursor(self.api.retweeters, id=tweet_id)
            for retweeter in self.__limit_handled(cursor.items()):
                retweeters.append(standarize_entry(self, {'tweet_id': tweet_id, 'retweeter_id': str(retweeter)}))
                print('Number of results: {}'.format(len(retweeters)), end='\r')
                if run_for and time.time() - start_time > run_for:
                    break
        except KeyboardInterrupt:
            pass
        return retweeters

    def get_retweeters_new(self, tweet_ids, run_for):
        '''returns the new retweeters from a given tweet'''
        self.current_url = '/statuses/retweeters/ids'
        if self.type == '':
            self.type = 'get_retweeters_new'

        start_time = time.time()
        tweets = [tweet._json for tweet in self.api.statuses_lookup(tweet_ids)]
        user_ids = [tweet['user']['id_str'] for tweet in tweets]
        retweeters = []
        print('Number of results: 0', end='\r')

        def callback(tweet):
            if run_for and time.time() - start_time > run_for:
                raise KeyboardInterrupt
            if 'retweeted_status' not in tweet:
                return
            if tweet['retweeted_status']['id_str'] not in tweet_ids:
                return

            retweeters.append(standarize_entry(self, tweet))
            print('Number of results: {}'.format(len(retweeters)), end='\r')

        try:
            self.get_timeline_new(user_ids, callback)
        except KeyboardInterrupt:
            pass

        return retweeters

    def get_followers(self, user, run_for):
        '''returns the followers of a user, ordered from new to old'''
        self.current_url = '/followers/list'
        self.type = 'get_followers'
        followers = []
        start_time = time.time()
        print('Number of results: 0', end='\r')
        try:
            cursor = tweepy.Cursor(self.api.followers, id=user)
            for follower in self.__limit_handled(cursor.items()):
                followers.append(standarize_entry(self, follower._json))
                print('Number of results: {}'.format(len(followers)), end='\r')
                if run_for and time.time() - start_time > run_for:
                    break
        except KeyboardInterrupt:
            pass
        return followers

    def get_profile(self, user):
        '''returns the profile information of a user'''
        self.current_url = '/users/show'
        self.type = 'get_profile'
        return standarize_entry(self, self.api.get_user(id=user)._json)

    def get_timeline(self, user, run_for):
        '''returns up to 3.200 of a user's most recent tweets'''
        self.current_url = '/statuses/user_timeline'
        self.type = 'get_timeline'
        tweets = []
        start_time = time.time()
        print('Number of results: 0', end='\r')
        try:
            cursor = tweepy.Cursor(self.api.user_timeline, id=user)
            for tweet in self.__limit_handled(cursor.items()):
                tweets.append(standarize_entry(self, tweet._json))
                print('Number of results: {}'.format(len(tweets)), end='\r')
                if run_for and time.time() - start_time > run_for:
                    break
        except KeyboardInterrupt:
            pass
        return tweets

    def get_timeline_new(self, users, callback):
        '''returns new tweets of a list of users'''
        self.current_url = '/statuses/user_timeline'
        if self.type == '':
            self.type = 'get_timeline_new'
        stream_listener = StreamListener(self, callback)
        stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream.filter(follow=users)

    def search(self, q, run_for):
        '''searches for already published tweets that match the search'''
        self.current_url = '/search/tweets'
        self.type = 'search'
        tweets = []
        start_time = time.time()
        print('Number of results: 0', end='\r')
        try:
            cursor = tweepy.Cursor(self.api.search, q=q)
            for tweet in self.__limit_handled(cursor.items()):
                tweets.append(standarize_entry(self, tweet._json))
                print('Number of results: {}'.format(len(tweets)), end='\r')
                if run_for and time.time() - start_time > run_for:
                    break
        except KeyboardInterrupt:
            pass
        return tweets

    def search_new(self, q, callback):
        '''returns new tweets that match the search'''
        self.current_url = '/search/tweets'
        self.type = 'search_new'
        stream_listener = StreamListener(self, callback)
        stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream.filter(track=q.split(' '))

    def __name_to_id(self, id_name_list):
        id_list = []
        for id_name in id_name_list:
            if re.search(r'^\d+$', id_name):
                id_list.append(id_name)
            else:
                profile = self.get_profile(id_name)
                id_list.append(str(profile['id']))
        return id_list

    def watch_users(self, user_ids, run_for):
        '''saves all the tweets and its retweets for a list of users'''
        self.type = 'watch_users'
        self.current_url = '/statuses/user_timeline'
        user_ids = self.__name_to_id(user_ids.split(' '))
        start_time = time.time()
        collected_data = []
        print('Number of results: 0', end='\r')
        tweets_by_user = {}
        for user_id in user_ids:
            tweets_by_user[int(user_id)] = []

        def num_results(data):
            num = 0
            for elem in data:
                if 'new_tweet' in elem:
                    num += 1
                num += len(elem['retweets'])
            return num

        def callback(tweet):
            tweet_id = tweet['id']
            user_id = tweet['user']['id']
            is_own_tweet = str(user_id) in user_ids
            is_retweet = 'retweeted_status' in tweet
            if is_retweet and tweet['retweeted_status']['user']['id_str'] not in user_ids:
                return  # the user was probably tagged
            if not is_own_tweet and is_retweet:
                user_id = tweet['retweeted_status']['user']['id']
                tweet_id = tweet['retweeted_status']['id']
                if tweet_id not in tweets_by_user[user_id]:
                    entry = {}
                    entry['user_id'] = user_id
                    entry['tweet_id'] = tweet_id
                    entry['retweets'] = [tweet]
                    try:
                        old_tweet = self.api.statuses_lookup([tweet_id])
                    except Exception:
                        old_tweet = []
                    if len(old_tweet) == 1:
                        entry['old_tweet'] = old_tweet[0]._json
                    tweets_by_user[user_id].append(tweet_id)
                    collected_data.append(entry)
                else:
                    entry = [entry for entry in collected_data if entry['user_id'] == user_id and entry['tweet_id'] == tweet_id][0]
                    entry['retweets'].append(tweet)
                print('Number of results: {}'.format(num_results(collected_data)), end='\r')
            elif is_own_tweet:
                entry = {}
                entry['user_id'] = user_id
                entry['tweet_id'] = tweet_id
                entry['new_tweet'] = tweet
                entry['retweets'] = []
                tweets_by_user[user_id].append(tweet_id)
                print('Number of results: {}'.format(num_results(collected_data)), end='\r')
            else:
                pass  # somebody responded a tweet

            if run_for and time.time() - start_time > run_for:
                raise KeyboardInterrupt

        try:
            self.get_timeline_new(user_ids, callback)
        except KeyboardInterrupt:
            pass

        return collected_data

    def __authenticate(self, credentials):
        '''authenticates to twitter with the given credentials'''
        auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
        auth.set_access_token(credentials['ACCESS_KEY'], credentials['ACCESS_SECRET'])
        api = tweepy.API(auth)
        return api

    def rotate_apikey(self):
        '''rotates the api key being used'''
        index = -1
        for i, twitter_api in enumerate(self.twitter_apis):
            if twitter_api == self.api_in_use:
                index = i
                break
        self.api_in_use = self.twitter_apis[(index + 1) % len(self.twitter_apis)]
        self.api = self.__authenticate(self.api_in_use)

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
