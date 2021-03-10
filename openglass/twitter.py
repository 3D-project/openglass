import json
import time
import tweepy
import uuid

class StreamListener(tweepy.StreamListener):
    def __init__(self, callback, search_id, current_url):
        self.callback = callback
        self.search_id = search_id
        self.current_url = current_url
        super().__init__()

    def on_status(self, status):
        self.callback(standarize_entry(self, status._json))

class Twitter:

    def __init__(self, twitter_apis):
        self.twitter_apis = twitter_apis
        if len(self.twitter_apis) == 0:
            exit('Provide at least one Twitter API key')
        for twitter_api in self.twitter_apis:
            twitter_api['expired_at'] = {}
        self.api_in_use = self.twitter_apis[0]
        self.api = self.__authenticate(self.api_in_use)
        self.current_url = ''
        self.search_id = str(uuid.uuid4())

    def get_retweeters(self, tweet_id):
        self.current_url = '/statuses/retweeters/ids'
        return [ standarize_entry(self, {'tweet_id': tweet_id, 'retweeter_id': retweeter}) for retweeter in self.__limit_handled(tweepy.Cursor(self.api.retweeters, id=tweet_id).items()) ]

    def get_followers(self, user):
        self.current_url = '/followers/list'
        return [ standarize_entry(self, follower._json) for follower in self.__limit_handled(tweepy.Cursor(self.api.followers, id=user).items()) ]

    def get_profile(self, user):
        self.current_url = '/users/show'
        return standarize_entry(self, self.api.get_user(id=user)._json)

    def get_timeline(self, user):
        self.current_url = '/statuses/user_timeline'
        return [ standarize_entry(self, tweet._json) for tweet in self.__limit_handled(tweepy.Cursor(self.api.user_timeline, id=user).items()) ]

    def get_timeline_new(self, users, callback):
        self.current_url = '/statuses/user_timeline'
        stream_listener = StreamListener(callback, self.search_id, self.current_url)
        stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream.filter(follow=users.split(' '))

    def search(self, q):
        self.current_url = '/search/tweets'
        return [ standarize_entry(self, tweet._json) for tweet in self.__limit_handled(tweepy.Cursor(self.api.search, q=q).items()) ]

    def search_new(self, q, callback):
        self.current_url = '/search/tweets'
        stream_listener = StreamListener(callback, self.search_id, self.current_url)
        stream = tweepy.Stream(auth=self.api.auth, listener=stream_listener)
        stream.filter(track=q.split(' '))

    def __authenticate(self, credentials):
        auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
        auth.set_access_token(credentials['ACCESS_KEY'], credentials['ACCESS_SECRET'])
        api = tweepy.API(auth)
        return api

    def __limit_handled(self, cursor):
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
        now = time.time()
        self.api_in_use['expired_at'][self.current_url] = now

        valid_apis = [ twitter_api for twitter_api in self.twitter_apis if self.current_url not in twitter_api['expired_at'] or now - twitter_api['expired_at'][self.current_url] >= 15 * 60 ]
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
    keys_to_delete = ['id_str', 'profile_background_color', 'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color', 'profile_text_color', 'favorited', 'filter_level']
    for key_to_delete in keys_to_delete:
        if key_to_delete in entry:
            del entry[key_to_delete]
    entry['og_id'] = str(uuid.uuid4())
    entry['og_search_id'] = obj.search_id
    entry['og_timestamp'] = int(time.time())
    entry['og_type'] = obj.current_url
    return entry
