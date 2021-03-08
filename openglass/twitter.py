import json
import time
import tweepy

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

    def get_retweeters(self, tweet_id):
        self.current_url = '/statuses/retweeters/ids'
        return  [ retweeter for retweeter in self.__limit_handled(tweepy.Cursor(self.api.retweeters, id=tweet_id).items()) ]

    def get_followers(self, user):
        self.current_url = '/followers/list'
        return [ follower._json for follower in self.__limit_handled(tweepy.Cursor(self.api.followers, id=user).items()) ]

    def get_profile(self, user):
        self.current_url = '/users/show'
        return self.api.get_user(id=user)._json

    def get_statuses(self, user):
        self.current_url = '/statuses/user_timeline'
        return [ tweet._json for tweet in self.__limit_handled(tweepy.Cursor(self.api.user_timeline, id=user).items()) ]

    def search(self, q):
        self.current_url = '/search/tweets'
        return [ tweet._json for tweet in self.__limit_handled(tweepy.Cursor(self.api.search, q=q).items()) ]

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
