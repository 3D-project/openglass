import json
import time
import tweepy

class Twitter:

    def __init__(self, twitter_apis):
        self.twitter_apis = twitter_apis
        self.api = self.__authenticate(self.twitter_apis[0])
        self.api_index = 0

    def get_followers(self, user):
        followers = []

        try:
            followers = self.api.followers(user)
        except tweepy.RateLimitError as e:
            self.__handle_time_limit()
        except Exception as e:
            return e

        return followers

    def get_profile(self, user):
        profile = []

        try:
            profile = self.api.get_user(user_id)
        except tweepy.RateLimitError as e:
            self.__handle_time_limit()
        except Exception as e:
            return e

        return profile

    def get_statuses(self, user):
        tweets = []
        try:
            tweets = self.api.user_timeline(user, count=200)
        except tweepy.RateLimitError as e:
            self.__handle_time_limit()
        except Exception as e:
            return e

        return tweets

    def search(self, q):
        tweets = []

        try:
            tweets = self.api.search(q, count=500)
        except tweepy.RateLimitError as e:
            self.__handle_time_limit()
        except Exception as e:
            return e

        return tweets

    def __authenticate(self, credentials):
            auth = tweepy.OAuthHandler(credentials['CONSUMER_KEY'], credentials['CONSUMER_SECRET'])
            auth.set_access_token(credentials['ACCESS_KEY'], credentials['ACCESS_SECRET'])
            api = tweepy.API(auth, parser=tweepy.parsers.JSONParser())
            return api

    def __limit_handled(self, cursor):
        while True:
            try:
                yield cursor.next()
            except tweepy.RateLimitError:
                self.__handle_time_limit()

    def __handle_time_limit(self):
        if self.twitter_apis.len() > 1:
            if self.api_index == (self.twitter_apis.len() - 1):
                window = 15 / self.twitter_apis.len()
                time.sleep(window * 60)
            else:
                self.api_index += 1
                self.api = self.__authenticate(self.twitter_apis[self.api_index])
        else:
            time.sleep(15 * 60)
