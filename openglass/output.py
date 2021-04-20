#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import os
import json
import time
import queue

# global variables used to avoid duplicating entries in the csv files
user_v_saved = []
tweet_v_saved = []
hashtag_v_saved = []
follows_e_saved = []
followed_e_saved = []
tweets_e_saved = []
retweets_e_saved = []
responds_e_saved = []
mentions_e_saved = []
usesht_e_saved = []


class User:
    def __init__(self, json_entry, output_dir, filename):
        self.header = 'uid,name,screen_name,location,description,protected,followers_count,friends_count,listed_count,statuses_count,created_at,favourites_count,verified,profile_use_background_image,has_extended_profile,default_profile,default_profile_image'
        self.id = json_entry['id']
        self.name = json_entry['name'].replace('"', '""')
        self.screen_name = json_entry['screen_name']
        self.location = json_entry.get('location', '')
        self.description = json_entry.get('description', None)
        if self.description is None:
            self.description = ''
        self.description = self.description.replace('"', '""')
        self.protected = json_entry.get('protected', False)
        self.followers_count = json_entry.get('followers_count', 0)
        self.friends_count = json_entry.get('friends_count', 0)
        self.listed_count = json_entry.get('listed_count', 0)
        self.statuses_count = json_entry.get('statuses_count', 0)
        self.created_at = json_entry.get('created_at', None)
        self.favourites_count = json_entry.get('favorites_count', 0)
        self.verified = json_entry.get('verified', False)
        self.profile_use_background_image = json_entry.get('profile_use_background_image', False)
        self.has_extended_profile = json_entry.get('has_extended_profile', False)
        self.default_profile = json_entry.get('default_profile', False)
        self.default_profile_image = json_entry.get('default_profile_image', False)
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.id},'
        entry += f'"{self.name}",'
        entry += f'{self.screen_name},'
        entry += f'"{self.location}",'
        entry += f'"{self.description}",'
        entry += f'{self.protected},'
        entry += f'{self.followers_count},'
        entry += f'{self.friends_count},'
        entry += f'{self.listed_count},'
        entry += f'{self.statuses_count},'
        entry += f'{self.created_at},'
        entry += f'{self.favourites_count},'
        entry += f'{self.verified},'
        entry += f'{self.profile_use_background_image},'
        entry += f'{self.has_extended_profile},'
        entry += f'{self.default_profile},'
        entry += f'{self.default_profile_image}'
        return entry

    def save_to_file(self, output_dir, filename):
        global user_v_saved
        if self.id in user_v_saved:
            return
        filename = f'{output_dir}/users_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        user_v_saved.append(self.id)


class Tweet:
    def __init__(self, json_entry, output_dir, filename):
        self.header = 'uid,text,truncated,is_quote_status,retweet_count,favorite_count,possibly_sensitive,lang,link,app,media_urls'
        self.id = json_entry['id']
        if 'extended_tweet' in json_entry:
            self.text = json_entry['extended_tweet']['full_text']
        else:
            self.text = json_entry['text']
        self.text = self.text.replace('"', '""')
        self.truncated = json_entry.get('truncated', False)
        self.is_quote_status = json_entry.get('is_quote_status', None)
        self.retweet_count = json_entry.get('retweet_count', None)
        self.favorite_count = json_entry.get('favorite_count', None)
        self.possibly_sensitive = json_entry.get('possibly_sensitive', None)
        self.lang = json_entry.get('lang', None)
        user = json_entry.get('user', {}).get('screen_name', 'UNKNOWN')
        self.link = f'https://twitter.com/{user}/status/{self.id}'
        match = re.search(r'>(.*?)</a>', json_entry.get('source', 'UNKNOWN'))
        if match is not None:
            self.app = match.group(1)
        else:
            self.app = json_entry.get('source', 'UNKNOWN')
        self.app = self.app.replace('"', '""')
        urls = json_entry.get('entities', {}).get('urls', [])
        self.media_urls = '-'.join([url['expanded_url'] for url in urls])
        self.save_to_file(output_dir, filename)

        for mentioned_user in json_entry.get('entities', {}).get('user_mentions', []):
            user = User(mentioned_user, output_dir, filename)
            Mentions(self.id, user.id, output_dir, filename)

        for ht in json_entry.get('entities', {}).get('hashtags', []):
            hashtag = ht['text']
            Hashtag(hashtag, output_dir, filename)
            UsesHT(self.id, hashtag, output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.id},'
        entry += f'"{self.text}",'
        entry += f'{self.truncated},'
        entry += f'{self.is_quote_status},'
        entry += f'{self.retweet_count},'
        entry += f'{self.favorite_count},'
        entry += f'{self.possibly_sensitive},'
        entry += f'{self.lang},'
        entry += f'{self.link},'
        entry += f'"{self.app}",'
        entry += f'{self.media_urls}'
        return entry

    def save_to_file(self, output_dir, filename):
        global tweet_v_saved
        if self.id in tweet_v_saved:
            return
        filename = f'{output_dir}/tweets_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        tweet_v_saved.append(self.id)


class Hashtag:
    def __init__(self, name, output_dir, filename):
        self.header = 'htname'
        self.name = name
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.name}'
        return entry

    def save_to_file(self, output_dir, filename):
        global hashtag_v_saved
        if self.name in hashtag_v_saved:
            return
        filename = f'{output_dir}/hashtags_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        hashtag_v_saved.append(self.name)


class Follows:
    def __init__(self, follower_id, followed_id, follower_number, output_dir, filename):
        self.header = 'follower_id,followed_id,follower_number'
        self.follower_id = follower_id
        self.followed_id = followed_id
        self.follower_number = follower_number
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.follower_id},'
        entry += f'{self.followed_id},'
        entry += f'{self.follower_number}'
        return entry

    def save_to_file(self, output_dir, filename):
        global follows_e_saved
        if (self.follower_id, self.followed_id) in follows_e_saved:
            return
        filename = f'{output_dir}/follows_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        follows_e_saved.append((self.follower_id, self.followed_id))


class Followed:
    def __init__(self, followed_id, follower_id, output_dir, filename):
        self.header = 'followed_id,follower_id'
        self.followed_id = followed_id
        self.follower_id = follower_id
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.followed_id},'
        entry += f'{self.follower_id}'
        return entry

    def save_to_file(self, output_dir, filename):
        global follows_e_saved
        if (self.followed_id, self.follower_id) in follows_e_saved:
            return
        filename = f'{output_dir}/followed_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        follows_e_saved.append((self.followed_id, self.follower_id))


class Tweeted:
    def __init__(self, user_id, tweet_id, output_dir, filename):
        self.header = 'user_id,tweet_id'
        self.user_id = user_id
        self.tweet_id = tweet_id
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.user_id},'
        entry += f'{self.tweet_id}'
        return entry

    def save_to_file(self, output_dir, filename):
        global tweets_e_saved
        if (self.user_id, self.tweet_id) in tweets_e_saved:
            return
        filename = f'{output_dir}/tweeted_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        tweets_e_saved.append((self.user_id, self.tweet_id))


class Retweeted:
    def __init__(self, t_retweeter_id, t_retweeted_id, output_dir, filename):
        self.header = 't_retweeter_id,t_retweeted_id'
        self.t_retweeter_id = t_retweeted_id
        self.t_retweeted_id = t_retweeted_id
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.t_retweeter_id},'
        entry += f'{self.t_retweeted_id}'
        return entry

    def save_to_file(self, output_dir, filename):
        global retweets_e_saved
        if (self.t_retweeter_id, self.t_retweeted_id) in retweets_e_saved:
            return
        filename = f'{output_dir}/retweeted_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        retweets_e_saved.append((self.t_retweeter_id, self.t_retweeted_id))


class Replied:
    def __init__(self, t_replier_id, t_replied_id, output_dir, filename):
        self.header = 't_replier_id,t_replied_id'
        self.t_replier_id = t_replier_id
        self.t_replied_id = t_replied_id
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.t_replier_id},'
        entry += f'{self.t_replied_id}'
        return entry

    def save_to_file(self, output_dir, filename):
        global responds_e_saved
        if (self.t_replier_id, self.t_replied_id) in responds_e_saved:
            return
        filename = f'{output_dir}/responds_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        responds_e_saved.append((self.t_replier_id, self.t_replied_id))


class Mentions:
    def __init__(self, t_mentioner_id, u_mentioned_id, output_dir, filename):
        self.header = 't_mentioner_id,u_mentioned_id'
        self.t_mentioner_id = t_mentioner_id
        self.u_mentioned_id = u_mentioned_id
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.t_mentioner_id},'
        entry += f'{self.u_mentioned_id}'
        return entry

    def save_to_file(self, output_dir, filename):
        global mentions_e_saved
        if (self.t_mentioner_id, self.u_mentioned_id) in mentions_e_saved:
            return
        filename = f'{output_dir}/mentions_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        mentions_e_saved.append((self.t_mentioner_id, self.u_mentioned_id))


class UsesHT:
    def __init__(self, tweet_id, hashtag, output_dir, filename):
        self.header = 'tweet_id,hashtag'
        self.tweet_id = tweet_id
        self.hashtag = hashtag
        self.save_to_file(output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.tweet_id},'
        entry += f'{self.hashtag}'
        return entry

    def save_to_file(self, output_dir, filename):
        global usesht_e_saved
        if (self.tweet_id, self.hashtag) in usesht_e_saved:
            return
        filename = f'{output_dir}/usesht_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        usesht_e_saved.append((self.tweet_id, self.hashtag))


def followers_to_csv(entry, output_dir, filename):
    '''converts input from the followers function into csv'''
    followed = User(entry['follows'], output_dir, filename)
    follower = User(entry, output_dir, filename)

    Follows(follower.id, followed.id, entry['follower_number'], output_dir, filename)


def profile_to_csv(entry, output_dir, filename):
    '''converts input from the profile function into csv'''
    User(entry, output_dir, filename)


def friends_to_csv(entry, output_dir, filename):
    '''converts input from the friends function into csv'''
    follower = User(entry['is_followed_by'], output_dir, filename)
    followed = User(entry, output_dir, filename)

    Followed(followed.id, follower.id, output_dir, filename)


def tweet_to_csv(tweeter, tweet, enrich, output_dir, filename):
    '''converts input from the watch function into csv'''
    is_retweet = 'retweeted_status' in tweet
    is_reply = tweet.get('in_reply_to_status_id_str', None) is not None
    is_quote = 'quoted_status' in tweet

    if enrich:
        entities = tweet.get('entities', {})
        user_mentions = entities.get('user_mentions', [])
        profiles = [tweeter.get_profile(um['id_str']) for um in user_mentions if um['id'] not in user_v_saved]
        tweet['entities']['user_mentions'] = profiles
    else:
        tweet['entities'] = {}

    if is_retweet:
        user_retweeter = User(tweet['user'], output_dir, filename)
        user_retweeted = User(tweet['retweeted_status']['user'], output_dir, filename)

        tweet_retweeted = Tweet(tweet['retweeted_status'], output_dir, filename)
        tweet_retweeter = Tweet(tweet, output_dir, filename)

        Tweeted(user_retweeted.id, tweet_retweeted.id, output_dir, filename)
        Tweeted(user_retweeter.id, tweet_retweeter.id, output_dir, filename)

        Retweeted(tweet_retweeter.id, tweet_retweeted.id, output_dir, filename)
    elif is_reply:
        user_replier = User(tweet['user'], output_dir, filename)
        tweet_replier = Tweet(tweet, output_dir, filename)
        Tweeted(user_replier.id, tweet_replier.id, output_dir, filename)

        tweet_replied_id = tweet['in_reply_to_status_id']
        # if the replied tweet is new, save it
        if enrich and tweet_replied_id not in tweet_v_saved:
            tweet_replied = tweeter.statuses_lookup([str(tweet_replied_id)])
            if len(tweet_replied) == 1:
                tweet_replied_j = tweet_replied[0]
            else:
                tweet_replied_j = {}
                tweet_replied_j['id'] = tweet_replied_id
                tweet_replied_j['text'] = ''
            tweet_replied = Tweet(tweet_replied_j, output_dir, filename)
            # if the tweet does no longer exist, it will not have a user filed
            if 'user' in tweet_replied_j:
                user_replied = User(tweet_replied_j['user'], output_dir, filename)
                Tweeted(user_replied.id, tweet_replied.id, output_dir, filename)

        Replied(tweet_replier.id, tweet_replied_id, output_dir, filename)
    elif is_quote:
        user_quoter = User(tweet['user'], output_dir, filename)
        user_quoted = User(tweet['quoted_status']['user'], output_dir, filename)

        tweet_quoter = Tweet(tweet, output_dir, filename)
        tweet_quoted = Tweet(tweet['quoted_status'], output_dir, filename)

        Tweeted(user_quoter.id, tweet_quoter.id, output_dir, filename)
        Tweeted(user_quoted.id, tweet_quoted.id, output_dir, filename)

        Retweeted(tweet_quoter.id, tweet_quoted.id, output_dir, filename)
    else:
        user = User(tweet['user'], output_dir, filename)

        tweet = Tweet(tweet, output_dir, filename)

        Tweeted(user.id, tweet.id, output_dir, filename)


def store_result(t, entry, args, filename, start_time):
    '''save the result in as a .csv, .jsonl, janus .csv or print as json'''
    if args.csv:
        filename = f'{filename}_{start_time}.csv'
        save_as_csv(t, entry, args, filename)
    elif args.jsonl:
        filename = f'{filename}_{start_time}.jsonl'
        save_as_jsonl(entry, args, filename)
    else:
        print(json.dumps(entry, indent=4, sort_keys=True))


def save_as_csv(t, entry, args, filename):
    '''Takes an entry as input and saves it in CSV format, optimized for janusgraph'''
    entry_type = entry.get('og_type', None)
    if entry_type == 'get_followers':
        followers_to_csv(entry, args.output, filename)
    elif entry_type == 'get_profile':
        profile_to_csv(entry, args.output, filename)
    elif entry_type == 'get_friends':
        friends_to_csv(entry, args.output, filename)
    elif (entry_type == 'watch' or
          entry_type == 'get_timeline' or
          entry_type == 'get_timeline_new' or
          entry_type == 'search'):
        tweet_to_csv(t, entry, args.enrich, args.output, filename)
    else:
        raise Exception('save_as_janus: entry type \'{}\' not supported'.format(entry_type))


def save_as_jsonl(entry, args, jsonfile):
    '''Takes an entry as input and saves it in a JSON L file.'''
    fd = os.open(f'{args.output}/{jsonfile}', os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
    os.write(fd, json.dumps(entry).encode('utf-8') + b'\n')
    os.close(fd)


def writer(t, q, flag, args, filename, start_time):
    '''waits entries in the queue and stores the results'''
    try:
        printed = False
        while True:
            try:
                if flag.value == 0:
                    entry = q.get()
                else:
                    if not printed:
                        print('')
                        printed = True
                    print(f'Finishing... {q.qsize()}', end='\r')
                    entry = q.get(True, 1)
                store_result(t, entry, args, filename, start_time)
            except KeyboardInterrupt:
                pass
    except queue.Empty:
        pass

