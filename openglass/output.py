#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json

# global variables used to avoid duplicating entries in the csv files
users_saved = []
tweets_saved = []


class User:
    def __init__(self, json_entry, output_dir, filename):
        self.header = 'uid,name,screen_name,location,description,protected,followers_count,friends_count,listed_count,statuses_count,created_at,favourites_count,verified,profile_use_background_image,has_extended_profile,default_profile,default_profile_image'
        self.id = json_entry['id']
        self.name = json_entry['name'].replace('"', '""')
        self.screen_name = json_entry['screen_name']
        self.location = json_entry.get('location', '')
        self.description = json_entry.get('description', '').replace('"', '""')
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
        global users_saved
        filename = f'{output_dir}/users_{filename}'
        if self.id in users_saved:
            return
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        users_saved.append(self.id)


class Tweet:
    def __init__(self, json_entry, output_dir, filename):
        self.header ='uid,text,truncated,is_quote_status,retweet_count,favorite_count,possibly_sensitive,lang'
        self.id = json_entry['id']
        if 'extended_tweet' in json_entry:
            self.text = json_entry['extended_tweet']['full_text']
        else:
            self.text = json_entry['text']
        self.text = self.text.replace('"', '""')
        self.truncated = json_entry.get('truncated', False)
        self.in_reply_to_status_id = json_entry.get('in_reply_to_user_id', None) # this should be a relation
        self.in_reply_to_user_id = json_entry.get('in_reply_to_user_id', None)  # this should be a relation
        self.is_quote_status = json_entry.get('is_quote_status', None)
        self.retweet_count = json_entry.get('retweet_count', None)
        self.favorite_count = json_entry.get('favorite_count', None)
        self.possibly_sensitive = json_entry.get('possibly_sensitive', None)
        self.lang = json_entry.get('lang', None)
        self.save_to_file(output_dir, filename)
        for mentioned_user in json_entry.get('entities', {}).get('user_mentions', []):
            Mentions(self.id, mentioned_user['id'], output_dir, filename)

    def to_entry(self):
        entry = ''
        entry += f'{self.id},'
        entry += f'"{self.text}",'
        entry += f'{self.truncated},'
        entry += f'{self.is_quote_status},'
        entry += f'{self.retweet_count},'
        entry += f'{self.favorite_count},'
        entry += f'{self.possibly_sensitive},'
        entry += f'{self.lang}'
        return entry

    def save_to_file(self, output_dir, filename):
        global tweets_saved
        filename = f'{output_dir}/tweets_{filename}'
        if self.id in tweets_saved:
            return
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)
        tweets_saved.append(self.id)


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
        filename = f'{output_dir}/follows_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


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
        filename = f'{output_dir}/followed_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


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
        filename = f'{output_dir}/tweeted_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


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
        filename = f'{output_dir}/retweeted_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


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
        filename = f'{output_dir}/responds_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


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
        filename = f'{output_dir}/mentions_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


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


def timeline_to_csv(entry, output_dir, filename):
    '''converts input from the timeline function into csv'''
    tweet = Tweet(entry, output_dir, filename)

    user = User(entry['user'], output_dir, filename)

    Tweeted(user.id, tweet.id, output_dir, filename)


def stream_to_csv(entry, output_dir, filename):
    '''converts input from the watch function into csv'''
    t = entry['tweet']
    if entry['type'] == 'retweet':
        user_retweeter = User(t['user'], output_dir, filename)
        user_retweeted = User(t['retweeted_status']['user'], output_dir, filename)

        tweet_retweeted = Tweet(t['retweeted_status'], output_dir, filename)
        tweet_retweeter = Tweet(t, output_dir, filename)

        Tweeted(user_retweeted.id, tweet_retweeted.id, output_dir, filename)
        Tweeted(user_retweeter.id, tweet_retweeter.id, output_dir, filename)

        Retweeted(tweet_retweeter.id, tweet_retweeted.id, output_dir, filename)
    elif entry['type'] == 'reply':
        user_replier = User(t['user'], output_dir, filename)
        user_replied = User(entry['replied_to']['user'], output_dir, filename)

        tweet_replier = Tweet(t, output_dir, filename)
        tweet_replied = Tweet(entry['replied_to'], output_dir, filename)

        Tweeted(user_replier.id, tweet_replier.id, output_dir, filename)
        Tweeted(user_replied.id, tweet_replied.id, output_dir, filename)

        Replied(tweet_replier.id, tweet_replied.id, output_dir, filename)
    elif entry['type'] == 'quote':
        user_quoter = User(t['user'], output_dir, filename)
        user_quoted = User(t['quoted_status']['user'], output_dir, filename)

        tweet_quoter = Tweet(t, output_dir, filename)
        tweet_quoted = Tweet(t['quoted_status'], output_dir, filename)

        Tweeted(user_quoter.id, tweet_quoter.id, output_dir, filename)
        Tweeted(user_quoted.id, tweet_quoted.id, output_dir, filename)

        Retweeted(tweet_quoter.id, tweet_quoted.id, output_dir, filename)
    elif entry['type'] == 'tweet':
        user = User(entry['tweet']['user'], output_dir, filename)

        tweet = Tweet(entry['tweet'], output_dir, filename)

        Tweeted(user.id, tweet.id, output_dir, filename)
    else:
        raise Exception(f'unknown entry type \'{entry["type"]}\' for watch')


def store_result(output_dir, entry, csv, jsonl, filename, start_time):
    '''save the result in as a .csv, .jsonl, janus .csv or print as json'''
    if csv:
        filename = f'{filename}_{start_time}.csv'
        save_as_csv(entry, output_dir, filename)
    elif jsonl:
        filename = f'{filename}_{start_time}.jsonl'
        save_as_jsonl(entry, output_dir, filename)
    else:
        print(json.dumps(entry, indent=4, sort_keys=True))


def save_as_csv(entry, output_dir, filename):
    '''Takes an entry as input and saves it in CSV format, optimized for janusgraph'''
    entry_type = entry.get('og_type', None)
    if entry_type == 'get_followers':
        followers_to_csv(entry, output_dir, filename)
    elif entry_type == 'get_profile':
        profile_to_csv(entry, output_dir, filename)
    elif entry_type == 'get_friends':
        friends_to_csv(entry, output_dir, filename)
    elif (entry_type == 'get_timeline' or
          entry_type == 'get_timeline_new' or
          entry_type == 'search'):
        timeline_to_csv(entry, output_dir, filename)
    elif entry_type == 'watch' or entry_type == 'search_new':
        stream_to_csv(entry, output_dir, filename)
    else:
        raise Exception('save_as_janus: entry type \'{}\' not supported'.format(entry_type))


def save_as_jsonl(entry, output_dir, jsonfile):
    '''Takes an entry as input and saves it in a JSON L file.'''
    fd = os.open(f'{output_dir}/{jsonfile}', os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
    os.write(fd, json.dumps(entry).encode('utf-8') + b'\n')
    os.close(fd)

