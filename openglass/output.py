import os
import sys
import csv
import json

users_saved = []
tweets_saved = []


class User:
    def __init__(self, json_entry):
        self.header = 'id,name,screen_name,location,description,protected,followers_count,friends_count,listed_count,statuses_count,created_at,favourites_count,verified,profile_use_background_image,has_extended_profile,default_profile,default_profile_image'
        self.id = json_entry['id']
        self.name = json_entry['name']
        self.screen_name = json_entry['screen_name']
        self.location = json_entry.get('location', None)
        self.description = json_entry.get('description', '').replace('"', '""')
        self.protected = json_entry.get('protected', False)
        self.followers_count = json_entry.get('followers_count', None)
        self.friends_count = json_entry.get('friends_count', None)
        self.listed_count = json_entry.get('listed_count', None)
        self.statuses_count = json_entry.get('statuses_count', None)
        self.created_at = json_entry.get('created_at', None)
        self.favourites_count = json_entry.get('favorites_count', None)
        self.verified = json_entry.get('verified', False)
        self.profile_use_background_image = json_entry.get('profile_use_background_image', None)
        self.has_extended_profile = json_entry.get('has_extended_profile', None)
        self.default_profile = json_entry.get('default_profile', None)
        self.default_profile_image = json_entry.get('default_profile_image', None)

    def to_entry(self):
        entry = ''
        entry += f'{self.id},'
        entry += f'{self.name},'
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

    def save_to_file(self, filename):
        global users_saved
        filename = f'users_{filename}'
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
    def __init__(self, json_entry):
        self.header ='id,text,truncated,is_quote_status,retweet_count,favorite_count,possibly_sensitive,lang'
        self.id = json_entry['id']
        self.text = json_entry['text'].replace('"', '""')
        self.truncated = json_entry.get('truncated', False)
        self.in_reply_to_status_id = json_entry.get('in_reply_to_user_id', None) # this should be a relation
        self.in_reply_to_user_id = json_entry.get('in_reply_to_user_id', None)  # this should be a relation
        self.is_quote_status = json_entry.get('is_quote_status', None)
        self.retweet_count = json_entry.get('retweet_count', None)
        self.favorite_count = json_entry.get('favorite_count', None)
        self.possibly_sensitive = json_entry.get('possibly_sensitive', None)
        self.lang = json_entry.get('lang', None)

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

    def save_to_file(self, filename):
        global tweets_saved
        filename = f'tweets_{filename}'
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
    def __init__(self, follower_id, followed_id):
        self.header = 'follower_id,followed_id'
        self.follower_id = follower_id
        self.followed_id = followed_id

    def to_entry(self):
        entry = ''
        entry += f'{self.follower_id},'
        entry += f'{self.followed_id}'
        return entry

    def save_to_file(self, filename):
        filename = f'follows_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


class Followed:
    def __init__(self, followed_id, follower_id):
        self.header = 'followed_id,follower_id'
        self.followed_id = followed_id
        self.follower_id = follower_id

    def to_entry(self):
        entry = ''
        entry += f'{self.followed_id},'
        entry += f'{self.follower_id}'
        return entry

    def save_to_file(self, filename):
        filename = f'followed_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


class Tweeted:
    def __init__(self, user_id, tweet_id):
        self.header = 'user_id,tweet_id'
        self.user_id = user_id
        self.tweet_id = tweet_id

    def to_entry(self):
        entry = ''
        entry += f'{self.user_id},'
        entry += f'{self.tweet_id}'
        return entry

    def save_to_file(self, filename):
        filename = f'tweeted_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


class Retweeted:
    def __init__(self, user_id, tweet_id):
        self.header = 'user_id,tweet_id'
        self.user_id = user_id
        self.tweet_id = tweet_id

    def to_entry(self):
        entry = ''
        entry += f'{self.user_id},'
        entry += f'{self.tweet_id}'
        return entry

    def save_to_file(self, filename):
        filename = f'retweeted_{filename}'
        store_header = not os.path.isfile(filename)
        fd = os.open(filename, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
        if store_header:
            os.write(fd, self.header.encode('utf-8') + b'\n')
        os.write(fd, self.to_entry().encode('utf-8') + b'\n')
        os.close(fd)


def followers(entry, filename):
    user = User(entry)
    user.save_to_file(filename)

    relation = Follows(user.id, entry['follows'])
    relation.save_to_file(filename)


def profile(entry, filename):
    user = User(entry)
    user.write_to_file(filename)


def friends(entry, filename):
    user = User(entry)
    user.save_to_file(filename)

    relation = Followed(user.id, entry['is_followed_by'])
    relation.save_to_file(filename)


def timeline(entry, filename):
    tweet = Tweet(entry)
    tweet.save_to_file(filename)

    user = User(entry['user'])
    user.save_to_file(filename)

    relation = Tweeted(user.id, tweet.id)
    relation.save_to_file(filename)


def watch(entry, filename):
    if entry['type'] == 'retweet':
        user_retweeter = User(entry['user'])
        user_retweeted = User(entry['retweeted_status']['user'])
        user_retweeter.save_to_file(filename)
        user_retweeted.save_to_file(filename)

        tweet_retweeted = Tweet(entry['retweeted_status'])
        # tweet_retweeter = Tweet(entry)
        tweet_retweeted.save_to_file(filename)

        retweeted = Retweeted(user_retweeter.id, tweet_retweeted.id)
        retweeted.save_to_file(filename)
    elif entry['type'] == 'old_tweet' or entry['type'] == 'new_tweet':
        user = User(entry['tweet']['user'])
        user.save_to_file(filename)

        tweet = Tweet(entry['tweet'])
        tweet.save_to_file(filename)

        tweeted = Tweeted(user.id, tweet.id)
        tweeted.save_to_file(filename)
    else:
        raise Exception(f'unknown entry type \'{entry["type"]}\' for watch')


def store_result(entry, csv, jsonl, janus, filename, start_time):
    '''save the result in as a .csv, .jsonl, janus .csv or print as json'''
    if csv:
        filename = "{}_{}.csv".format(filename, start_time)
        save_as_csv(entry, filename)
    elif jsonl:
        filename = "{}_{}.jsonl".format(filename, start_time)
        save_as_jsonl(entry, filename)
    elif janus:
        filename = "{}_{}.csv".format(filename, start_time)
        save_as_janus(entry, filename)
    else:
        print(json.dumps(entry, indent=4, sort_keys=True))


def save_as_janus(entry, filename):
    entry_type = entry['og_type']
    if entry_type == 'get_followers':
        followers(entry, filename)
    elif entry_type == 'get_profile':
        profile(entry, filename)
    elif entry_type == 'get_friends':
        friends(entry, filename)
    elif (entry_type == 'get_timeline' or
          entry_type == 'get_timeline_new' or
          entry_type == 'search' or
          entry_type == 'search_new'):
        timeline(entry, filename)
    elif entry_type == 'watch':
        watch(entry, filename)
    else:
        raise Exception('save_as_janus: entry type \'{}\' not supported'.format(entry_type))


def save_as_csv(entry, csvfile):
    """
    Takes a list of dictionaries as input and outputs a CSV file.
    """
    if not os.path.isfile(csvfile):
        csvfile = open(csvfile, 'w', newline='')
        fieldnames = entry.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,
                                extrasaction='ignore', delimiter=',')
        writer.writeheader()
    else:
        csvfile = open(csvfile, 'a', newline='')
        fieldnames = entry.keys()
        writer = csv.DictWriter(
            csvfile, fieldnames=fieldnames,
            extrasaction='ignore', delimiter=',')

    writer.writerow(entry)

    csvfile.close()


def save_as_jsonl(entry, jsonfile):
    """
    Takes an entry as input and saves it in a JSON L file.
    """
    # use os module to increase speed
    fd = os.open(jsonfile, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
    os.write(fd, json.dumps(entry).encode('utf-8') + b'\n')
    os.close(fd)

