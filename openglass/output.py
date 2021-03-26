import os
import sys
import csv
import json


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


def followers(entry, filename):
    userfile = f'users_{filename}'
    store_header = not os.path.isfile(userfile)

    fd = os.open(userfile, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
    user = User(entry)
    if store_header:
        os.write(fd, user.header.encode('utf-8') + b'\n')
    os.write(fd, user.to_entry().encode('utf-8') + b'\n')
    os.close(fd)

    relationsfile = f'relations_{filename}'
    store_header = not os.path.isfile(relationsfile)

    fd = os.open(relationsfile, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
    relation = Follows(user.id, entry['follows'])
    if store_header:
        os.write(fd, relation.header.encode('utf-8') + b'\n')
    os.write(fd, relation.to_entry().encode('utf-8') + b'\n')
    os.close(fd)


def profile(entry, filename):
    userfile = f'users_{filename}'
    store_header = not os.path.isfile(userfile)

    fd = os.open(userfile, os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o660)
    user = User(entry)
    if store_header:
        os.write(fd, user.header.encode('utf-8') + b'\n')
    os.write(fd, user.to_entry().encode('utf-8') + b'\n')
    os.close(fd)


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
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore', delimiter=',')

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

