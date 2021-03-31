#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import uuid
import time
import glob
import argparse
from datetime import datetime
from .twitter import Twitter
from .telegram import Telegram
from .utility import Utility
from .output import store_result


def main(cwd=None):
    """
    The main() function implements all of the logic that the command-line version of
    openglass uses.
    """

    utility = Utility()

    if utility.platform == "Darwin":
        if cwd:
            os.chdir(cwd)

    # Parse arguments
    parser = argparse.ArgumentParser(
        formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=28)
    )

    parser.add_argument(
        "--config",
        metavar="FILENAME",
        default=None,
        help="Filename of custom global settings",
    )
    parser.add_argument(
        "--settings",
        action='store_true',
        help="Print openglass setting",
    )
    parser.add_argument(
        "--version",
        action='store_true',
        help="Print openglass version",
    )
    parser.add_argument(
        "--csv",
        action='store_true',
        help="Stores results as csv",
    )
    parser.add_argument(
        "--jsonl",
        action='store_true',
        help="Stores results as jsonl",
    )
    parser.add_argument(
        "--output",
        metavar="output directory",
        help="Specify the directory in wich the output should be stored",
    )
    parser.add_argument(
        "--twitter",
        action='store_true',
        help="Query the twitter endpoints. Use search to search, timeline for user's statuses, profile for user's profile",
    )
    parser.add_argument(
        "--search",
        metavar="SEARCH QUERY",
        default=None,
        help="Specify the terms to search for old tweets",
    )
    parser.add_argument(
        "--watch-search",
        metavar="SEARCH QUERY",
        default=None,
        help="Specify the terms to search for new tweets (can be used with --watch-users)",
    )
    parser.add_argument(
        "--timeline",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve its past tweets",
    )
    parser.add_argument(
        "--timeline-new",
        metavar="USERNAMES OR IDS",
        default=None,
        help="Specify the users to retrieve their new tweets",
    )
    parser.add_argument(
        "--profile",
        metavar="USERNAMES OR IDS",
        default=None,
        help="Specify the users to retrieve their profile",
    )
    parser.add_argument(
        "--followers",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve the users that follow him/her",
    )
    parser.add_argument(
        "--friends",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve the users that is following",
    )
    parser.add_argument(
        "--retweeters",
        metavar="TWEET ID",
        default=None,
        help="Specify the tweet to retrieve the users that retweeted it",
    )
    parser.add_argument(
        "--retweeters-new",
        metavar="TWEET IDS",
        default=None,
        help="Specify the tweets to retrieve the new retweeters",
    )
    parser.add_argument(
        "--watch-users",
        metavar="USERNAMES OR IDS",
        default=None,
        help="Specify the users to retrieve all their new tweets and their retweets (can be used with --watch-search)",
    )
    parser.add_argument(
        "--run-for",
        metavar="Amount of time",
        default=None,
        help="Specify for how long should openglass run. Example 100s, 5h, 3d",
    )
    parser.add_argument(
        "--max-results",
        metavar="Max number of results",
        default=None,
        help="Specify how many results max should openglass obtain",
    )
    parser.add_argument(
        "--telegram",
        action='store_true',
        help="Query telegram enpoints",
    )
    parser.add_argument(
        "--channel-users",
        metavar="CHANNEL ID",
        default=None,
        help="Query telegram channels and return its users in json",
    )
    parser.add_argument(
        "--channel-messages",
        metavar="CHANNEL ID",
        default=None,
        help="Query telegram channels and return its messages in json",
    )
    parser.add_argument(
        "--domains",
        action='store_true',
        help="Parse links"
    )
    parser.add_argument(
        "--channel-links",
        action='store_true',
        help="Parse links to telegram channels"
    )

    args = parser.parse_args()

    if args.version:
        print(
            "Openglass version {}".format(utility.version)
        )
        return

    if args.settings:
        if args.config:
            print(
                "Openglass settings {}".format(utility.print_settings(args.config))
            )
        else:
            print(
                "Openglass settings {}".format(utility.print_settings())
            )
        return

    file_outputs = ['csv', 'jsonl']
    num_output = sum([1 for elem in file_outputs if getattr(args, elem) is True])
    if num_output > 1:
        print('decide between --csv and --jsonl')
        return

    if num_output == 0 and args.output is not None:
        print('to select an output directory, supply --csv or --jsonl')
        return

    if args.output is not None:
        if not os.path.isdir(args.output):
            print('the output directory does not exist')
            return
    else:
        args.output = os.getcwd()

    if not args.telegram and not args.twitter:
        print('supply --twitter or --telegram')
        return

    if args.telegram and args.twitter:
        print('decide between --twitter and --telegram')
        return

    twitter_actions = ['search', 'watch_search', 'timeline', 'timeline_new', 'profile', 'followers', 'friends', 'retweeters', 'retweeters_new', 'watch_users']
    num_actions = sum([1 for elem in twitter_actions if getattr(args, elem) is not None])

    if args.twitter and num_actions != 1:
        if not (num_actions == 2 and args.watch_search and args.watch_users):
            print('select one twitter action')
            return
    if args.telegram and num_actions != 0:
        print('twitter options are not allowded with --telegram')

    telegram_actions = ['channel_users', 'channel_messages']
    num_actions = sum([1 for elem in telegram_actions if getattr(args, elem) is not None])

    if args.telegram and num_actions != 1:
        print('select one telegram action')
        return
    if args.twitter and num_actions != 0:
        print('telegram options are not allowded with --twitter')
        return

    if args.run_for and args.telegram:
        print('--run-for can only be used with --twitter')
        return

    if args.run_for:
        if re.search(r'^\d+[smhd]$', args.run_for) is None:
            print(f'invalid format for --run-for: {args.run_for}')
            return
        amount = args.run_for[:-1]
        span = args.run_for[-1]
        if span == 's':
            args.run_for = int(amount)
        if span == 'm':
            args.run_for = int(amount) * 60
        if span == 'h':
            args.run_for = int(amount) * 60 * 60
        if span == 'd':
            args.run_for = int(amount) * 24 * 60 * 60
    else:
        args.run_for = None

    if args.max_results:
        try:
            args.max_results = int(args.max_results)
        except ValueError:
            print(f'invalid value for --max-results: {args.max_results}')
            return

    # Re-load settings, if a custom config was passed in
    if args.config:
        utility.load_settings(args.config)
    else:
        utility.load_settings()

    start_time = int(time.time())
    now = datetime.now()
    print('started at: {}'.format(now.strftime("%B %d, %H:%M")))

    number_of_results = 0
    filename = ''

    if args.twitter:

        def entry_handler(obj, entry):
            nonlocal number_of_results
            number_of_results += 1
            if args.jsonl or args.csv:
                print('Number of results: {}'.format(number_of_results), end='\r')
            entry = standarize_entry(obj, entry)
            store_result(args.output, entry, args.csv, args.jsonl, filename, start_time)
            if args.run_for and time.time() - start_time > args.run_for:
                raise KeyboardInterrupt
            if args.max_results and number_of_results >= args.max_results:
                raise KeyboardInterrupt

        if args.config:
            utility.load_settings(args.config)
        else:
            utility.load_settings()

        t = Twitter(utility.get_setting('twitter_apis'))
        if args.search:
            print('Press Ctrl-C to exit')
            filename = 'search_{}'.format(args.search.replace(' ', '_'))
            try:
                t.search(args.search, entry_handler)
            except KeyboardInterrupt:
                pass
        if args.watch_search or args.watch_users:
            print('Press Ctrl-C to exit')
            name = ''
            if args.watch_search:
                name += args.watch_search.replace(' ', '_')
                args.watch_search = args.watch_search.split(' ')
            if args.watch_users:
                name += args.watch_users.replace(' ', '_')
                args.watch_users = args.watch_users.split(' ')
            filename = 'watch_{}'.format(name)
            try:
                t.watch(args.watch_users, args.watch_search, entry_handler)
            except KeyboardInterrupt:
                pass
        elif args.timeline:
            print('Press Ctrl-C to exit')
            filename = 'timeline_{}'.format(args.timeline.replace(' ', '_'))
            try:
                t.get_timeline(args.timeline, entry_handler, args.max_results)
            except KeyboardInterrupt:
                pass
        elif args.timeline_new:
            print('Press Ctrl-C to exit')
            filename = 'timeline_new_{}'.format(args.timeline_new.replace(' ', '_'))
            try:
                t.get_timeline_new(args.timeline_new.split(' '), entry_handler)
            except KeyboardInterrupt:
                pass
        elif args.profile:
            # profile is different from the rest as is not async
            filename = 'profile_{}'.format(args.profile.replace(' ', '_'))
            users = args.profile.split(' ')
            for user in users:
                profile = t.get_profile(user)
                entry_handler(t, profile)
        elif args.followers:
            print('Press Ctrl-C to exit')
            filename = 'followers_{}'.format(args.followers.replace(' ', '_'))
            try:
                t.get_followers(args.followers, entry_handler, args.max_results)
            except KeyboardInterrupt:
                pass
        elif args.friends:
            print('Press Ctrl-C to exit')
            filename = 'friends_{}'.format(args.friends.replace(' ', '_'))
            try:
                t.get_friends(args.friends, entry_handler, args.max_results)
            except KeyboardInterrupt:
                pass
        elif args.retweeters:
            print('Press Ctrl-C to exit')
            filename = 'retweeters_{}'.format(args.retweeters.replace(' ', '_'))
            try:
                t.get_retweeters(args.retweeters, entry_handler)
            except KeyboardInterrupt:
                pass
        elif args.retweeters_new:
            print('Press Ctrl-C to exit')
            filename = 'retweeters_new_{}'.format(args.retweeters_new.replace(' ', '_'))
            try:
                t.get_retweeters_new(args.retweeters_new.split(' '), entry_handler)
            except KeyboardInterrupt:
                pass

    if args.telegram:
        if args.config:
            utility.load_settings(args.config)
        else:
            utility.load_settings()

        t = Telegram(utility.get_setting('telegram'))

        if args.channel_users:
            res = t.get_channel(args.channel_users)
            filename = args.channel_users.replace(' ', '_')
        elif args.channel_messages:
            res = t.get_messages(args.channel_messages)
            if args.search:
                res = search_dict(res, args.search)
            if args.domains:
                res = t.parse_domains(res)
            if args.channel_links:
                res = t.parse_channel_links(res)
            filename = args.channel_messages.replace(' ', '_')
        for entry in res:
            store_result(args.output, entry, args.csv, args.jsonl, filename, start_time)

    if number_of_results == 0:
        print('No results')
        return

    files = glob.glob(f'{args.output}/*{start_time}*')
    print('')
    for filename in files:
        print('[+] created {}'.format(filename))


def search_dict(res_dict, query_value):
    filtered_dict = []
    for r in res_dict:
        path = getpath(r, query_value)
        if path:
            filtered_dict.append(r)
    return filtered_dict


def getpath(nested_dict, value, prepath=()):
    for k, v in nested_dict.items():
        path = prepath + (k,)
        if value in str(v):  # found value
            return path
        elif hasattr(v, 'items'):  # v is a dict
            p = getpath(v, value, path)  # recursive call
            if p is not None:
                return p


def delete_unsued_keys(entry):
    unused_keys = ['id_str', 'profile_background_color', 'profile_link_color', 'profile_sidebar_border_color', 'profile_sidebar_fill_color', 'profile_text_color', 'favorited', 'filter_level']
    dict_keys = []
    keys_to_delete = []
    for key in entry:
        if key in unused_keys:
            keys_to_delete.append(key)
        elif type(entry[key]) == dict:
            dict_keys.append(key)
    for k in keys_to_delete:
        del entry[k]
    for k in dict_keys:
        delete_unsued_keys(entry[k])


def standarize_entry(obj, entry):
    '''
    remove unused entrys and add important fields
    a unique id 'og_id', a search id 'og_search_id'
    a timestamp 'og_timestamp' and a type 'og_type'
    '''

    delete_unsued_keys(entry)
    entry['og_id'] = str(uuid.uuid4())
    entry['og_search_id'] = obj.search_id
    entry['og_timestamp'] = int(time.time())
    entry['og_type'] = obj.type
    return entry


# some functions to parse json date
class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()

        if isinstance(o, bytes):
            return list(o)

        return json.JSONEncoder.default(self, o)


if __name__ == "__main__":
    main()
