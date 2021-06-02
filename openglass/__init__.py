#! /usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import json
import uuid
import time
import glob
import argparse
import logging
import logging.handlers
import multiprocessing
from datetime import datetime
from .twitter import Twitter
from .telegram import Telegram
from .utility import Utility
from .output import writer


def main(cwd=None):
    """
    The main() function implements all of the logic that the command-line version of
    openglass uses.
    """

    utility = Utility()

    if utility.platform == "Darwin":
        if cwd:
            os.chdir(cwd)

    loggers = [logger for logger in logging.Logger.manager.loggerDict]
    for logger in loggers:
        logging.getLogger(logger).setLevel(logging.ERROR)

    f = logging.Formatter(fmt='%(asctime)s %(levelname)s: %(message)s')
    handlers = [
        logging.handlers.RotatingFileHandler('openglass.log',
                                             encoding='utf8',
                                             maxBytes=100000,
                                             backupCount=1),
        # logging.StreamHandler()
    ]
    root_logger = logging.getLogger()
    level = logging.ERROR
    root_logger.setLevel(level)
    for h in handlers:
        h.setFormatter(f)
        h.setLevel(level)
        root_logger.addHandler(h)

    # Parse arguments
    parser = argparse.ArgumentParser(
        formatter_class=lambda prog: argparse.HelpFormatter(prog, max_help_position=28)
    )
    parser.add_argument(
        "--config",
        metavar="FILENAME",
        required=True,
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
    type_group = parser.add_argument_group('mode')
    mxg_type = type_group.add_mutually_exclusive_group(required=False)
    mxg_type.add_argument(
        "--twitter",
        action='store_true',
        help="Query twitter endpoints",
    )
    mxg_type.add_argument(
        "--telegram",
        action='store_true',
        help="Query telegram enpoints",
    )
    twitter_actions = parser.add_argument_group('twitter actions')
    mxg_twitter = twitter_actions.add_mutually_exclusive_group(required=False)
    mxg_twitter.add_argument(
        "--search",
        metavar="TERM1 #HashTag",
        nargs='*',
        default=None,
        help="Specify the terms to search for old tweets",
    )
    mxg_twitter.add_argument(
        "--watch-search",
        metavar="USERNAME1 USERNAME2",
        nargs='*',
        default=None,
        help="Specify the terms to search for new tweets",
    )
    mxg_twitter.add_argument(
        "--watch-users",
        metavar="USERNAME1 USERNAME2",
        nargs='*',
        default=None,
        help="Specify the users to retrieve all their new tweets and their retweets",
    )
    mxg_twitter.add_argument(
        "--timeline",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve its past tweets",
    )
    mxg_twitter.add_argument(
        "--timeline-new",
        metavar="USERNAMES OR IDS",
        default=None,
        help="Specify the users to retrieve their new tweets",
    )
    mxg_twitter.add_argument(
        "--profile",
        metavar="USERNAME1 USERNAME2",
        nargs='*',
        default=None,
        help="Specify the users to retrieve their profile",
    )
    mxg_twitter.add_argument(
        "--followers",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve the users that follow him/her",
    )
    mxg_twitter.add_argument(
        "--friends",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve the users that is following",
    )
    mxg_twitter.add_argument(
        "--retweeters",
        metavar="TWEET ID",
        default=None,
        help="Specify the tweet to retrieve the users that retweeted it",
    )
    mxg_twitter.add_argument(
        "--retweeters-new",
        metavar="TWEETID1 TWEETID2",
        nargs='*',
        default=None,
        help="Specify the tweets to retrieve the new retweeters",
    )
    tweeter_params = parser.add_argument_group('twitter extra parameters')
    tweeter_params.add_argument(
        "--enrich",
        action='store_true',
        help="Specify if the tweets obtained should be enriched: get the 'repliad' and 'mentions' edges. It will take longer to run",
    )
    tweeter_params.add_argument(
        "--languages",
        metavar="LANGUAGE1 LANGUAGE2",
        nargs='*',
        default=None,
        help="Specify the list of languages you are interested in",
    )
    tweeter_params.add_argument(
        "--filter-level",
        choices=['none', 'low', 'medium'],
        default='none',
        help="Specify the filter_level of the tweets you wish to retreive",
    )
    telegram_actions = parser.add_argument_group('telegram actions')
    mxg_telegram = telegram_actions.add_mutually_exclusive_group(required=False)
    mxg_telegram.add_argument(
        "--channel-users",
        metavar="CHANNEL ID",
        default=None,
        help="Query telegram channels and return its users in json",
    )
    mxg_telegram.add_argument(
        "--channel-messages",
        metavar="CHANNEL ID",
        default=None,
        help="Query telegram channels and return its messages in json",
    )
    telegram_actions.add_argument(
        "--domains",
        action='store_true',
        help="Parse links"
    )
    telegram_actions.add_argument(
        "--channel-links",
        action='store_true',
        help="Parse links to telegram channels"
    )
    exit_group = parser.add_argument_group('exit options')
    exit_group.add_argument(
        "--run-for",
        metavar="TIME TO RUN",
        default=None,
        help="Specify for how long should openglass run. Example 100s, 5h, 3d",
    )
    exit_group.add_argument(
        "--max-results",
        metavar="NUMBER",
        type=int,
        default=None,
        help="Specify how many results max should openglass obtain",
    )
    output_group = parser.add_argument_group('output')
    mxg_output = output_group.add_mutually_exclusive_group(required=False)
    mxg_output.add_argument(
        "--csv",
        action='store_true',
        help="Stores results as csv",
    )
    mxg_output.add_argument(
        "--jsonl",
        action='store_true',
        help="Stores results as jsonl",
    )
    output_group.add_argument(
        "--output",
        metavar="DIRECTORY",
        default=os.getcwd(),
        help="Specify the directory in wich the output should be stored",
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

    if not os.path.isdir(args.output):
        print('the output directory does not exist')
        return

    if args.languages and (not args.watch_users and not args.watch_search):
        print('the --language option only works with --watch-users and --watch-search')

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

    queue = multiprocessing.Queue(1000)
    flag = multiprocessing.Value('i', 0)  # 0 keep running 1 for finish

    if args.twitter:

        t = Twitter(utility.get_setting('twitter_apis'))

        def entry_handler(obj, entry):
            nonlocal number_of_results
            number_of_results += 1
            if args.jsonl or args.csv:
                print('Number of results: {}'.format(number_of_results), end='\r')
            entry = standarize_entry(obj, entry)
            queue.put(entry)  # add the entry to the queue
            if args.run_for and time.time() - start_time > args.run_for:
                raise KeyboardInterrupt
            if args.max_results and number_of_results >= args.max_results:
                raise KeyboardInterrupt

        if bool(args.search):
            print('Press Ctrl-C to exit')
            filename = 'search_{}'.format('_'.join(args.search))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.search(args.search, entry_handler)
            except KeyboardInterrupt:
                pass
        if bool(args.watch_search) or bool(args.watch_users):
            print('Press Ctrl-C to exit')
            name = ''
            if bool(args.watch_search):
                name += '_'.join(args.watch_search)
            if bool(args.watch_users):
                name += '_'.join(args.watch_users)
            if bool(args.languages) is False:
                args.languages = None
            filename = 'watch_{}'.format(name)
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.watch(args.watch_users,
                        args.watch_search,
                        args.languages,
                        args.filter_level,
                        entry_handler)
            except KeyboardInterrupt:
                pass
        elif args.timeline:
            print('Press Ctrl-C to exit')
            filename = 'timeline_{}'.format(args.timeline.replace(' ', '_'))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.get_timeline(args.timeline, entry_handler, args.max_results)
            except KeyboardInterrupt:
                pass
        elif bool(args.timeline_new):
            print('Press Ctrl-C to exit')
            filename = 'timeline_new_{}'.format('_'.join(args.timeline_new))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.get_timeline_new(args.timeline_new, entry_handler)
            except KeyboardInterrupt:
                pass
        elif bool(args.profile):
            filename = 'profile_{}'.format('_'.join(args.profile))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            # profile is different from the rest as is not async
            for user in args.profile:
                profile = t.get_profile(user)
                entry_handler(t, profile)
        elif args.followers:
            print('Press Ctrl-C to exit')
            filename = 'followers_{}'.format(args.followers.replace(' ', '_'))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.get_followers(args.followers, entry_handler, args.max_results)
            except KeyboardInterrupt:
                pass
        elif args.friends:
            print('Press Ctrl-C to exit')
            filename = 'friends_{}'.format(args.friends.replace(' ', '_'))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.get_friends(args.friends, entry_handler, args.max_results)
            except KeyboardInterrupt:
                pass
        elif args.retweeters:
            print('Press Ctrl-C to exit')
            filename = 'retweeters_{}'.format(args.retweeters.replace(' ', '_'))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.get_retweeters(args.retweeters, entry_handler)
            except KeyboardInterrupt:
                pass
        elif bool(args.retweeters_new):
            print('Press Ctrl-C to exit')
            filename = 'retweeters_new_{}'.format('_'.join(args.retweeters_new))
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            try:
                t.get_retweeters_new(args.retweeters_new, entry_handler)
            except KeyboardInterrupt:
                pass

    if args.telegram:

        t = Telegram(utility.get_setting('telegram'))

        if args.channel_users:
            filename = args.channel_users.replace(' ', '_')
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            res = t.get_channel(args.channel_users)
        elif args.channel_messages:
            filename = args.channel_messages.replace(' ', '_')
            p = multiprocessing.Process(target=writer, args=(t, queue, flag, args, filename, start_time))
            p.start()
            res = t.get_messages(args.channel_messages)
            if args.search:
                res = search_dict(res, args.search)
            if args.domains:
                res = t.parse_domains(res)
            if args.channel_links:
                res = t.parse_channel_links(res)
        number_of_results += len(res)
        for entry in res:
            queue.put(entry)  # add the entry to the queue

    flag.value = 1
    p.join()

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
