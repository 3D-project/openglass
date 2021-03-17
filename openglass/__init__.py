import os
import re
import csv
import sys
import json
import uuid
import time
import aiohttp
import asyncio
import argparse
from datetime import date, datetime
from .twitter import Twitter
from .telegram import Telegram
from .utility import Utility


def main(cwd=None):
    """
    The main() function implements all of the logic that the command-line version of
    openglass uses.
    """

    start_time = int(time.time())
    now = datetime.now()
    print('started at: {}'.format(now.strftime("%B %d, %H:%M")))

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
        "--url",
        metavar="URL to send the results to",
        default=None,
        help="Sends the results to the specified URL",
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
        help="Specify the term to search",
    )
    parser.add_argument(
        "--search-new",
        metavar="SEARCH QUERY",
        default=None,
        help="Specify the term to search",
    )
    parser.add_argument(
        "--timeline",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve its timeline",
    )
    parser.add_argument(
        "--timeline-new",
        metavar="users IDs separated with spaces",
        default=None,
        help="Specify the user to retrieve its timeline",
    )
    parser.add_argument(
        "--profile",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve its profile",
    )
    parser.add_argument(
        "--followers",
        metavar="USERNAME OR ID",
        default=None,
        help="Specify the user to retrieve its profile",
    )
    parser.add_argument(
        "--retweeters",
        metavar="Tweet ID",
        default=None,
        help="Specify the Tweet id to retrieve the retweeters",
    )
    parser.add_argument(
        "--retweeters-new",
        metavar="Tweet IDs separated with spaces",
        default=None,
        help="Specify the Tweet ids to retrieve the new retweeters",
    )
    parser.add_argument(
        "--run-for",
        metavar="Amount of time",
        default=None,
        help="Specify for how long should openglass run. Example 100s, 5h, 3d",
    )
    parser.add_argument(
        "--watch-users",
        metavar="users NAMEs or IDs separated with spaces",
        default=None,
        help="Get all the tweets and their retweets of each watched user",
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

    version = bool(args.version)
    settings = bool(args.settings)
    csv = bool(args.csv)
    jsonl = bool(args.jsonl)
    url = bool(args.url)
    q_url = args.url
    domains = bool(args.domains)
    twitter = bool(args.twitter)
    timeline = bool(args.timeline)
    q_timeline = args.timeline
    timeline_new = bool(args.timeline_new)
    q_timeline_new = args.timeline_new
    search = bool(args.search)
    q_search = args.search
    search_new = bool(args.search_new)
    q_search_new = args.search_new
    profile = bool(args.profile)
    q_profile = args.profile
    followers = bool(args.followers)
    q_followers = args.followers
    retweeters = bool(args.retweeters)
    q_retweeters = args.retweeters
    retweeters_new = bool(args.retweeters_new)
    q_retweeters_new = args.retweeters_new
    watch_users = bool(args.watch_users)
    q_watch_users = args.watch_users
    telegram = bool(args.telegram)
    channel_users = bool(args.channel_users)
    q_channel_users = args.channel_users
    channel_links = bool(args.channel_links)
    channel_messages = bool(args.channel_messages)
    run_for = bool(args.run_for)
    q_run_for = args.run_for
    q_channel_messages = args.channel_messages
    config_filename = args.config

    if version:
        print(
            "Openglass version {}".format(utility.version)
        )
        return

    if settings:
        if config_filename:
            print(
                "Openglass settings {}".format(utility.print_settings(config_filename))
            )
        else:
            print(
                "Openglass settings {}".format(utility.print_settings())
            )
        return

    if not telegram and not twitter:
        parser.print_help()
        return

    if telegram and twitter:
        parser.print_help()
        return

    num_actions = 0
    if twitter:
        if search:
            num_actions += 1
        if search_new:
            num_actions += 1
        if timeline:
            num_actions += 1
        if timeline_new:
            num_actions += 1
        if profile:
            num_actions += 1
        if followers:
            num_actions += 1
        if retweeters:
            num_actions += 1
        if retweeters_new:
            num_actions += 1
        if watch_users:
            num_actions += 1

    if telegram:
        if channel_users:
            num_actions += 1
        if channel_messages:
            num_actions += 1

    if num_actions != 1:
        parser.print_help()
        return

    if run_for and telegram:
        parser.print_help()
        return

    if run_for:
        if re.search(r'^\d+[smhd]$', q_run_for) is None:
            parser.print_help()
            return
        amount = q_run_for[:-1]
        span = q_run_for[-1]
        if span == 's':
            q_run_for = int(amount)
        if span == 'm':
            q_run_for = int(amount) * 60
        if span == 'h':
            q_run_for = int(amount) * 60 * 60
        if span == 'd':
            q_run_for = int(amount) * 24 * 60 * 60
    else:
        q_run_for = None

    # Re-load settings, if a custom config was passed in
    if config_filename:
        utility.load_settings(config_filename)
    else:
        utility.load_settings()

    number_of_results = 0
    filename = ''

    if twitter:

        def entry_handler(obj, entry):
            nonlocal number_of_results
            number_of_results += 1
            if jsonl or csv or url:
                print('Number of results: {}'.format(number_of_results), end='\r')
            entry = standarize_entry(obj, entry)
            store_result([entry], csv, jsonl, url, q_url, filename, start_time)
            if run_for and time.time() - start_time > q_run_for:
                raise KeyboardInterrupt

        if config_filename:
            utility.load_settings(config_filename)
        else:
            utility.load_settings()

        t = Twitter(utility.get_setting('twitter_apis'))
        if search:
            print('Press Ctrl-C to exit')
            filename = 'search_{}'.format(q_search.replace(' ', '_'))
            try:
                t.search(q_search, entry_handler)
            except KeyboardInterrupt:
                pass
        if search_new:
            print('Press Ctrl-C to exit')
            filename = 'search_new_{}'.format(q_search_new.replace(' ', '_'))
            try:
                t.search_new(q_search_new, entry_handler)
            except KeyboardInterrupt:
                pass
        elif timeline:
            print('Press Ctrl-C to exit')
            filename = 'timeline_{}'.format(q_timeline.replace(' ', '_'))
            try:
                t.get_timeline(q_timeline, entry_handler)
            except KeyboardInterrupt:
                pass
        elif timeline_new:
            print('Press Ctrl-C to exit')
            filename = 'timeline_new_{}'.format(q_timeline_new.replace(' ', '_'))
            try:
                t.get_timeline_new(q_timeline_new.split(' '), entry_handler)
            except KeyboardInterrupt:
                pass
        elif profile:
            filename = 'profile_{}'.format(q_profile.replace(' ', '_'))
            profile = t.get_profile(q_profile)
            number_of_results += 1
            store_result([profile], csv, jsonl, url, q_url, filename, start_time)
        elif followers:
            print('Press Ctrl-C to exit')
            filename = 'followers_{}'.format(q_followers.replace(' ', '_'))
            try:
                t.get_followers(q_followers, entry_handler)
            except KeyboardInterrupt:
                pass
        elif retweeters:
            print('Press Ctrl-C to exit')
            filename = 'retweeters_{}'.format(q_retweeters.replace(' ', '_'))
            try:
                t.get_retweeters(q_retweeters, entry_handler)
            except KeyboardInterrupt:
                pass
        elif retweeters_new:
            print('Press Ctrl-C to exit')
            filename = 'retweeters_new_{}'.format(q_retweeters_new.replace(' ', '_'))
            try:
                t.get_retweeters_new(q_retweeters_new.split(' '), entry_handler)
            except KeyboardInterrupt:
                pass
        elif watch_users:
            print('Press Ctrl-C to exit')
            filename = 'watch_{}'.format(q_watch_users.replace(' ', '_'))
            try:
                t.watch_users(q_watch_users, entry_handler)
            except KeyboardInterrupt:
                pass

    if telegram:
        if config_filename:
            utility.load_settings(config_filename)
        else:
            utility.load_settings()

        t = Telegram(utility.get_setting('telegram'))

        if channel_users:
            res = t.get_channel(q_channel_users)
            filename = q_channel_users.replace(' ', '_')
        elif channel_messages:
            res = t.get_messages(q_channel_messages)
            if search:
                res = search_dict(res, q_search)
            if domains:
                res = t.parse_domains(res)
            if channel_links:
                res = t.parse_channel_links(res)
            filename = q_channel_messages.replace(' ', '_')

        store_result(res, csv, jsonl, url, q_url, filename, start_time)

    if number_of_results == 0:
        print('No results')
        return

    if jsonl:
        filename = "{}-{}.jsonl".format(filename, start_time)
    elif csv:
        filename = "{}-{}.csv".format(filename, start_time)
    if jsonl or csv:
        print('\n[+] created {}'.format(filename))


def store_result(data, csv, jsonl, url, q_url, filename, start_time):
    '''save the result in as a .csv, .jsonl or print as json'''
    if csv:
        filename = "{}-{}.csv".format(filename, start_time)
        save_as_csv(data, filename)
    if jsonl:
        filename = "{}-{}.jsonl".format(filename, start_time)
        save_as_jsonl(data, filename)
    if url:
        asyncio.run(send_to_url(data, q_url))
    if not csv and not jsonl and not url:
        for elem in data:
            print(json.dumps(elem, indent=4, sort_keys=True))


async def send_to_url(data, url):
    timeout = aiohttp.ClientTimeout(sock_read=0.001)
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, json=data, timeout=timeout) as resp:
                pass
        except aiohttp.client_exceptions.ClientConnectorError:
            pass
        except aiohttp.client_exceptions.ServerDisconnectedError:
            pass
        except asyncio.exceptions.TimeoutError:
            pass


def save_as_csv(res_dict, csvfile):
    """
    Takes a list of dictionaries as input and outputs a CSV file.
    """
    if not os.path.isfile(csvfile):
        csvfile = open(csvfile, 'w', newline='')
        fieldnames = res_dict[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,
                                extrasaction='ignore', delimiter=';')
        writer.writeheader()
    else:
        csvfile = open(csvfile, 'a', newline='')
        fieldnames = res_dict[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,
                                extrasaction='ignore', delimiter=';')

    for r in res_dict:
        writer.writerow(r)

    csvfile.close()


def save_as_jsonl(res_dict, jsonfile):
    """
    Takes a list of dictionaries as input and outputs a JSON file.
    """
    with open(jsonfile, 'a') as fh:
        fh.write('\n'.join([json.dumps(line) for line in res_dict]) + '\n')


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
