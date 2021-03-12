import argparse
import os
import re
import csv
from datetime import date, datetime
import json
import sys
import time
from .twitter import Twitter
from .telegram import Telegram
from .utility import Utility


def main(cwd=None):
    """
    The main() function implements all of the logic that the command-line version of
    openglass uses.
    """

    epoch_time = int(time.time())

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
        "--json",
        action='store_true',
        help="Stores results as json",
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
        metavar="users IDs separated with spaces",
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
    json = bool(args.json)
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
        sys.exit()

    if settings:
        if config_filename:
            print(
                "Openglass settings {}".format(utility.print_settings(config_filename))
            )
        else:
            print(
                "Openglass settings {}".format(utility.print_settings())
            )
        sys.exit()

    if json and csv:
        parser.print_help()
        sys.exit()

    if not telegram and not twitter:
        parser.print_help()
        sys.exit()

    if telegram and twitter:
        parser.print_help()
        sys.exit()

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
        sys.exit()

    if (run_for and telegram) or (run_for and (search or timeline or profile or followers or retweeters)):
        parser.print_help()
        sys.exit()

    if run_for:
        if re.search(r'\d+[smhd]', q_run_for) is None:
            parser.print_help()
            sys.exit()
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

    # Re-load settings, if a custom config was passed in
    if config_filename:
        utility.load_settings(config_filename)
    else:
        utility.load_settings()

    if twitter:
        if config_filename:
            utility.load_settings(config_filename)
        else:
            utility.load_settings()

        t = Twitter(utility.get_setting('twitter_apis'))
        if search:
            res = t.search(q_search)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_search, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_search, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        if search_new:
            if not run_for:
                print('Press Ctrl-C to exit')
            csv_name = "{}-{}.csv".format(q_search_new.replace(' ', '_'), epoch_time)
            json_name = "{}-{}.json".format(q_search_new.replace(' ', '_'), epoch_time)
            res = []

            def save_result():
                if csv:
                    save_as_csv(res, csv_name)
                elif json:
                    save_as_json(res, json_name)
                else:
                    print_to_stdout(res)

            def callback(entry):
                res.append(entry)
                print('Number of results: {}'.format(len(res)), end='\r')
                if run_for and time.time() - epoch_time > q_run_for:
                    save_result()
                    sys.exit()
            print('Number of results: 0', end='\r')
            try:
                t.search_new(q_search_new, callback)
            except KeyboardInterrupt:
                save_result()
                sys.exit()
        elif timeline:
            res = t.get_timeline(q_timeline)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_timeline, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_timeline, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        elif timeline_new:
            if not run_for:
                print('Press Ctrl-C to exit')
            csv_name = "{}-{}.csv".format(q_timeline_new.replace(' ', '_'), epoch_time)
            json_name = "{}-{}.json".format(q_timeline_new.replace(' ', '_'), epoch_time)
            res = []

            def save_result():
                if csv:
                    save_as_csv(res, csv_name)
                elif json:
                    save_as_json(res, json_name)
                else:
                    print_to_stdout(res)

            def callback(entry):
                res.append(entry)
                print('Number of results: {}'.format(len(res)), end='\r')
                if run_for and time.time() - epoch_time > q_run_for:
                    save_result()
                    sys.exit()
            print('Number of results: 0', end='\r')
            try:
                t.get_timeline_new(q_timeline_new.split(' '), callback)
            except KeyboardInterrupt:
                save_result()
                sys.exit()
        elif profile:
            res = t.get_profile(q_profile)
            if csv:
                save_as_csv([res], "{}-{}.csv".format(q_profile, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_profile, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        elif followers:
            res = t.get_followers(q_followers)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_followers, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_followers, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        elif retweeters:
            res = t.get_retweeters(q_retweeters)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_retweeters, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_retweeters, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        elif retweeters_new:
            if not run_for:
                print('Press Ctrl-C to exit')
                q_run_for = None
            res = t.get_retweeters_new(q_retweeters_new.split(' '), q_run_for)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_retweeters_new, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_retweeters_new, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        elif watch_users:
            if not run_for:
                print('Press Ctrl-C to exit')
                q_run_for = None
            res = t.watch_users(q_watch_users, q_run_for)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_watch_users, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_watch_users, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()

    if telegram:
        if config_filename:
            utility.load_settings(config_filename)
        else:
            utility.load_settings()

        t = Telegram(utility.get_setting('telegram'))

        if channel_users:
            res = t.get_channel(q_channel_users)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_channel_users, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_channel_users, epoch_time))
            else:
                print_to_stdout(res)
            sys.exit()
        elif channel_messages:
            res = t.get_messages(q_channel_messages)
            if search:
                res = search_dict(res, q_search)
            if domains:
                res = t.parse_domains(res)
            if channel_links:
                res = t.parse_channel_links(res)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_channel_messages, epoch_time))
            elif json:
                save_as_json(res, "{}-{}.json".format(q_channel_messages, epoch_time))
            else:
                print(json.dumps(res, indent=4, sort_keys=True, cls=DateTimeEncoder))
            sys.exit()


def save_as_csv(res_dict, csvfile):
    """
    Takes a list of dictionaries as input and outputs a CSV file.
    """
    if len(res_dict) == 0:
        print('No results')
        return
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


def save_as_json(res_dict, jsonfile):
    """
    Takes a list of dictionaries as input and outputs a JSON file.
    """
    if len(res_dict) == 0:
        print('No results')
        return
    fh = open(jsonfile, 'w', newline='')
    fh.write(json.dumps(res_dict))
    fh.close()


def print_to_stdout(res_dict):
    if len(res_dict) == 0:
        print('No results')
        return
    print(json.dumps(res_dict, indent=4, sort_keys=True))


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
