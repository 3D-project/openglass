import argparse
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
        help="Print results as csv",
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
        "--timeline",
        metavar="USERNAME OR ID",
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
    domains = bool(args.domains)
    twitter = bool(args.twitter)
    timeline = bool(args.timeline)
    q_timeline = args.timeline
    search = bool(args.search)
    q_search = args.search
    profile = bool(args.profile)
    q_profile = args.profile
    followers = bool(args.followers)
    q_followers = args.followers
    telegram = bool(args.telegram)
    channel_users = bool(args.channel_users)
    q_channel_users = args.channel_users
    channel_links = bool(args.channel_links)
    channel_messages = bool(args.channel_messages)
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
            res = t.search(q_search)['statuses']
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_search, epoch_time))
            else:
                print(json.dumps(res, indent=4, sort_keys=True))
            sys.exit()
        elif timeline:
            res = t.get_statuses(q_timeline)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_timeline, epoch_time))
            else:
                print(json.dumps(res, indent=4, sort_keys=True))
            sys.exit()
        elif profile:
            res = t.get_statuses(q_profile)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_profile, epoch_time))
            else:
                print(json.dumps(res, indent=4, sort_keys=True))
            sys.exit()
        elif followers:
            res = t.get_followers(q_followers)
            if csv:
                save_as_csv(res, "{}-{}.csv".format(q_followers, epoch_time))
            else:
                print(json.dumps(res, indent=4, sort_keys=True))
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
            else:
                print(json.dumps(res, indent=4, sort_keys=True))
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
            else:
                print(json.dumps(res, indent=4, sort_keys=True, cls=DateTimeEncoder))
            sys.exit()

def save_as_csv(res_dict, csvfile):
    """
    Takes a list of dictionaries as input and outputs a CSV file.
    """
    with open(csvfile, 'w', newline='') as csvfile:
        fieldnames = res_dict[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,
                                         extrasaction='ignore', delimiter = ';')
        writer.writeheader()
        for r in res_dict:
            writer.writerow(r)

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
        if value in str(v): # found value
            return path
        elif hasattr(v, 'items'): # v is a dict
            p = getpath(v, value, path) # recursive call
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
