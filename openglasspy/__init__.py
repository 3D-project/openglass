import argparse
from datetime import date, datetime
import json
import sys

from .twitter import Twitter
from .telegram import Telegram
from .utility import Utility

def main(cwd=None):
    """
    The main() function implements all of the logic that the command-line version of
    openglasspy uses.
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
        help="Print openglasspy setting",
    )
    parser.add_argument(
        "--version",
        action='store_true',
        help="Print openglasspy version",
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


    args = parser.parse_args()

    version = bool(args.version)
    settings = bool(args.settings)
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
    channel_messages = bool(args.channel_messages)
    q_channel_messages = args.channel_messages
    config_filename = args.config

    if version:
        print(
            "Openglasspy version {}".format(utility.version)
        )
        sys.exit()

    if settings:
        if config_filename:
            print(
                "Openglasspy settings {}".format(utility.print_settings(config_filename))
            )
        else:
            print(
                "Openglasspy settings {}".format(utility.print_settings())
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
            data = t.search(q_search)['statuses']
            print(json.dumps(data, indent=4, sort_keys=True))
            sys.exit()
        elif timeline:
            res = t.get_statuses(q_timeline)
            print(json.dumps(res, indent=4, sort_keys=True))
            sys.exit()
        elif profile:
            res = t.get_statuses(q_profile)
            print(json.dumps(res, indent=4, sort_keys=True))
            sys.exit()
        elif followers:
            res = t.get_followers(q_followers)
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
            print(json.dumps(res, indent=4, sort_keys=True))
            sys.exit()
        elif channel_messages:
            res = t.get_messages(q_channel_messages)
            print(json.dumps(res, indent=4, sort_keys=True, cls=DateTimeEncoder))
            sys.exit()

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
