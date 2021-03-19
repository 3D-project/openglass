.. OpenGlass documentation master file, created by
   sphinx-quickstart on Tue Feb  9 13:30:51 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to OpenGlass's documentation!
=====================================

.. toctree::
   :maxdepth: 2
   :caption: Contents:

OpenGlass is a tool to query various social network and search for different
types of information.

At this moment it is in alpha version and supports limited search functionalities
for Twitter_ and Telegram_.

OpenGlass was designed as an OSINT_ tool to identify and discover
disinformation campaigns.


.. _Twitter: https://www.twitter.com
.. _Telegram: https://www.telegram.org
.. _OSINT: https://en.wikipedia.org/wiki/Open-source_intelligence

=================
 Getting Started
=================

OpenGlass can be installed via pypi by running: ::

  $  pip install openglass

Then you can run it with: ::

  $  openglass --help

------------------------------
 Setup the configuration file
------------------------------

OpenGlass uses a config file in JSON format. A config_ example file is provided
in the code repository.

::

  {
    "twitter_apis": [
      {
        "CONSUMER_KEY": "CONSUMER_KEY",
        "CONSUMER_SECRET": "CONSUMER_SECRET",
        "ACCESS_KEY": "ACCESS_KEY",
        "ACCESS_SECRET": "ACCESS_SECRET"
      }
    ],
    "telegram": {
      "phone": "PHONE_NUMBER",
      "username": "USERNAME",
      "app_id": "APP_ID",
      "api_hash": "API_HASH",
      "servers": {
        "test": "149.154.167.40:443",
        "production": "149.154.167.50:443"
      }
    }
  }

To obtain API keys from Twitter_ and Telegram_ check their respective api docs:

* `Telegram api`_

* `Twitter api`_

.. _config: https://github.com/hiromipaw/openglass/blob/master/config.json.example
.. _`Telegram api`: https://core.telegram.org/api/obtaining_api_id
.. _`Twitter api`: https://developer.twitter.com/en/docs/twitter-api

-------------
 Run queries
-------------

Telegram
~~~~~~~~

To query telegram you need to use the ``--telegram`` switch

::

  --telegram
  --channel-users CHANNEL ID
                             Query telegram channels and return its users in json
  --channel-messages CHANNEL ID
                             Query telegram channels and return its messages in json
  --domains                  Parse links
  --channel-links            Parse links to telegram channels



To do a query in Telegram run:

::

  $  openglass --config config.json --telegram --channel-users "ChannelID"

More specific queries are possible. Specifically the ``--domains`` switch returns
a list of high level domains (or FQDN_).

The ``--channel-links`` switch returns a list of shared telegram channels.

These two switch are important if you want to know which links are shared in a
Telegram_ channel and which other channels are cross referenced.

.. _FQDN: https://en.wikipedia.org/wiki/Fully_qualified_domain_name


Twitter
~~~~~~~~

To do a query on twitter use the ``--twitter`` switch:

::

  --twitter
  --search SEARCH QUERY
                             Specify the terms to search for old tweets
  --search-new SEARCH QUERY
                             Specify the terms to search for new tweets
  --timeline USERNAME OR ID
                             Specify the user to retrieve its past tweets
  --timeline-new USERNAMES OR IDS
                             Specify the users to retrieve their new tweets
  --profile USERNAME OR ID
                             Specify the user to retrieve its profile
  --followers USERNAME OR ID
                             Specify the user to retrieve the users that follow him/her
  --friends USERNAME OR ID
                             Specify the user to retrieve the users that is following
  --retweeters TWEET ID
                             Specify the tweet to retrieve the users that retweeted it
  --retweeters-new TWEET IDS
                             Specify the tweets to retrieve the new retweeters
  --watch USERNAMES OR IDS
                             Specify the users to retrieve all their new tweets and their retweets

Run a query with:

::

  $  openglass --config config.json --twitter --search "search query"


If you want to contribute please do get in touch on Github_.


.. _Github: https://github.com/hiromipaw/openglass

-------------
 Save output
-------------

The switch ``--csv`` saves result to a file called {query_type}_{query}_{timestamp}.csv

The switch ``--jsonl`` saves result to a file called {query_type}_{query}_{timestamp}.jsonl

-------------
 Timeout
-------------

The switch ``--run-for`` specifies for how long should OpenGlass run. Seconds, minutes, hours or days can be specified.
