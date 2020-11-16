# Welcome to Openglass

Openglass is a tool to query various social network and search for different type
of information.

Openglass is in its first versions still. Please expect bugs.
If you want to contribute please do get in touch.

## Installing

```
sh
pip install openglass

```

Then run it with:

```sh
onionshar --help
```

## Run queries

To do a query on twitter use the `--twitter switch`:

```
--twitter
--search SEARCH QUERY
                           Specify the term to search
--timeline USERNAME OR ID
                           Specify the user to retrieve its timeline
--profile USERNAME OR ID
                           Specify the user to retrieve its profile
--followers USERNAME OR ID
                           Specify the user to retrieve its profile
```

Run a query with:

```
sh
openglass --config config.json --twitter --search "@twitter"
```

To query telegram you need to use the ``--telegram`` switch

```

--telegram
--channel-users CHANNEL ID
                           Query telegram channels and return its users in json
 --channel-messages CHANNEL ID
                           Query telegram channels and return its messages in json
```

To do a query in Telegram run:

```
sh
openglass --config config.json --telegram --channel-users "ChannelID"

```


The switch ` --csv ` prints result to a file called result.csv
