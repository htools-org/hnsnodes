# Hnsnodes

Hnsnodes is a fork of [Bitnodes](https://github.com/ayeowch/bitnodes) modified to work with the [Handshake](https://handshake.org/) network.

The default branch `handshake` is the one to be used. `master` is used to keep up with upstream changes.

To see what's changed, compare the branches: https://github.com/htools-org/hnsnodes/compare/master...handshake.

Not all features work, only `crawl.py` does. To use it,
```sh
# 1. Install python2 (preferably with a version manager like pyenv)
# 2. Install packages
pip install -r requirements.txt

# 3. Setup redis (if not localhost, change in utils.py)
docker run --name redis -p 6379:6379 -d redis

# 4. Register (free) and get an API key for geoip from https://www.maxmind.com
# and place it in `geoip/.maxmind_license_key`
# Then run this to download geoip db:
./geoip/update.sh

# 5. Create crawler config file from default and modify if needed
cp ./conf/crawl.conf.default ./conf/crawl.conf

# 6. Start crawler
python -u crawl.py conf/crawl.conf master

# It runs forever and keeps dumping the current list of reachable nodes
# in data/crawl/{timestamp}.json

# To pretty print as table or json objects, there's a script:
python scripts/parse-crawl-log.py       # help
python scripts/parse-crawl-log.py -t    # table
python scripts/parse-crawl-log.py -j    # json (array of objects)
python scripts/parse-crawl-log.py -jp   # json (prettified)
```

For more info, check out the [Bitnodes' Wiki](https://github.com/ayeowch/bitnodes/wiki/Provisioning-Bitcoin-Network-Crawler).

Huge thanks to bitnodes for creating and maintaining https://bitnodes.io/. Original readme below.

---

![Bitnodes](https://bitnodes.io/static/img/bitnodes-github.png "Bitnodes")

Bitnodes estimates the relative size of the Bitcoin peer-to-peer network by finding all of its reachable nodes. The current methodology involves sending [getaddr](https://en.bitcoin.it/wiki/Protocol_specification#getaddr) messages recursively to find all the reachable nodes in the network, starting from a set of seed nodes. Bitnodes uses Bitcoin protocol version 70001 (i.e. >= /Satoshi:0.8.x/), so nodes running an older protocol version will be skipped.

See [Provisioning Bitcoin Network Crawler](https://github.com/ayeowch/bitnodes/wiki/Provisioning-Bitcoin-Network-Crawler) for steps on setting up a machine to run Bitnodes. The [Redis Data](https://github.com/ayeowch/bitnodes/wiki/Redis-Data) contains the list of keys and their associated values that are written by the scripts in this project. If you wish to access the data, e.g. network snapshots, collected using this project, see [API](https://bitnodes.io/api/).

#### Links

* [Home](https://bitnodes.io/)

* [API](https://bitnodes.io/api/)

* [Network Snapshot](https://bitnodes.io/nodes/)

* [Charts](https://bitnodes.io/dashboard/)

* [Live Map](https://bitnodes.io/nodes/live-map/)

* [Network Map](https://bitnodes.io/nodes/network-map/)

* [Leaderboard](https://bitnodes.io/nodes/leaderboard/)

* [Client Status](https://bitnodes.io/dashboard/bitcoind/)

* [Combined Estimation](https://bitnodes.io/nodes/all/)

* [Check Your Node](https://bitnodes.io/#join-the-network)

* [What is a Bitcoin node?](https://bitnodes.io/what-is-a-bitcoin-node/)

#### CI

[![CircleCI](https://circleci.com/gh/ayeowch/bitnodes.svg?style=svg)](https://circleci.com/gh/ayeowch/bitnodes)
