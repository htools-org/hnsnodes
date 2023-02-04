#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# crawl.py - Greenlets-based Bitcoin network crawler.
#
# Copyright (c) Addy Yeow <ayeowch@gmail.com>
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
Greenlets-based Bitcoin network crawler.
"""

from gevent import monkey
monkey.patch_all()

import geoip2.database
import gevent
import json
import logging
import os
import random
import redis
import redis.connection
import socket
import sys
import time
from binascii import hexlify
from binascii import unhexlify
from collections import Counter
from configparser import ConfigParser
from geoip2.errors import AddressNotFoundError
from ipaddress import ip_address
from ipaddress import ip_network

from protocol import Connection
from protocol import ConnectionError
from protocol import ProtocolError
from protocol import TO_SERVICES
from utils import get_keys
from utils import http_get_txt
from utils import ip_to_network
from utils import new_redis_conn

redis.connection.socket = gevent.socket

CONF = {}

# MaxMind databases
ASN = geoip2.database.Reader('geoip/GeoLite2-ASN.mmdb')


def getaddr(conn):
    """
    Sends getaddr message.
    """
    addr_msgs = []
    try:
        conn.getaddr(block=False)
    except (ProtocolError, ConnectionError, socket.error) as err:
        logging.debug(f'{conn.to_addr}: {err}')
    else:
        addr_wait = 0
        while addr_wait < CONF['socket_timeout']:
            addr_wait += 1
            gevent.sleep(0.3)
            try:
                msgs = conn.get_messages(commands=[b'addr', b'addrv2'])
            except (ProtocolError, ConnectionError, socket.error) as err:
                logging.debug(f'{conn.to_addr}: {err}')
                break
            if msgs and any([msg['count'] > 1 for msg in msgs]):
                addr_msgs = msgs
                break
    return addr_msgs


def get_peers(conn):
    """
    Returns included peering nodes with age <= max. age.
    """
    now = int(time.time())
    peers = set()
    excluded_count = 0

    addr_msgs = getaddr(conn)

    for addr_msg in addr_msgs:
        if 'addr_list' not in addr_msg:
            continue

        for peer in addr_msg['addr_list']:
            timestamp = peer['timestamp']
            age = now - timestamp  # seconds
            if age < 0 or age > CONF['max_age']:
                continue
            address = peer['ipv4'] or peer['ipv6'] or peer['onion']
            port = peer['port'] if peer['port'] > 0 else CONF['port']
            services = peer['services']
            if not address:
                continue
            if is_excluded(address):
                logging.debug(f'Exclude: ({address}, {port})')
                excluded_count += 1
                continue
            peers.add((address, port, services, timestamp))

    logging.debug(f'{conn.to_addr} '
                  f'Peers: {len(peers)} (Excluded: {excluded_count})')

    # Reject peers if hard limit is hit.
    if len(peers) > 1000:
        logging.warning(f'{conn.to_addr} peers rejected')
        peers = set()
    peers = list(peers)[:CONF['peers_per_node']]
    return peers


def get_cached_peers(conn, redis_conn):
    """
    Returns cached peering nodes.
    """
    key = f'peer:{conn.to_addr[0]}-{conn.to_addr[1]}'
    peers = redis_conn.get(key)
    if peers:
        peers = eval(peers)
        logging.debug(f'{conn.to_addr} Peers: {len(peers)}')
    else:
        peers = get_peers(conn)
        ttl = CONF['addr_ttl']
        ttl += random.randint(0, CONF['addr_ttl_var']) / 100.0 * ttl
        redis_conn.setex(key, int(ttl), str(peers))

    # Exclude timestamp from the tuples.
    peers = set([
        (address, port, services)
        for (address, port, services, timestamp) in peers])
    return peers


def connect(key, redis_conn):
    """
    Establishes connection with a node to:
    1) Send version message
    2) Receive version and verack message
    3) Send getaddr message
    4) Receive addr message containing list of peering nodes
    Stores state and height for node in Redis.
    """
    version_msg = {}

    redis_conn.set(key, '')  # Set Redis key for a new node.

    # (address, port, services) = key[5:].split('-', 2)
    (address, port) = key[5:].split('-', 1)
    # services = int(services)
    height = redis_conn.get('height')
    if height:
        height = int(height)

    proxy = None
    if address.endswith('.onion') and CONF['onion']:
        proxy = random.choice(CONF['tor_proxies'])

    conn = Connection((address, int(port)),
                      (CONF['source_address'], 0),
                      magic_number=CONF['magic_number'],
                      socket_timeout=CONF['socket_timeout'],
                      proxy=proxy,
                      protocol_version=CONF['protocol_version'],
                      #   to_services=services,
                      to_services=TO_SERVICES,
                      from_services=CONF['services'],
                      user_agent=CONF['user_agent'],
                      height=height,
                      relay=CONF['relay'])
    try:
        logging.debug(f'Connecting to {conn.to_addr}')
        conn.open()
        version_msg = conn.handshake()
    except (ProtocolError, ConnectionError, socket.error) as err:
        logging.debug(f'{conn.to_addr}: {err}')

    redis_pipe = redis_conn.pipeline()
    if version_msg:
        # try:
        #     conn.getaddr(block=False)
        # except (ProtocolError, ConnectionError, socket.error) as err:
        #     logging.debug("%s: %s", conn.to_addr, err)
        # else:
        #     addr_wait = 0
        #     while addr_wait < CONF['socket_timeout']:
        #         addr_wait += 1
        #         gevent.sleep(0.3)
        #         try:
        #             msgs = conn.get_messages(commands=['addr'])
        #         except (ProtocolError, ConnectionError, socket.error) as err:
        #             logging.debug("%s: %s", conn.to_addr, err)
        #             break
        #         if msgs and any([msg['count'] > 0 for msg in msgs]):
        #             addr_msgs = msgs
        #             break
        version = version_msg.get('version', '')
        user_agent = version_msg.get('user_agent', '')
        from_services = version_msg.get('services', 0)
        height = version_msg.get('height', 0)

        # if from_services != services:
        #     logging.debug(f'{conn.to_addr} '
        #                   f'Expected {services}, '
        #                   f'got {from_services} for services')
        #     key = f'node:{address}-{port}-{from_services}'

        height_key = f'height:{address}-{port}-{from_services}'
        redis_pipe.setex(height_key, CONF['max_age'], height)

        version_key = f'version:{address}-{port}'
        redis_pipe.setex(version_key,
                         CONF['max_age'],
                         str((version, user_agent, from_services)))

        peers = get_cached_peers(conn, redis_conn)
        for peer in peers:
            redis_pipe.sadd('pending', str(peer))
        redis_pipe.set(key, '')
        up_key = "node:{}-{}-{}".format(address, port, from_services)
        redis_pipe.sadd('up', up_key)
    conn.close()
    redis_pipe.execute()


def dump(timestamp, nodes, redis_conn):
    """
    Dumps data for reachable nodes into timestamp-prefixed JSON file and
    returns most common height from the nodes.
    """
    json_data = []

    logging.info('Building JSON data')
    for node in nodes:
        (address, port, services) = node.decode()[5:].split('-', 2)
        height_key = f'height:{address}-{port}-{services}'
        try:
            height = int(redis_conn.get(height_key))
        except TypeError:
            logging.warning(f'{height_key} missing')
            height = 0
        version_key = f'version:{address}-{port}'
        try:
            version = eval(redis_conn.get(version_key))
        except TypeError:
            logging.warning(f'{version_key} missing')
            version = (0, '', services)
        json_data.append(
            [address, int(port), int(services), height, version[1]])
    logging.info(f'Built JSON data: {len(json_data)}')

    if len(json_data) == 0:
        logging.warning(f'len(json_data): {len(json_data)}')
        return 0

    json_output = os.path.join(CONF['crawl_dir'], f'{timestamp}.json')
    open(json_output, 'w').write(json.dumps(json_data))
    logging.info(f'Wrote {json_output}')

    return Counter([node[3] for node in json_data]).most_common(1)[0][0]


def restart(timestamp, redis_conn):
    """
    Dumps data for the reachable nodes into a JSON file.
    Loads all reachable nodes from Redis into the crawl set.
    Removes keys for all nodes from current crawl.
    Updates included ASNs with current list from external URL.
    Updates excluded networks with current list of bogons.
    Updates number of reachable nodes in Redis.
    """
    redis_pipe = redis_conn.pipeline()

    nodes = redis_conn.smembers('up')  # Reachable nodes.
    redis_pipe.delete('up')

    for node in nodes:
        (address, port, services) = node.decode()[5:].split('-', 2)
        redis_pipe.sadd('pending', str((address, int(port), int(services))))

    for key in get_keys(redis_conn, 'node:*'):
        redis_pipe.delete(key)

    for key in get_keys(redis_conn, 'crawl:cidr:*'):
        redis_pipe.delete(key)

    if CONF['include_checked']:
        checked_nodes = redis_conn.zrangebyscore(
            'check', timestamp - CONF['max_age'], timestamp)
        for node in checked_nodes:
            (address, port, services) = eval(node)
            if is_excluded(address):
                logging.debug(f'Exclude: {address}')
                continue
            redis_pipe.sadd('pending', str((address, port, services)))

    redis_pipe.execute()

    update_included_asns()

    update_excluded_networks()

    reachable_nodes = len(nodes)
    logging.info(f'Reachable nodes: {reachable_nodes}')
    redis_conn.lpush('nodes', str((timestamp, reachable_nodes)))

    height = dump(timestamp, nodes, redis_conn)
    logging.info(f'Height: {height}')


def cron(redis_conn):
    """
    Assigned to a worker to perform the following tasks periodically to
    maintain a continuous crawl:
    1) Reports the current number of nodes in crawl set
    2) Initiates a new crawl once the crawl set is empty
    """
    start = int(time.time())

    while True:
        pending_nodes = redis_conn.scard('pending')
        logging.info(f'Pending: {pending_nodes}')

        if pending_nodes == 0:
            redis_conn.set('crawl:master:state', 'starting')
            now = int(time.time())
            elapsed = now - start
            redis_conn.set('elapsed', elapsed)
            logging.info(f'Elapsed: {elapsed}')
            logging.info('Restarting')
            restart(now, redis_conn)
            while int(time.time()) - start < CONF['snapshot_delay']:
                gevent.sleep(1)
            start = int(time.time())
            redis_conn.set('crawl:master:state', 'running')

        gevent.sleep(CONF['cron_delay'])


def task(redis_conn):
    """
    Assigned to a worker to retrieve (pop) a node from the crawl set and
    attempt to establish connection with a new node.
    """
    while True:
        if not CONF['master']:
            while redis_conn.get('crawl:master:state') != b'running':
                gevent.sleep(CONF['socket_timeout'])

        node = redis_conn.spop('pending')  # Pop random node from set.
        if node is None:
            gevent.sleep(1)
            continue

        node = eval(node)  # Convert string from Redis to tuple.

        # Skip IPv6 node.
        if ':' in node[0] and not CONF['ipv6']:
            continue

        # key = "node:{}-{}-{}".format(node[0], node[1], node[2])
        key = f'node:{node[0]}-{node[1]}'
        if redis_conn.exists(key):
            continue

        # Check if prefix has hit its limit.
        if ':' in node[0] and CONF['ipv6_prefix'] < 128:
            cidr = ip_to_network(node[0], CONF['ipv6_prefix'])
            nodes = redis_conn.incr(f'crawl:cidr:{cidr}')
            if nodes > CONF['nodes_per_ipv6_prefix']:
                logging.debug(f'CIDR {cidr}: {nodes}')
                continue

        connect(key, redis_conn)


def set_pending(redis_conn):
    """
    Initializes pending set in Redis with a list of reachable nodes from DNS
    seeders and hardcoded list of .onion nodes to bootstrap the crawler.
    """
    REDIS_CONN.sadd('pending', ('127.0.0.10', 15010, TO_SERVICES))
    return

    for seeder in CONF['seeders']:
        nodes = []

        try:
            ipv4_nodes = socket.getaddrinfo(seeder, None, socket.AF_INET)
        except socket.gaierror as err:
            logging.warning(err)
        else:
            nodes.extend(ipv4_nodes)

        if CONF['ipv6']:
            try:
                ipv6_nodes = socket.getaddrinfo(seeder, None, socket.AF_INET6)
            except socket.gaierror as err:
                logging.warning(err)
            else:
                nodes.extend(ipv6_nodes)

        for node in nodes:
            address = node[-1][0]
            if is_excluded(address):
                logging.debug(f'Exclude: {address}')
                continue
            logging.debug(f'{seeder}: {address}')
            redis_conn.sadd(
                'pending', str((address, CONF['port'], TO_SERVICES)))

    if CONF['onion']:
        for address in CONF['onion_nodes']:
            redis_conn.sadd(
                'pending', str((address, CONF['port'], TO_SERVICES)))


def is_excluded(address):
    """
    Returns True if address is found in exclusion rules, False if otherwise.

    In priority order, the rules are:
    - Include onion address
    - Exclude private address
    - Exclude address without ASN when include_asns/exclude_asns is set
    - Exclude if address is in exclude_asns
    - Exclude bad address
    - Exclude if address is in exclude_ipv4_networks/exclude_ipv6_networks
    - Exclude if address is not in include_asns
    - Include address
    """
    if address.endswith('.onion'):
        return False

    if CONF['exclude_private'] and ip_address(address).is_private:
        return True

    asn = None
    if CONF['include_asns'] or CONF['exclude_asns']:
        try:
            asn_record = ASN.asn(address)
        except AddressNotFoundError:
            asn = None
        else:
            asn = f'AS{asn_record.autonomous_system_number}'
        if asn is None:
            return True

    if CONF['exclude_asns'] and asn in CONF['exclude_asns']:
        return True

    if ':' in address:
        address_family = socket.AF_INET6
        key = 'exclude_ipv6_networks'
    else:
        address_family = socket.AF_INET
        key = 'exclude_ipv4_networks'
    try:
        addr = int(hexlify(socket.inet_pton(address_family, address)), 16)
    except socket.error:
        logging.warning(f'Bad address: {address}')
        return True
    if any([(addr & net[1] == net[0]) for net in CONF[key]]):
        return True

    if CONF['include_asns'] and asn not in CONF['include_asns']:
        return True

    return False


def update_included_asns():
    """
    Updates included ASNs with current list from external URL.
    """
    if not CONF['include_asns_from_url']:
        return

    txt = http_get_txt(CONF['include_asns_from_url'])
    CONF['include_asns'] = list_included_asns(txt)
    logging.info(f"ASNs: {len(CONF['include_asns'])}")


def list_included_asns(txt, asns=None):
    """
    Converts list of ASNs from configuration file into a set.
    """
    if asns is None:
        asns = set()
    lines = txt.strip().split('\n')
    for line in lines:
        line = line.strip()
        if line.startswith('AS'):
            asns.add(line)
    return asns


def update_excluded_networks():
    """
    Updates excluded networks with current bogons and current list from
    external URL.
    """
    CONF['exclude_ipv4_networks'] = CONF['default_exclude_ipv4_networks']
    CONF['exclude_ipv6_networks'] = CONF['default_exclude_ipv6_networks']

    if CONF['exclude_ipv4_bogons']:
        urls = [
            'http://www.team-cymru.org/Services/Bogons/fullbogons-ipv4.txt',
            'http://www.spamhaus.org/drop/drop.txt',
            'https://www.spamhaus.org/drop/edrop.txt',
        ]
        for url in urls:
            txt = http_get_txt(url)
            CONF['exclude_ipv4_networks'] = list_excluded_networks(
                txt, networks=CONF['exclude_ipv4_networks'])

    if CONF['exclude_ipv6_bogons']:
        urls = [
            'http://www.team-cymru.org/Services/Bogons/fullbogons-ipv6.txt',
        ]
        for url in urls:
            txt = http_get_txt(url)
            CONF['exclude_ipv6_networks'] = list_excluded_networks(
                txt, networks=CONF['exclude_ipv6_networks'])

    if CONF['exclude_ipv4_networks_from_url']:
        txt = http_get_txt(CONF['exclude_ipv4_networks_from_url'])
        CONF['exclude_ipv4_networks'] = list_excluded_networks(
            txt, networks=CONF['exclude_ipv4_networks'])

    if CONF['exclude_ipv6_networks_from_url']:
        txt = http_get_txt(CONF['exclude_ipv6_networks_from_url'])
        CONF['exclude_ipv6_networks'] = list_excluded_networks(
            txt, networks=CONF['exclude_ipv6_networks'])

    logging.info(
        f"IPv4: {len(CONF['exclude_ipv4_networks'])}, "
        f"IPv6: {len(CONF['exclude_ipv6_networks'])}")


def list_excluded_networks(txt, networks=None):
    """
    Converts list of networks from configuration file into a list of tuples of
    network address and netmask to be excluded from the crawl.
    """
    if networks is None:
        networks = set()
    lines = txt.strip().split('\n')
    for line in lines:
        line = line.split('#')[0].split(';')[0].strip()
        try:
            network = ip_network(line)
        except ValueError:
            continue
        else:
            networks.add((int(network.network_address), int(network.netmask)))
    return networks


def init_conf(argv):
    """
    Populates CONF with key-value pairs from configuration file.
    """
    conf = ConfigParser()
    conf.read(argv[1])
    CONF['logfile'] = conf.get('crawl', 'logfile')
    CONF['magic_number'] = unhexlify(conf.get('crawl', 'magic_number'))
    CONF['port'] = conf.getint('crawl', 'port')
    CONF['db'] = conf.getint('crawl', 'db')
    CONF['seeders'] = conf.get('crawl', 'seeders').strip().split('\n')
    CONF['workers'] = conf.getint('crawl', 'workers')
    CONF['debug'] = conf.getboolean('crawl', 'debug')
    CONF['source_address'] = conf.get('crawl', 'source_address')
    CONF['protocol_version'] = conf.getint('crawl', 'protocol_version')
    CONF['user_agent'] = conf.get('crawl', 'user_agent')
    CONF['services'] = conf.getint('crawl', 'services')
    CONF['relay'] = conf.getint('crawl', 'relay')
    CONF['socket_timeout'] = conf.getint('crawl', 'socket_timeout')
    CONF['cron_delay'] = conf.getint('crawl', 'cron_delay')
    CONF['snapshot_delay'] = conf.getint('crawl', 'snapshot_delay')
    CONF['addr_ttl'] = conf.getint('crawl', 'addr_ttl')
    CONF['addr_ttl_var'] = conf.getint('crawl', 'addr_ttl_var')
    CONF['max_age'] = conf.getint('crawl', 'max_age')
    CONF['peers_per_node'] = conf.getint('crawl', 'peers_per_node')
    CONF['ipv6'] = conf.getboolean('crawl', 'ipv6')
    CONF['ipv6_prefix'] = conf.getint('crawl', 'ipv6_prefix')
    CONF['nodes_per_ipv6_prefix'] = conf.getint('crawl',
                                                'nodes_per_ipv6_prefix')

    CONF['include_asns'] = None
    include_asns = conf.get('crawl', 'include_asns').strip()
    if include_asns:
        CONF['include_asns'] = set(include_asns.split('\n'))
    CONF['include_asns_from_url'] = conf.get('crawl', 'include_asns_from_url')

    CONF['exclude_asns'] = None
    exclude_asns = conf.get('crawl', 'exclude_asns').strip()
    if exclude_asns:
        CONF['exclude_asns'] = set(exclude_asns.split('\n'))

    CONF['exclude_private'] = conf.getboolean('crawl', 'exclude_private')

    CONF['default_exclude_ipv4_networks'] = list_excluded_networks(
        conf.get('crawl', 'exclude_ipv4_networks'))
    CONF['default_exclude_ipv6_networks'] = list_excluded_networks(
        conf.get('crawl', 'exclude_ipv6_networks'))

    CONF['exclude_ipv4_networks'] = CONF['default_exclude_ipv4_networks']
    CONF['exclude_ipv6_networks'] = CONF['default_exclude_ipv6_networks']

    CONF['exclude_ipv4_bogons'] = conf.getboolean('crawl',
                                                  'exclude_ipv4_bogons')
    CONF['exclude_ipv6_bogons'] = conf.getboolean('crawl',
                                                  'exclude_ipv6_bogons')

    CONF['exclude_ipv4_networks_from_url'] = conf.get(
        'crawl', 'exclude_ipv4_networks_from_url')
    CONF['exclude_ipv6_networks_from_url'] = conf.get(
        'crawl', 'exclude_ipv6_networks_from_url')

    CONF['onion'] = conf.getboolean('crawl', 'onion')
    CONF['tor_proxies'] = []
    if CONF['onion']:
        tor_proxies = conf.get('crawl', 'tor_proxies').strip().split('\n')
        CONF['tor_proxies'] = [
            (p.split(':')[0], int(p.split(':')[1])) for p in tor_proxies]
    CONF['onion_nodes'] = conf.get('crawl', 'onion_nodes').strip().split('\n')

    CONF['include_checked'] = conf.getboolean('crawl', 'include_checked')

    CONF['crawl_dir'] = conf.get('crawl', 'crawl_dir')
    if not os.path.exists(CONF['crawl_dir']):
        os.makedirs(CONF['crawl_dir'])

    # Set to True for master process
    CONF['master'] = argv[2] == 'master'


def main(argv):
    if len(argv) < 3 or not os.path.exists(argv[1]):
        print('Usage: crawl.py [config] [master|slave]')
        return 1

    # Initialize global conf.
    init_conf(argv)

    # Initialize logger.
    loglevel = logging.INFO
    if CONF['debug']:
        loglevel = logging.DEBUG

    logformat = ('[%(process)d] %(asctime)s,%(msecs)05.1f %(levelname)s '
                 '(%(funcName)s) %(message)s')
    logging.basicConfig(level=loglevel,
                        format=logformat,
                        filename=CONF['logfile'],
                        filemode='a')

    # also log to stdout
    logging.getLogger().addHandler(
        logging.StreamHandler(sys.stdout)
    )
    print(f"Log: {CONF['logfile']}, press CTRL+C to terminate..")

    redis_conn = new_redis_conn(db=CONF['db'])

    if CONF['master']:
        redis_conn.set('crawl:master:state', 'starting')
        logging.info('Removing all keys')
        redis_pipe = redis_conn.pipeline()
        redis_pipe.delete('up')
        for key in get_keys(redis_conn, 'node:*'):
            redis_pipe.delete(key)
        for key in get_keys(redis_conn, 'crawl:cidr:*'):
            redis_pipe.delete(key)
        for key in get_keys(redis_conn, 'peer:*'):
            redis_pipe.delete(key)
        redis_pipe.delete('pending')
        redis_pipe.execute()
        update_included_asns()
        update_excluded_networks()
        set_pending(redis_conn)
        redis_conn.set('crawl:master:state', 'running')

    # Spawn workers (greenlets) including one worker reserved for cron tasks.
    workers = []
    if CONF['master']:
        workers.append(gevent.spawn(cron, redis_conn))
    for _ in range(CONF['workers'] - len(workers)):
        workers.append(gevent.spawn(task, redis_conn))
    logging.info(f'Workers: {len(workers)}')

    try:
        gevent.joinall(workers)
    except KeyboardInterrupt:
        pass

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
