#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# ping.py - Greenlets-based Bitcoin network pinger.
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
Greenlets-based Bitcoin network pinger.
"""

from gevent import monkey
monkey.patch_all()

import gevent
import gevent.pool
import glob
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
from configparser import ConfigParser

from protocol import Connection
from protocol import ConnectionError
from protocol import ProtocolError
from utils import get_keys
from utils import ip_to_network
from utils import new_redis_conn

redis.connection.socket = gevent.socket

CONF = {}


class Keepalive(object):
    """
    Implements keepalive mechanic to keep the specified connection with a node.
    """

    def __init__(self, conn=None, version_msg=None, redis_conn=None):
        self.conn = conn
        self.node = conn.to_addr

        self.start_time = int(time.time())
        self.last_ping = self.start_time
        self.last_version = self.start_time

        self.ping_delay = 30
        self.version_delay = CONF['version_delay']

        self.redis_conn = redis_conn
        self.redis_pipe = redis_conn.pipeline()

        # version = version_msg.get('version', '')
        user_agent = version_msg.get('user_agent', '')
        services = version_msg.get('services', '')

        # Open connections are tracked in open set with the associated data
        # stored in opendata set in Redis.
        self.data = self.node + (
            # version,
            user_agent,
            self.start_time,
            services)
        self.redis_conn.sadd('opendata', str(self.data))

    def keepalive(self):
        """
        Periodically sends ping message and refreshes version information.
        """
        while True:
            now = time.time()

            if now > self.last_ping + self.ping_delay:
                if not self.ping(now):
                    break

            if now > self.last_version + self.version_delay:
                if not self.version(now):
                    break

            if not self.sink():
                break

            gevent.sleep(0.1)

        self.close()

    def close(self):
        self.redis_conn.srem('opendata', str(self.data))
        self.conn.close()

    def ping(self, now):
        """
        Sends a ping message. Ping time is stored in Redis for round-trip time
        (RTT) calculation.
        """
        self.last_ping = now

        nonce = random.getrandbits(64)
        try:
            self.conn.ping(nonce=nonce)
        except socket.error as err:
            logging.info(f'Closing {self.node} ({err})')
            return False
        logging.debug(f'pinging {self.node} ({nonce}) {now}')

        key = f'ping:{self.node[0]}-{self.node[1]}:{nonce}'
        self.redis_conn.lpush(key, int(self.last_ping * 1000))  # milliseconds
        self.redis_conn.expire(key, CONF['rtt_ttl'])

        # try:
        #     self.ping_delay = int(self.redis_conn.get('elapsed'))
        # except TypeError:
        #     pass

        return True

    def version(self, now):
        """
        Refreshes version information using response from latest handshake.
        """
        self.last_version = now

        version_key = f'version:{self.node[0]}-{self.node[1]}'
        version_data = self.redis_conn.get(version_key)

        if version_data is None:
            return True

        version, user_agent, services = eval(version_data)
        if all([version, user_agent, services]):
            data = self.node + (
                # version,
                user_agent,
                self.start_time,
                services)

            if self.data != data:
                self.redis_conn.srem('opendata', str(self.data))
                self.redis_conn.sadd('opendata', str(data))
                self.data = data

        return True

    def sink(self):
        """
        Sinks received messages to flush them off socket buffer.
        """
        try:
            msgs = self.conn.get_messages()
        except socket.timeout:
            pass
        except (ProtocolError, ConnectionError, socket.error) as err:
            logging.info(f'Closing {self.node} ({err})')
            return False
        else:
            # Cache block inv messages
            for msg in msgs:
                if msg['command'] != b'inv':
                    continue
                ms = msg['timestamp']
                for inv in msg['inventory']:
                    if inv['type'] != 2:
                        continue
                    key = f"binv:{inv['hash'].decode()}"
                    self.redis_pipe.execute_command(
                        'ZADD', key, 'LT', ms,
                        f'{self.node[0]}-{self.node[1]}')
                    self.redis_pipe.expire(key, CONF['inv_ttl'])
            self.redis_pipe.execute()

        return True


def task(redis_conn):
    """
    Assigned to a worker to retrieve (pop) a node from the reachable set and
    attempt to establish and maintain connection with the node.
    """
    node = redis_conn.spop('reachable')
    if node is None:
        return
    (address, port, services, height) = eval(node)
    node = (address, port)

    # Check if prefix has hit its limit
    cidr_key = None
    if ':' in address and CONF['ipv6_prefix'] < 128:
        cidr = ip_to_network(address, CONF['ipv6_prefix'])
        cidr_key = f'ping:cidr:{cidr}'
        nodes = redis_conn.incr(cidr_key)
        logging.info(f'+CIDR {cidr}: {nodes}')
        if nodes > CONF['nodes_per_ipv6_prefix']:
            logging.info(f'CIDR limit reached: {cidr}')
            nodes = redis_conn.decr(cidr_key)
            logging.info(f'-CIDR {cidr}: {nodes}')
            return

    if redis_conn.sadd('open', str(node)) == 0:
        logging.info(f'Connection exists: {node}')
        if cidr_key:
            nodes = redis_conn.decr(cidr_key)
            logging.info(f'-CIDR {cidr}: {nodes}')
        return

    proxy = None
    if address.endswith('.onion') and CONF['onion']:
        proxy = random.choice(CONF['tor_proxies'])

    version_msg = {}
    conn = Connection(node,
                      (CONF['source_address'], 0),
                      magic_number=CONF['magic_number'],
                      socket_timeout=CONF['socket_timeout'],
                      proxy=proxy,
                      protocol_version=CONF['protocol_version'],
                      to_services=services,
                      from_services=CONF['services'],
                      user_agent=CONF['user_agent'],
                      height=height,
                      relay=CONF['relay'])
    try:
        logging.debug(f'Connecting to {conn.to_addr}')
        conn.open()
        version_msg = conn.handshake()
    except (ProtocolError, ConnectionError, socket.error) as err:
        logging.debug(f'Closing {node} ({err})')
        conn.close()

    if not version_msg:
        if cidr_key:
            nodes = redis_conn.decr(cidr_key)
            logging.info(f'-CIDR {cidr}: {nodes}')
        redis_conn.srem('open', str(node))
        return

    if address.endswith('.onion'):
        # Map local port to .onion node.
        local_port = conn.socket.getsockname()[1]
        logging.debug(f'Local port {conn.to_addr}: {local_port}')
        redis_conn.set(f'onion:{local_port}', str(conn.to_addr))

    Keepalive(
        conn=conn,
        version_msg=version_msg,
        redis_conn=redis_conn).keepalive()

    if cidr_key:
        nodes = redis_conn.decr(cidr_key)
        logging.info(f'-CIDR {cidr}: {nodes}')
    redis_conn.srem('open', str(node))


def cron(pool, redis_conn):
    """
    Assigned to a worker to perform the following tasks periodically to
    maintain a continuous network-wide connections:

    [Master]
    1) Checks for a new snapshot
    2) Loads new reachable nodes into the reachable set in Redis
    3) Signals listener to get reachable nodes from opendata set

    [Master/Slave]
    1) Spawns workers to establish and maintain connection with reachable nodes
    """
    magic_number = hexlify(CONF['magic_number']).decode()
    publish_key = f'snapshot:{magic_number}'
    snapshot = None

    while True:
        if CONF['master']:
            new_snapshot = get_snapshot()

            if new_snapshot != snapshot:
                nodes = get_nodes(new_snapshot)
                if len(nodes) == 0:
                    continue

                logging.info(f'New snapshot: {new_snapshot}')
                snapshot = new_snapshot

                logging.info(f'Nodes: {len(nodes)}')

                reachable_nodes = set_reachable(nodes, redis_conn)
                logging.info(f'New reachable nodes: {reachable_nodes}')

                # Allow connections to stabilize before publishing snapshot.
                gevent.sleep(CONF['socket_timeout'])
                redis_conn.publish(publish_key, int(time.time()))

            connections = redis_conn.scard('open')
            logging.info(f'Connections: {connections}')

        for _ in range(min(redis_conn.scard('reachable'), pool.free_count())):
            pool.spawn(task, redis_conn)

        workers = CONF['workers'] - pool.free_count()
        logging.info(f'Workers: {workers}')

        gevent.sleep(CONF['cron_delay'])


def get_snapshot():
    """
    Returns latest JSON file (based on creation date) containing a snapshot of
    all reachable nodes from a completed crawl.
    """
    snapshot = None
    try:
        snapshot = max(glob.iglob(f"{CONF['crawl_dir']}/*.json"))
    except ValueError as err:
        logging.warning(err)
    return snapshot


def get_nodes(path):
    """
    Returns all reachable nodes from a JSON file.
    """
    nodes = []
    text = open(path, 'r').read()
    try:
        nodes = json.loads(text)
    except ValueError as err:
        logging.warning(err)
    return nodes


def set_reachable(nodes, redis_conn):
    """
    Adds reachable nodes that are not already in the open set into the
    reachable set in Redis. New workers can be spawned separately to establish
    and maintain connection with these nodes.
    """
    for node in nodes:
        address = node[0]
        port = node[1]
        services = node[2]
        height = node[3]
        if not redis_conn.sismember('open', str((address, port))):
            redis_conn.sadd(
                'reachable', str((address, port, services, height)))
    return redis_conn.scard('reachable')


def init_conf(argv):
    """
    Populates CONF with key-value pairs from configuration file.
    """
    conf = ConfigParser(inline_comment_prefixes='#')
    conf.read(argv[1])
    CONF['logfile'] = conf.get('ping', 'logfile')
    CONF['log_to_console'] = conf.getboolean('ping', 'log_to_console')
    CONF['magic_number'] = unhexlify(conf.get('ping', 'magic_number'))
    CONF['db'] = conf.getint('ping', 'db')
    CONF['workers'] = conf.getint('ping', 'workers')
    CONF['debug'] = conf.getboolean('ping', 'debug')
    CONF['source_address'] = conf.get('ping', 'source_address')
    CONF['protocol_version'] = conf.getint('ping', 'protocol_version')
    CONF['user_agent'] = conf.get('ping', 'user_agent')
    CONF['services'] = conf.getint('ping', 'services')
    CONF['relay'] = conf.getint('ping', 'relay')
    CONF['socket_timeout'] = conf.getint('ping', 'socket_timeout')
    CONF['cron_delay'] = conf.getint('ping', 'cron_delay')
    CONF['rtt_ttl'] = conf.getint('ping', 'rtt_ttl')
    CONF['inv_ttl'] = conf.getint('ping', 'inv_ttl')
    CONF['version_delay'] = conf.getint('ping', 'version_delay')
    CONF['ipv6_prefix'] = conf.getint('ping', 'ipv6_prefix')
    CONF['nodes_per_ipv6_prefix'] = conf.getint('ping',
                                                'nodes_per_ipv6_prefix')

    CONF['onion'] = conf.getboolean('ping', 'onion')
    CONF['tor_proxies'] = []
    if CONF['onion']:
        tor_proxies = conf.get('ping', 'tor_proxies').strip().split('\n')
        CONF['tor_proxies'] = [
            (p.split(':')[0], int(p.split(':')[1])) for p in tor_proxies]

    CONF['crawl_dir'] = conf.get('ping', 'crawl_dir')
    if not os.path.exists(CONF['crawl_dir']):
        os.makedirs(CONF['crawl_dir'])

    # Set to True for master process
    CONF['master'] = argv[2] == 'master'


def main(argv):
    if len(argv) < 3 or not os.path.exists(argv[1]):
        print('Usage: ping.py [config] [master|slave]')
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
    if CONF['log_to_console']:
        logging.getLogger().addHandler(
            logging.StreamHandler(sys.stdout)
        )

    print(f"Log: {CONF['logfile']}, press CTRL+C to terminate..")

    redis_conn = new_redis_conn(db=CONF['db'])

    if CONF['master']:
        redis_pipe = redis_conn.pipeline()
        logging.info('Removing all keys')
        redis_pipe.delete('reachable')
        redis_pipe.delete('open')
        redis_pipe.delete('opendata')
        for key in get_keys(redis_conn, 'ping:cidr:*'):
            redis_pipe.delete(key)
        redis_pipe.execute()

    # Initialize a pool of workers (greenlets).
    pool = gevent.pool.Pool(CONF['workers'])
    pool.spawn(cron, pool, redis_conn)
    pool.join()

    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
