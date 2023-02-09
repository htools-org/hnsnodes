from multiprocessing import Process
import sys

from crawl import main as crawl_main
from ping import main as ping_main
from resolve import main as resolve_main
from export import main as export_main


def print_help():
    print(
        'Usage: run.py [network]\n'
        '\t(network = main | regtest)'
    )


if len(sys.argv) != 2:
    print('Invalid network.\n')
    print_help()
    exit(1)

network = sys.argv[1]

if network == 'main' or network == 'mainnet':
    config_suffix = '.main.conf'
elif network == 'regtest':
    config_suffix = '.regtest.conf'
else:
    print('Invalid network.\n')
    print_help()
    exit(1)


def run_crawl():
    crawl_config_path = 'conf/crawl' + config_suffix
    crawl_main([None, crawl_config_path, 'master'])


def run_ping():
    ping_config_path = 'conf/ping' + config_suffix
    ping_main([None, ping_config_path, 'master'])


def run_resolve():
    resolve_config_path = 'conf/resolve' + config_suffix
    resolve_main([None, resolve_config_path, 'master'])


def run_export():
    export_config_path = 'conf/export' + config_suffix
    export_main([None, export_config_path, 'master'])


try:
    crawl = Process(target=run_crawl)
    crawl.start()
    ping = Process(target=run_ping)
    ping.start()
    resolve = Process(target=run_resolve)
    resolve.start()
    export = Process(target=run_export)
    export.start()

    crawl.join()
    ping.join()
    resolve.join()
    export.join()
except KeyboardInterrupt:
    crawl.terminate()
    ping.terminate()
    resolve.terminate()
    export.terminate()
