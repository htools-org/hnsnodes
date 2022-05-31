import collections
import json
import os
import argparse

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '..'))
CRAWL_DATA_DIR = ROOT_DIR + '/data/crawl/d3f26e5b'


def is_valid_file(parser, arg):
    # https://stackoverflow.com/a/11541450/1724828
    if not arg:
        return None
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')  # return an open file handle


# ---
# Parse arguments
# ---
parser = argparse.ArgumentParser(
    description='Parse the json dumps from crawler.py')
parser.add_argument("file", help="input json file (defaults to latest crawl)", nargs='?',
                    metavar="FILE", type=lambda x: is_valid_file(parser, x))

parser.add_argument('--table', '-t', action='store_true',
                    help='whether to print table of nodes')
parser.add_argument('--json', '-j', action='store_true',
                    help='whether to print list of objects')
parser.add_argument('--pretty', '-p', action='store_true',
                    help='use with --json to prettify')
args = parser.parse_args()

if not args.table and not args.json:
    parser.print_help()

    print('\nSelect at least one of [table, json].')
    exit(1)

# ---
# Open file
# ---
file = args.file
if not file:
    files = [x for x in os.listdir(CRAWL_DATA_DIR) if x.endswith('.json')]
    files.sort()
    file = open(CRAWL_DATA_DIR + '/' + files[-1], 'r')
filedata = json.load(file)


# ---
# Print
# ---
nodes = []

if args.table:
    print('Address\t\tPort\tSrvs\tHeight\tUserAgent')
    print('-------\t\t----\t----\t------\t---------')

for row in filedata:
    address, port, services, height, agent = row
    node = {
        'address': address,
        'port': port,
        'services': services,
        'height': height,
        'agent': agent,
    }
    nodes.append(node)

    if args.table:
        print('{address}\t{port}\t{services}\t{height}\t{agent}'.format(**node))

if args.table:
    print('\nTotal: ' + str(len(nodes)))
    print('\nCount by version:')

    version_counter = collections.Counter(map(lambda x: x['agent'], nodes))
    for agent, count in version_counter.most_common():
        print(agent + ': ' + str(count))


if args.json:
    if args.pretty:
        print(json.dumps(nodes, indent=4, sort_keys=True))
    else:
        print(nodes)
