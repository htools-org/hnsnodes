import argparse
import gzip
import itertools
import os
import pathlib
import shutil
import tarfile
from datetime import datetime, timedelta

NOW = datetime.now()

# see main()
options = dict()


def create_test_data():
    datadir = pathlib.Path(__file__, '..', 'testdata').resolve()
    print('[*] Writing test data to', datadir)
    with open(datadir / 'fileslist.txt', 'r') as f:
        filenames = map(lambda x: x.strip(), f.readlines())

    for filename in filenames:
        with open(datadir / 'crawl/mainnet' / filename, 'w') as f:
            f.write(f'{filename} data')


# create_test_data()


def get_files_in_dir(dir: str):
    return list(
        filter(
            lambda x: os.path.isfile(
                os.path.join(dir, x)
            ) and x.endswith('.json'),
            sorted(os.listdir(dir))
        )
    )


def parse_date_from_file(filename: str):
    if not filename.endswith('.json'):
        raise Exception(f'Invalid filename: not json: {filename}')
    timestamp = int(filename[:-5])
    file_date = datetime.fromtimestamp(timestamp)
    # human_friendly_date = file_date.strftime('%Y-%m-%d %H:%M:%S')
    return file_date


def is_old_date(date: datetime):
    return date < (NOW - options['OLD_DATE_DELTA'])


def process_dir(dir: pathlib.Path):
    # Get all files
    filenames = get_files_in_dir(dir)
    print(f'[*] found {len(filenames)} files.')

    # Parse dates
    files = list(
        map(lambda x: {'filename': x, 'date': parse_date_from_file(x)}, filenames))

    # Filter only old files (ignore new)
    old_files = list(filter(lambda x: is_old_date(x['date']), files))
    print(
        f'[*] {len(old_files)} are old files, {len(filenames)-len(old_files)} are new.')

    if len(old_files) == 0:
        print('[*] Nothing to do here.')
        return

    # Group by year-month
    files_by_group = [
        {
            'group': k,
            'files': [file['filename'] for file in g]
        }
        for k, g in itertools.groupby(old_files, key=lambda x: f'{x["date"].year:04}-{x["date"].month:02}')
    ]
    # print(json.dumps(files_by_group, indent=2))
    print(
        f'[*] {len(files_by_group)} groups:')
    print('\n'.join(
        [f'\t{group["group"]}: {len(group["files"])} files' for group in files_by_group])
    )

    # Confirm run
    if not options['YES']:
        if input('\nAdd to archive? [y/n]: ').lower() not in ['y', 'yes']:
            print('[*] Aborting.')
            exit(0)

    # Create/Open archives for each group
    for group in files_by_group:
        with tarfile.open(os.path.join(dir, f'{group["group"]}.tar'), 'a:') as tar:
            for filename in group["files"]:
                file_path = os.path.join(dir, filename)
                try:
                    if tar.getmember(filename):
                        if options['DELETE_ARCHIVED_FILES']:
                            os.remove(file_path)
                        continue
                except KeyError:
                    pass  # does not exist
                tar.add(file_path, filename, recursive=False)
                if options['DELETE_ARCHIVED_FILES']:
                    os.remove(file_path)


def gzip_old_tars(dir: pathlib.Path):
    tars = list(
        filter(
            lambda x: os.path.isfile(
                os.path.join(dir, x)
            ) and x.endswith('.tar'),
            sorted(os.listdir(dir))
        )
    )
    print(f'[*] Compressing {abs(len(tars) - 1)} tars:', tars[:-1])

    # gzip all except latest
    for tarfile in tars[:-1]:
        with open(dir / tarfile, 'rb') as f_in:
            with gzip.open(dir / f'{tarfile}.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        # delete uncompressed tar
        os.remove(dir / tarfile)


def main():
    default_data_path = pathlib.Path(__file__, '..', '..', 'data').resolve()
    # print('default_data_path:', default_data_path)

    parser = argparse.ArgumentParser(
        prog=os.path.basename(__file__),
        description='Archive old data files')

    parser.add_argument('-y', '--yes', action='store_true',
                        help='Skip confirmation')
    parser.add_argument('--days', type=int, default=90,
                        help='files older than DAYS are considered')
    parser.add_argument('--delete', action='store_true',
                        help='Delete files after copying to archive')
    parser.add_argument('--compress', action='store_true',
                        help='gzip uncompressed tars')
    parser.add_argument('datadir', type=pathlib.Path, nargs='?',
                        default=default_data_path,
                        help='Path to data folder which contains crawl,export')

    config = parser.parse_args()

    datadir: pathlib.Path = config.datadir.resolve()
    options['YES'] = config.yes
    options['OLD_DATE_DELTA'] = timedelta(days=config.days)
    options['DELETE_ARCHIVED_FILES'] = config.delete

    # Print config
    print('Current config:')
    print('  DATA_DIR:', datadir)
    print('  OLD_DATE_DELTA:', options['OLD_DATE_DELTA'].days, 'days')
    print('  DELETE_ARCHIVED_FILES:', options['DELETE_ARCHIVED_FILES'])
    print('  GZIP_UNCOMPRESSED_TARS:', config.compress)

    # Confirm run
    if not config.yes:
        if input('\nContinue? [y/n]: ').lower() not in ['y', 'yes']:
            print('[*] Aborting.')
            exit(0)

    # Process crawl data
    if pathlib.Path(datadir, 'crawl').is_dir():
        process_dir(datadir / 'crawl' / 'mainnet')
        if config.compress:
            gzip_old_tars(datadir / 'crawl' / 'mainnet')

    # Process export data
    if pathlib.Path(datadir, 'export').is_dir():
        process_dir(datadir / 'export' / 'mainnet')
        if config.compress:
            gzip_old_tars(datadir / 'export' / 'mainnet')


main()
