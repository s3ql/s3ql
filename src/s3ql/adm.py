'''
adm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError, setup_logging
from . import CURRENT_FS_REV, REV_VER_MAP
from .backends.comprenc import ComprencBackend
from .database import Connection
from .deltadump import TIME, INTEGER
from .common import (get_seq_no, is_mounted, get_backend, load_params,
                     save_params)
from .metadata import dump_and_upload_metadata, download_metadata
from . import metadata
from .parse_args import ArgumentParser
from datetime import datetime as Datetime
from getpass import getpass
from contextlib import contextmanager
import os
import shutil
import functools
import sys
import textwrap
import time

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Manage S3QL File Systems.",
        epilog=textwrap.dedent('''\
               Hint: run `%(prog)s <action> --help` to get help on the additional
               arguments that the different actions take.'''))

    pparser = ArgumentParser(add_help=False, epilog=textwrap.dedent('''\
               Hint: run `%(prog)s --help` to get help on other available actions and
               optional arguments that can be used with all actions.'''))
    pparser.add_storage_url()

    subparsers = parser.add_subparsers(metavar='<action>', dest='action',
                                       help='may be either of')
    subparsers.add_parser("passphrase", help="change file system passphrase",
                          parents=[pparser])
    subparsers.add_parser("clear", help="delete file system and all data",
                          parents=[pparser])
    subparsers.add_parser("download-metadata",
                          help="Interactively download metadata backups. "
                               "Use only if you know what you are doing.",
                          parents=[pparser])
    sparser = subparsers.add_parser("upgrade", help="upgrade file system to newest revision",
                          parents=[pparser])
    sparser.add_argument("--threads", type=int, default=20,
                        help='Number of threads to use')

    parser.add_debug()
    parser.add_quiet()
    parser.add_log()
    parser.add_authfile()
    parser.add_backend_options()
    parser.add_cachedir()
    parser.add_version()

    options = parser.parse_args(args)

    return options

def main(args=None):
    '''Change or show S3QL file system parameters'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    # Check if fs is mounted on this computer
    # This is not foolproof but should prevent common mistakes
    if is_mounted(options.storage_url):
        raise QuietError('Can not work on mounted file system.')

    if options.action == 'clear':
        with get_backend(options, raw=True) as backend:
            return clear(backend, options)

    with get_backend(options) as backend:
        if options.action == 'upgrade':
            return upgrade(backend, options)
        elif options.action == 'passphrase':
            return change_passphrase(backend)

        elif options.action == 'download-metadata':
            return download_metadata_cmd(backend, options)

def download_metadata_cmd(backend, options):
    '''Download old metadata backups'''

    backups = sorted(backend.list('s3ql_metadata'))

    if not backups:
        raise QuietError('No metadata backups found.')

    log.info('The following backups are available:')
    log.info('%3s  %-23s %-15s', 'No', 'Name', 'Date')
    for (i, name) in enumerate(backups):
        try:
            params = backend.lookup(name)
        except:
            log.error('Error retrieving information about %s, skipping', name)
            continue

        if 'last-modified' in params:
            date = Datetime.fromtimestamp(params['last-modified']).strftime('%Y-%m-%d %H:%M:%S')
        else:
            # (metadata might from an older fs revision)
            date = '(unknown)'

        log.info('%3d  %-23s %-15s', i, name, date)

    name = None
    while name is None:
        buf = input('Enter no to download: ')
        try:
            name = backups[int(buf.strip())]
        except:
            log.warning('Invalid input')

    cachepath = options.cachepath
    for i in ('.db', '.params'):
        if os.path.exists(cachepath + i):
            raise QuietError('%s already exists, aborting.' % cachepath + i)

    param = backend.lookup(name)
    download_metadata(backend, cachepath + ".db", name)

    # Raise sequence number so that fsck.s3ql actually uses the
    # downloaded backup
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in backend.list('s3ql_seq_no_') ]
    param['seq_no'] = max(seq_nos) + 1
    save_params(cachepath, param)

def change_passphrase(backend):
    '''Change file system passphrase'''

    if not isinstance(backend, ComprencBackend) and backend.passphrase:
        raise QuietError('File system is not encrypted.')

    data_pw = backend.passphrase

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    wrap_pw = wrap_pw.encode('utf-8')

    backend.passphrase = wrap_pw
    backend['s3ql_passphrase'] = data_pw
    backend['s3ql_passphrase_bak1'] = data_pw
    backend['s3ql_passphrase_bak2'] = data_pw
    backend['s3ql_passphrase_bak3'] = data_pw
    backend.passphrase = data_pw

def clear(backend, options):
    print('I am about to delete all data in %s.' % backend,
          'This includes any S3QL file systems as well as any other stored objects.',
          'Please enter "yes" to continue.', '> ', sep='\n', end='')
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Deleting...')
    for suffix in ('.db', '.params'):
        name = options.cachepath + suffix
        if os.path.exists(name):
            os.unlink(name)

    name = options.cachepath + '-cache'
    if os.path.exists(name):
        shutil.rmtree(name)

    backend.clear()

    log.info('File system deleted.')
    log.info('Note: it may take a while for the removals to propagate through the backend.')

def get_old_rev_msg(rev, prog):
    return textwrap.dedent('''\
        The last S3QL version that supported this file system revision
        was %(version)s. To run this version's %(prog)s, proceed along
        the following steps:

          $ wget http://s3ql.googlecode.com/files/s3ql-%(version)s.tar.bz2 \
            || wget https://bitbucket.org/nikratio/s3ql/downloads/s3ql-%(version)s.tar.bz2
          $ tar xjf s3ql-%(version)s.tar.bz2
          $ (cd s3ql-%(version)s; ./setup.py build_ext --inplace)
          $ s3ql-%(version)s/bin/%(prog)s <options>
        ''' % { 'version': REV_VER_MAP[rev],
                'prog': prog })

def upgrade(backend, options):
    '''Upgrade file system to newest revision'''

    log.info('Getting file system parameters..')

    # Check for cached metadata
    cachepath = options.cachepath
    db = None
    seq_no = get_seq_no(backend)
    if os.path.exists(cachepath + '.params'):
        param = load_params(cachepath)
        if param['seq_no'] < seq_no:
            log.info('Ignoring locally cached metadata (outdated).')
            param = backend.lookup('s3ql_metadata')
        elif param['seq_no'] > seq_no:
            print('File system not unmounted cleanly, need to run fsck before upgrade.')
            print(get_old_rev_msg(param['revision'], 'fsck.s3ql'))
            raise QuietError()
        else:
            log.info('Using cached metadata.')
            db = Connection(cachepath + '.db')
    else:
        param = backend.lookup('s3ql_metadata')

    # Check for unclean shutdown
    if param['seq_no'] < seq_no:
        print(textwrap.fill(textwrap.dedent('''\
            Backend reports that fs is still mounted. If this is not the case, the file system may
            have not been unmounted cleanly or the data from the most-recent mount may have not yet
            propagated through the backend. In the later case, waiting for a while should fix the
            problem, in the former case you should try to run fsck on the computer where the file
            system has been mounted most recently.
            ''')))

        print(get_old_rev_msg(param['revision'], 'fsck.s3ql'))
        raise QuietError()

    # Check that the fs itself is clean
    if param['needs_fsck']:
        print('File system is damaged, need to run fsck before upgrade.')
        print(get_old_rev_msg(param['revision'], 'fsck.s3ql'))
        raise QuietError()

    # Check revision
    if param['revision'] < CURRENT_FS_REV-1:
        print(textwrap.dedent('''
            File system revision too old to upgrade!

            You need to use an older S3QL version to upgrade to a more recent
            revision before you can use this version to upgrade to the newest
            revision.
            '''))
        print(get_old_rev_msg(param['revision'] + 1, 's3qladm'))
        raise QuietError()

    elif param['revision'] >= CURRENT_FS_REV:
        print('File system already at most-recent revision')
        return

    print(textwrap.dedent('''
        I am about to update the file system to the newest revision.
        You will not be able to access the file system with any older version
        of S3QL after this operation.

        You should make very sure that this command is not interrupted and
        that no one else tries to mount, fsck or upgrade the file system at
        the same time.

        '''))

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    if not db:
        with monkeypatch_metadata_retrieval():
            db = download_metadata(backend, cachepath + '.db')

    log.info('Upgrading from revision %d to %d...', param['revision'], CURRENT_FS_REV)

    param['revision'] = CURRENT_FS_REV
    param['last-modified'] = time.time()
    param['seq_no'] += 1

    # Upgrade
    for name in ('atime', 'mtime', 'ctime'):
        db.execute('ALTER TABLE inodes ADD COLUMN {time}_ns '
                   'INT NOT NULL DEFAULT 0'.format(time=name))
        db.execute('UPDATE inodes SET {time}_ns = {time} * 1e9'.format(time=name))

    dump_and_upload_metadata(backend, db, param)
    backend['s3ql_seq_no_%d' % param['seq_no']] = b'Empty'

    # Declare local metadata as outdated so that it won't be used. It still
    # contains the old [acm]time columns which are NON NULL, and would prevent
    # us from inserting new rows.
    param['seq_no'] = 0
    save_params(cachepath, param)

    print('File system upgrade complete.')


@contextmanager
def monkeypatch_metadata_retrieval():
    create_tables_bak = metadata.create_tables
    @functools.wraps(metadata.create_tables)
    def create_tables(conn):
        create_tables_bak(conn)
        conn.execute('DROP TABLE inodes')
        conn.execute("""
           CREATE TABLE inodes (
           id        INTEGER PRIMARY KEY AUTOINCREMENT,
           uid       INT NOT NULL,
           gid       INT NOT NULL,
           mode      INT NOT NULL,
           mtime     REAL NOT NULL,
           atime     REAL NOT NULL,
           ctime     REAL NOT NULL,
           refcount  INT NOT NULL,
           size      INT NOT NULL DEFAULT 0,
           rdev      INT NOT NULL DEFAULT 0,
           locked    BOOLEAN NOT NULL DEFAULT 0
        )""")
    metadata.create_tables = create_tables

    DUMP_SPEC_bak = metadata.DUMP_SPEC[2]
    metadata.DUMP_SPEC[2] = ('inodes', 'id', (('id', INTEGER, 1),
                                              ('uid', INTEGER),
                                              ('gid', INTEGER),
                                              ('mode', INTEGER),
                                              ('mtime', TIME),
                                              ('atime', TIME),
                                              ('ctime', TIME),
                                              ('size', INTEGER),
                                              ('rdev', INTEGER),
                                              ('locked', INTEGER),
                                              ('refcount', INTEGER)))

    try:
        yield
    finally:
        metadata.DUMP_SPEC[2] = DUMP_SPEC_bak
        metadata.create_tables = create_tables_bak

if __name__ == '__main__':
    main(sys.argv[1:])
