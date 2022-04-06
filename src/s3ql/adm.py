'''
adm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError, setup_logging
from . import CURRENT_FS_REV, REV_VER_MAP
from .backends.comprenc import ComprencBackend
from .deltadump import INTEGER, BLOB
from .database import Connection
from base64 import b64decode
from .common import (get_seq_no, is_mounted, get_backend, load_params,
                     save_params, handle_on_return, get_backend_factory,
                     AsyncFn)
from . import metadata
from .parse_args import ArgumentParser
from datetime import datetime as Datetime
from getpass import getpass
from queue import Queue, Full as QueueFull
import os
import re
import shutil
import sys
from unittest import mock
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

    subparsers = parser.add_subparsers(metavar='<action>', dest='action',
                                       help='may be either of')
    subparsers.add_parser("passphrase", help="change file system passphrase",
                          parents=[pparser])
    sparser = subparsers.add_parser("clear", help="delete file system and all data",
                                    parents=[pparser])
    sparser.add_argument("--threads", type=int, default=20,
                        help='Number of threads to use')
    subparsers.add_parser("recover-key", help="Recover master key from offline copy.",
                          parents=[pparser])
    subparsers.add_parser("download-metadata",
                          help="Interactively download metadata backups. "
                               "Use only if you know what you are doing.",
                          parents=[pparser])
    sparser = subparsers.add_parser("upgrade", help="upgrade file system to newest revision",
                          parents=[pparser])
    sparser.add_argument("--threads", type=int, default=20,
                        help='Number of threads to use')

    parser.add_storage_url()
    parser.add_debug()
    parser.add_quiet()
    parser.add_log()
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
        return clear(options)
    elif options.action == 'upgrade':
        return upgrade(options)

    if options.action == 'recover-key':
        with get_backend(options, raw=True) as backend:
            return recover(backend, options)

    with get_backend(options) as backend:
        if options.action == 'passphrase':
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
    metadata.download_metadata(backend, cachepath + ".db", name)

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

    print(textwrap.dedent('''\
       NOTE: If your password has been compromised already, then changing
       it WILL NOT PROTECT YOUR DATA, because an attacker may have already
       retrieved the master key.
       '''))
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

def recover(backend, options):
    print("Enter master key (should be 11 blocks of 4 characters each): ")
    data_pw = sys.stdin.readline()
    data_pw = re.sub(r'\s+', '', data_pw)
    try:
        data_pw = b64decode(data_pw)
    except ValueError:
        raise QuietError("Malformed master key. Expected valid base64.")

    if len(data_pw) != 32:
        raise QuietError("Malformed master key. Expected length 32, got %d."
                         % len(data_pw))

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    wrap_pw = wrap_pw.encode('utf-8')

    backend = ComprencBackend(wrap_pw, ('lzma', 2), backend)
    backend['s3ql_passphrase'] = data_pw
    backend['s3ql_passphrase_bak1'] = data_pw
    backend['s3ql_passphrase_bak2'] = data_pw
    backend['s3ql_passphrase_bak3'] = data_pw

@handle_on_return
def clear(options, on_return):
    backend_factory = lambda: options.backend_class(options)
    backend = on_return.enter_context(backend_factory())

    print('I am about to DELETE ALL DATA in %s.' % backend,
          'This includes not just S3QL file systems but *all* stored objects.',
          'Depending on the storage service, it may be neccessary to run this command',
          'several times to delete all data, and it may take a while until the ',
          'removal becomes effective.',
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

    queue = Queue(maxsize=options.threads)

    def removal_loop():
        with backend_factory() as backend:
            while True:
                key = queue.get()
                if key is None:
                    return
                backend.delete(key)

    threads = []
    for _ in range(options.threads):
        t = AsyncFn(removal_loop)
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)

    stamp = time.time()
    for (i, obj_id) in enumerate(backend.list()):
        stamp2 = time.time()
        if stamp2 - stamp > 1:
            sys.stdout.write('\r..deleted %d objects so far..' % i)
            sys.stdout.flush()
            stamp = stamp2

            # Terminate early if any thread failed with an exception
            for t in threads:
                if not t.is_alive():
                    t.join_and_raise()

        # Avoid blocking if all threads terminated
        while True:
            try:
                queue.put(obj_id, timeout=1)
            except QueueFull:
                pass
            else:
                break
            for t in threads:
                if not t.is_alive():
                    t.join_and_raise()

    queue.maxsize += len(threads)
    for t in threads:
        queue.put(None)

    for t in threads:
        t.join_and_raise()

    sys.stdout.write('\n')
    log.info('All visible objects deleted.')



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


def create_old_tables(conn):
    conn.execute("""
    CREATE TABLE objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL,
        size      INT NOT NULL
    )""")
    conn.execute("""
    CREATE TABLE blocks (
        id        INTEGER PRIMARY KEY,
        hash      BLOB(32) UNIQUE,
        refcount  INT,
        size      INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id)
    )""")
    conn.execute("""
    CREATE TABLE inodes (
        -- id has to specified *exactly* as follows to become
        -- an alias for the rowid.
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime_ns  INT NOT NULL,
        atime_ns  INT NOT NULL,
        ctime_ns  INT NOT NULL,
        refcount  INT NOT NULL,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )""")
    conn.execute("""
    CREATE TABLE inode_blocks (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        block_id    INTEGER NOT NULL REFERENCES blocks(id),
        PRIMARY KEY (inode, blockno)
    )""")
    conn.execute("""
    CREATE TABLE symlink_targets (
        inode     INTEGER PRIMARY KEY REFERENCES inodes(id),
        target    BLOB NOT NULL
    )""")
    conn.execute("""
    CREATE TABLE names (
        id     INTEGER PRIMARY KEY,
        name   BLOB NOT NULL,
        refcount  INT NOT NULL,
        UNIQUE (name)
    )""")
    conn.execute("""
    CREATE TABLE contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name_id   INT NOT NULL REFERENCES names(id),
        inode     INT NOT NULL REFERENCES inodes(id),
        parent_inode INT NOT NULL REFERENCES inodes(id),

        UNIQUE (parent_inode, name_id)
    )""")
    conn.execute("""
    CREATE TABLE ext_attributes (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        name_id   INTEGER NOT NULL REFERENCES names(id),
        value     BLOB NOT NULL,

        PRIMARY KEY (inode, name_id)
    )""")
    conn.execute("""
    CREATE VIEW contents_v AS
    SELECT * FROM contents JOIN names ON names.id = name_id
    """)
    conn.execute("""
    CREATE VIEW ext_attributes_v AS
    SELECT * FROM ext_attributes JOIN names ON names.id = name_id
    """)

OLD_DUMP_SPEC = [
             ('objects', 'id', (('id', INTEGER, 1),
                                ('size', INTEGER),
                                ('refcount', INTEGER))),

             ('blocks', 'id', (('id', INTEGER, 1),
                             ('hash', BLOB, 32),
                             ('size', INTEGER),
                             ('obj_id', INTEGER, 1),
                             ('refcount', INTEGER))),

             ('inodes', 'id', (('id', INTEGER, 1),
                               ('uid', INTEGER),
                               ('gid', INTEGER),
                               ('mode', INTEGER),
                               ('mtime_ns', INTEGER),
                               ('atime_ns', INTEGER),
                               ('ctime_ns', INTEGER),
                               ('size', INTEGER),
                               ('rdev', INTEGER),
                               ('locked', INTEGER),
                               ('refcount', INTEGER))),

             ('inode_blocks', 'inode, blockno',
              (('inode', INTEGER),
               ('blockno', INTEGER, 1),
               ('block_id', INTEGER, 1))),

             ('symlink_targets', 'inode', (('inode', INTEGER, 1),
                                           ('target', BLOB))),

             ('names', 'id', (('id', INTEGER, 1),
                              ('name', BLOB),
                              ('refcount', INTEGER))),

             ('contents', 'parent_inode, name_id',
              (('name_id', INTEGER, 1),
               ('inode', INTEGER, 1),
               ('parent_inode', INTEGER))),

             ('ext_attributes', 'inode', (('inode', INTEGER),
                                          ('name_id', INTEGER),
                                          ('value', BLOB))),
]
    
@handle_on_return
def upgrade(options, on_return):
    '''Upgrade file system to newest revision'''

    log.info('Getting file system parameters..')

    backend_factory = get_backend_factory(options)
    backend = on_return.enter_context(backend_factory())

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
        the same time. You may interrupt the update with Ctrl+C and resume
        at a later time, but the filesystem will not be in a usable state
        until the upgrade is complete.
        '''))

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')
    sys.stdout.flush()

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    if not db:
        with mock.patch.object(metadata, 'create_tables', create_old_tables), \
             mock.patch.object(metadata, 'DUMP_SPEC', OLD_DUMP_SPEC):
            db = metadata.download_metadata(backend, cachepath + '.db')

    log.info('Upgrading from revision %d to %d...', param['revision'], CURRENT_FS_REV)

    param['revision'] = CURRENT_FS_REV
    param['last-modified'] = time.time()
    param['seq_no'] += 1

    # Altering table per https://sqlite.org/lang_altertable.html, section 7
    # foreign_keys pragma should be off already.
    db.execute("""
    CREATE TABLE objects_new (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        hash        BLOB(32) UNIQUE,
        refcount    INT NOT NULL,
        phys_size   INT NOT NULL,
        length      INT NOT NULL
    )""")
    db.execute("""
    CREATE TABLE inode_blocks_new (
        inode     INTEGER NOT NULL REFERENCES inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES objects(id),
        PRIMARY KEY (inode, blockno)
    )""")
                
    object_refcounts = db.get_val('SELECT COUNT(id) FROM objects WHERE refcount != 1')
    if object_refcounts:
        raise RuntimeError(
            f'`objects` table has refcount != 1 in {object_refcounts} rows!')
    
    db.execute('INSERT INTO objects_new (id, hash, refcount, phys_size, length) '
               'SELECT objects.id, blocks.hash, blocks.refcount, objects.size, blocks.size '
               'FROM blocks LEFT JOIN objects ON blocks.obj_id = objects.id')
    
    db.execute('INSERT INTO inode_blocks_new (inode, blockno, obj_id) '
               'SELECT inode, blockno, obj_id '
               'FROM inode_blocks LEFT JOIN blocks ON (block_id = blocks.id)')
    null_rows = db.get_val('SELECT COUNT(*) FROM inode_blocks_new '
                           'WHERE obj_id IS NULL')
    if null_rows:
        raise RuntimeError(f'`inode_blocks_new` table has {null_rows} NULL values')
    
    db.execute('DROP TABLE inode_blocks')
    db.execute('DROP TABLE blocks')
    db.execute('DROP TABLE objects')
    db.execute('ALTER TABLE inode_blocks_new RENAME TO inode_blocks')
    db.execute('ALTER TABLE objects_new RENAME TO objects')

    log.info('Cleaning up local metadata...')
    db.execute('ANALYZE')
    db.execute('VACUUM')
    
    metadata.dump_and_upload_metadata(backend, db, param)

    backend['s3ql_seq_no_%d' % param['seq_no']] = b'Empty'

    print('File system upgrade complete.')



if __name__ == '__main__':
    main(sys.argv[1:])
