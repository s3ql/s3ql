'''
adm.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError, setup_logging
from . import CURRENT_FS_REV, REV_VER_MAP
from .backends.comprenc import ComprencBackend
from .backends.common import NoSuchObject
from .database import Connection
from .common import (get_seq_no, is_mounted, get_backend, load_params,
                     save_params, handle_on_return, get_backend_factory,
                     AsyncFn)
from .metadata import dump_and_upload_metadata, download_metadata
from .parse_args import ArgumentParser
from datetime import datetime as Datetime
from getpass import getpass
from queue import Queue, Full as QueueFull
import os
import s3ql.backends.common
import s3ql.backends.s3c
import hashlib
import hmac
import struct
import shutil
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
        with get_backend(options, raw=True) as backend:
            return clear(backend, options)
    elif options.action == 'upgrade':
        return upgrade(options)

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

@handle_on_return
def upgrade(options, on_return):
    '''Upgrade file system to newest revision'''

    s3ql.backends.comprenc.UPGRADE_MODE = checksum_basic_mapping_old
    s3ql.backends.s3c.UPGRADE_MODE = checksum_basic_mapping_old

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
        db = download_metadata(backend, cachepath + '.db')

    log.info('Upgrading from revision %d to %d...', param['revision'], CURRENT_FS_REV)

    update_obj_metadata(backend, backend_factory, db, options)

    param['revision'] = CURRENT_FS_REV
    param['last-modified'] = time.time()
    param['seq_no'] += 1

    dump_and_upload_metadata(backend, db, param)
    backend['s3ql_seq_no_%d' % param['seq_no']] = b'Empty'

    print('File system upgrade complete.')


def update_obj_metadata(backend, backend_factory, db, options):
    '''Upgrade metadata of storage objects'''

    # No need to update sequence number, since we are going to
    # write out a new one after the upgrade.
    extra_objects = { 's3ql_metadata',
                      's3ql_passphrase', 's3ql_passphrase_bak1',
                      's3ql_passphrase_bak2', 's3ql_passphrase_bak3' }
    extra_objects |= { 's3ql_metadata_bak_%d' % i for i in range(30) }

    def yield_objects():
        for obj_id in extra_objects:
            yield obj_id
        for (id_,) in db.query('SELECT id FROM objects'):
            yield 's3ql_data_%d' % id_
    total = db.get_val('SELECT COUNT(id) FROM objects') + len(extra_objects)

    queue = Queue(maxsize=options.threads)
    threads = []
    for _ in range(options.threads):
        t = AsyncFn(upgrade_loop, queue, options, backend_factory)
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)

    stamp = time.time()
    for (i, obj_id) in enumerate(yield_objects()):
        stamp2 = time.time()
        if stamp2 - stamp > 1:
            sys.stdout.write('\r..processed %d/%d objects (%d%%)..'
                             % (i, total, i/total*100))
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

def upgrade_loop(queue, options, backend_factory):

    with backend_factory() as backend:
        while True:
            key = queue.get()
            if key is None:
                break

            # When reading passphrase objects, we have to use the
            # "outer" password
            if key.startswith('s3ql_passphrase'):
                data_pw = backend.passphrase
                backend.passphrase = options.fs_passphrase
            try:
                try:
                    meta = backend.lookup(key)
                except NoSuchObject:
                    continue

                if meta['needs_reupload']:
                    del meta['needs_reupload']
                    backend.update_meta(key, meta)
            finally:
                if key.startswith('s3ql_passphrase'):
                    backend.passphrase = data_pw

def checksum_basic_mapping_old(metadata, key=None):
    '''Compute checksum for mapping of elementary types

    Keys of *d* must be strings. Values of *d* must be of elementary
    type (i.e., `str`, `bytes`, `int`, `float`, `complex`, `bool` or
    None). If there is a key named ``signature``, then it is excluded
    from the checksum computation.

    If *key* is None, compute MD5. Otherwise compute HMAC using *key*.
    '''

    # In order to compute a safe checksum, we need to convert each object to a
    # unique representation (i.e, we can't use repr(), as it's output may change
    # in the future). Furthermore, we have to make sure that the representation
    # is theoretically reversible, or there is a potential for collision
    # attacks.

    if key is None:
        chk = hashlib.md5()
    else:
        chk = hmac.new(key, digestmod=hashlib.sha256)

    for mkey in sorted(metadata.keys()):
        assert isinstance(mkey, str)
        if mkey == 'signature':
            continue
        chk.update(mkey.encode('utf-8'))
        val = metadata[mkey]
        if isinstance(val, str):
            val = b'\0s' + val.encode('utf-8') + b'\0'
        elif val is None:
            val = b'\0n'
        elif isinstance(val, int):
            val = b'\0i' + ('%d' % val).encode() + b'\0'
        elif isinstance(val, bool):
            val = b'\0t' if val else b'\0f'
        elif isinstance(val, float):
            val = b'\0d' + struct.pack('<d', val)
        elif isinstance(val, (bytes, bytearray)):
            assert len(val).bit_length() <= 32
            val = b'\0b' + struct.pack('<I', len(val))
        else:
            raise ValueError("Don't know how to checksum %s instances" % type(val))
        chk.update(val)

    return chk.digest()

if __name__ == '__main__':
    main(sys.argv[1:])
