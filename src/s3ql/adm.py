'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError, setup_logging
from . import CURRENT_FS_REV, REV_VER_MAP, PICKLE_PROTOCOL, BUFSIZE
from .backends.comprenc import ComprencBackend
from .database import Connection
from .common import (get_backend_cachedir, get_seq_no, stream_write_bz2,
                     stream_read_bz2, is_mounted, AsyncFn, get_backend,
                     get_backend_factory, pretty_print_size, split_by_n,
                     handle_on_return)
from .metadata import restore_metadata, cycle_metadata, dump_metadata
from .parse_args import ArgumentParser
from datetime import datetime as Datetime
from getpass import getpass
from base64 import b64encode
from queue import Queue, Full as QueueFull
import os
import pickle
import shutil
import sys
import tempfile
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
    parser.add_fatal_warnings()

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
        with get_backend(options, plain=True) as backend:
            return clear(backend, options)

    if options.action == 'upgrade':
        return upgrade(options)

    with get_backend(options) as backend:
        if options.action == 'passphrase':
            return change_passphrase(backend)

        if options.action == 'download-metadata':
            return download_metadata(backend, options.storage_url)

def download_metadata(backend, storage_url):
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

    cachepath = get_backend_cachedir(storage_url, '.')
    for i in ('.db', '.params'):
        if os.path.exists(cachepath + i):
            raise QuietError('%s already exists, aborting.' % cachepath + i)

    param = backend.lookup(name)
    with tempfile.TemporaryFile() as tmpfh:
        def do_read(fh):
            tmpfh.seek(0)
            tmpfh.truncate()
            stream_read_bz2(fh, tmpfh)

        log.info('Downloading and decompressing %s...', name)
        backend.perform_read(do_read, name)

        log.info("Reading metadata...")
        tmpfh.seek(0)
        restore_metadata(tmpfh, cachepath + '.db')

    # Raise sequence number so that fsck.s3ql actually uses the
    # downloaded backup
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in backend.list('s3ql_seq_no_') ]
    param['seq_no'] = max(seq_nos) + 1
    with open(cachepath + '.params', 'wb') as fh:
        pickle.dump(param, fh, PICKLE_PROTOCOL)

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
    cachepath = get_backend_cachedir(options.storage_url, options.cachedir)
    for suffix in ('.db', '.params'):
        name = cachepath + suffix
        if os.path.exists(name):
            os.unlink(name)

    name = cachepath + '-cache'
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
          $ (cd s3ql-%(version)s; ./setup.py build_ext)
          $ s3ql-%(version)s/bin/%(prog)s <options>
        ''' % { 'version': REV_VER_MAP[rev],
                'prog': prog })


@handle_on_return
def upgrade(options, on_return):
    '''Upgrade file system to newest revision'''

    log.info('Getting file system parameters..')
    cachepath = get_backend_cachedir(options.storage_url, options.cachedir)

    backend_factory = get_backend_factory(options)
    backend = on_return.enter_context(backend_factory())

    # Check for cached metadata
    db = None
    seq_no = get_seq_no(backend)
    if os.path.exists(cachepath + '.params'):
        with open(cachepath + '.params', 'rb') as fh:
            param = pickle.load(fh)
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
        # Need to download metadata
        with tempfile.TemporaryFile() as tmpfh:
            def do_read(fh):
                tmpfh.seek(0)
                tmpfh.truncate()
                stream_read_bz2(fh, tmpfh)

            log.info("Downloading & uncompressing metadata...")
            backend.perform_read(do_read, "s3ql_metadata")

            log.info("Reading metadata...")
            tmpfh.seek(0)
            db = restore_metadata(tmpfh, cachepath + '.db')

    log.info('Upgrading from revision %d to %d...', param['revision'], CURRENT_FS_REV)

    param['revision'] = CURRENT_FS_REV
    param['last-modified'] = time.time()
    param['seq_no'] += 1

    # Ensure that there are backups of the master key
    if backend.passphrase is not None:
        data_pw = backend.passphrase
        backend.passphrase = backend.fs_passphrase
        for i in range(1,4):
            obj_id = 's3ql_passphrase_bak%d' % i
            if obj_id not in backend:
                backend[obj_id] = data_pw
        backend.passphrase = data_pw

    # Upgrade all objects, so that we can remove legacy conversion
    # routines in the next release.
    update_obj_metadata(backend, backend_factory, db, options.threads)

    log.info('Dumping metadata...')
    with tempfile.TemporaryFile() as fh:
        dump_metadata(db, fh)
        def do_write(obj_fh):
            fh.seek(0)
            stream_write_bz2(fh, obj_fh)
            return obj_fh

        log.info("Compressing and uploading metadata...")
        backend.store('s3ql_seq_no_%d' % param['seq_no'], b'Empty')
        obj_fh = backend.perform_write(do_write, "s3ql_metadata_new", metadata=param,
                                      is_compressed=True)

    log.info('Wrote %s of compressed metadata.', pretty_print_size(obj_fh.get_obj_size()))
    log.info('Cycling metadata backups...')
    cycle_metadata(backend)

    backend['s3ql_seq_no_%d' % param['seq_no']] = b'Empty'

    with open(cachepath + '.params', 'wb') as fh:
        pickle.dump(param, fh, PICKLE_PROTOCOL)

    log.info('Cleaning up local metadata...')
    db.execute('ANALYZE')
    db.execute('VACUUM')

    print('File system upgrade complete.')

    if backend.passphrase is not None:
        print('Please store the following master key in a safe location. It allows ',
              'decryption of the S3QL file system in case the storage objects holding ',
              'this information get corrupted:',
              '---BEGIN MASTER KEY---',
              ' '.join(split_by_n(b64encode(backend.passphrase).decode(), 4)),
              '---END MASTER KEY---',
              sep='\n')

@handle_on_return
def update_obj_metadata(backend, backend_factory, db,
                        thread_count, on_return):
    '''Upgrade metadata of storage objects'''

    plain_backend = backend.backend

    # No need to update sequence number, since we are going to
    # write out a new one after the upgrade.
    if backend.passphrase is None:
        extra_objects = { 's3ql_metadata' }
    else:
        extra_objects = { 's3ql_metadata',
                          's3ql_passphrase', 's3ql_passphrase_bak1',
                          's3ql_passphrase_bak2', 's3ql_passphrase_bak3' }

    for i in range(30):
        obj_id = 's3ql_metadata_bak_%d' % i
        if obj_id in plain_backend:
            extra_objects.add(obj_id)

    def yield_objects():
        for (id_,) in db.query('SELECT id FROM objects'):
            yield 's3ql_data_%d' % id_
        for obj_id in extra_objects:
            yield obj_id
    total = db.get_val('SELECT COUNT(id) FROM objects') + len(extra_objects)

    queue = Queue(maxsize=thread_count)
    threads = []
    for _ in range(thread_count):
        t = AsyncFn(upgrade_loop, queue, on_return.push(backend_factory()))
        # Don't wait for worker threads, gives deadlock if main thread
        # terminates with exception
        t.daemon = True
        t.start()
        threads.append(t)

    # Updating this value is prone to race conditions. However,
    # we don't care because this is for an approximate progress
    # output only.
    queue.rewrote_size = 0
    stamp = 0
    for (i, obj_id) in enumerate(yield_objects()):
        stamp2 = time.time()
        if stamp2 - stamp > 1:
            sys.stdout.write('\r..processed %d/%d objects (%.1f%%, %s rewritten)..'
                             % (i, total, i/total*100,
                                pretty_print_size(queue.rewrote_size)))
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

def upgrade_loop(queue, backend):
    plain_backend = backend.backend

    while True:
        obj_id = queue.get()
        if obj_id is None:
            break

        meta = plain_backend.lookup(obj_id)
        if meta.get('format_version', 0) == 2:
            continue

        # For important objects, we make a copy first (just to be safe)
        if not obj_id.startswith('s3ql_data'):
            plain_backend.copy(obj_id, 's3ql_pre2.13' + obj_id[4:])

        # When reading passphrase objects, we have to use the
        # "outer" password
        if obj_id.startswith('s3ql_passphrase'):
            data_pw = backend.passphrase
            backend.passphrase = backend.fs_passphrase

        meta = backend._convert_legacy_metadata(meta)
        if meta['encryption'] == 'AES':
            # Two statements to reduce likelihood of update races
            size = rewrite_legacy_object(backend, obj_id)
            queue.rewrote_size += size
        else:
            plain_backend.update_meta(obj_id, meta)

        if obj_id.startswith('s3ql_passphrase'):
            backend.passphrase = data_pw

def rewrite_legacy_object(backend, obj_id):
    with tempfile.TemporaryFile() as tmpfh:

        # Read object
        def do_read(fh):
            tmpfh.seek(0)
            tmpfh.truncate()
            while True:
                buf = fh.read(BUFSIZE)
                if not buf:
                    break
                tmpfh.write(buf)
            return fh.metadata

        meta = backend.perform_read(do_read, obj_id)

        # Write object
        def do_write(fh):
            tmpfh.seek(0)
            while True:
                buf = tmpfh.read(BUFSIZE)
                if not buf:
                    break
                fh.write(buf)
            return fh
        out_fh = backend.perform_write(do_write, obj_id, meta)

        return out_fh.get_obj_size()

if __name__ == '__main__':
    main(sys.argv[1:])
