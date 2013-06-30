'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, QuietError, setup_logging
from . import CURRENT_FS_REV, REV_VER_MAP
from .backends.common import BetterBackend, get_backend, DanglingStorageURLError
from .common import (get_backend_cachedir, get_seq_no, stream_write_bz2,
                     stream_read_bz2, PICKLE_PROTOCOL)
from .metadata import restore_metadata, cycle_metadata, dump_metadata
from .parse_args import ArgumentParser
from datetime import datetime as Datetime
from getpass import getpass
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
    subparsers.add_parser("upgrade", help="upgrade file system to newest revision",
                          parents=[pparser])
    subparsers.add_parser("clear", help="delete file system and all data",
                          parents=[pparser])
    subparsers.add_parser("download-metadata",
                          help="Interactively download metadata backups. "
                               "Use only if you know what you are doing.",
                          parents=[pparser])

    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_log()
    parser.add_authfile()
    parser.add_ssl()
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
    match = options.storage_url + ' /'
    with open('/proc/mounts', 'r') as fh:
        for line in fh:
            if line.startswith(match):
                raise QuietError('Can not work on mounted file system.')

    if options.action == 'clear':
        try:
            backend = get_backend(options, plain=True)
        except DanglingStorageURLError as exc:
            raise QuietError(str(exc)) from None
        return clear(backend,
                     get_backend_cachedir(options.storage_url, options.cachedir))

    try:
        backend = get_backend(options)
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc)) from None

    if options.action == 'upgrade':
        return upgrade(backend, get_backend_cachedir(options.storage_url,
                                                   options.cachedir))

    if options.action == 'passphrase':
        return change_passphrase(backend)

    if options.action == 'download-metadata':
        return download_metadata(backend, options.storage_url)


def download_metadata(backend, storage_url):
    '''Download old metadata backups'''

    backups = sorted(backend.list('s3ql_metadata_bak_'))

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

    if not isinstance(backend, BetterBackend) and backend.passphrase:
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
    backend.passphrase = data_pw

def clear(backend, cachepath):
    print('I am about to delete all data in %s.' % backend,
          'This includes any S3QL file systems as well as any other stored objects.',
          'Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Deleting...')

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
        was %(version)s. You can run this version's %(prog)s by executing:
        
          $ wget http://s3ql.googlecode.com/files/s3ql-%(version)s.tar.bz2
          $ tar xjf s3ql-%(version)s.tar.bz2
          $ (cd s3ql-%(version)s; ./setup.py build_ext)
          $ s3ql-%(version)s/bin/%(prog)s <options>
        ''' % { 'version': REV_VER_MAP[rev],
                'prog': prog })

def upgrade(backend, cachepath):
    '''Upgrade file system to newest revision'''

    log.info('Getting file system parameters..')

    seq_nos = list(backend.list('s3ql_seq_no_'))
    if (seq_nos[0].endswith('.meta')
        or seq_nos[0].endswith('.dat')):
        print(textwrap.dedent(''' 
            File system revision too old to upgrade!
            
            You need to use an older S3QL version to upgrade to a more recent
            revision before you can use this version to upgrade to the newest
            revision.
            '''))
        print(get_old_rev_msg(11 + 1, 's3qladm'))
        raise QuietError()
    seq_no = get_seq_no(backend)

    # Check for cached metadata
    if os.path.exists(cachepath + '.params'):
        with open(cachepath + '.params', 'rb') as fh:
            param = pickle.load(fh)
        if param['seq_no'] < seq_no:
            param = backend.lookup('s3ql_metadata')
        elif param['seq_no'] > seq_no:
            print('File system not unmounted cleanly, need to run fsck before upgrade.')
            print(get_old_rev_msg(param['revision'], 'fsck.s3ql'))
            raise QuietError()
        else:
            # Move local metadata out of the way before we overwrite it
            # (you never know...)
            if os.path.exists(cachepath + '.db.bak'):
                raise QuietError('Metadata backup already exists, did something go wrong?')
            os.rename(cachepath + '.db', cachepath + '.db.bak')
            shutil.copyfile(cachepath + '.params', cachepath + '.params.bak')
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
    if param['revision'] != 16:
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

    # For this upgrade, we just need to ensure that we don't use the locally
    # cached SQLite database (because some types have changed)
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
        
    log.info('Wrote %.2f MiB of compressed metadata.', obj_fh.get_obj_size() / 1024 ** 2)
    log.info('Cycling metadata backups...')
    cycle_metadata(backend)

    with open(cachepath + '.params', 'wb') as fh:
        pickle.dump(param, fh, PICKLE_PROTOCOL)

    log.info('Cleaning up local metadata...')
    db.execute('ANALYZE')
    db.execute('VACUUM')


if __name__ == '__main__':
    main(sys.argv[1:])

