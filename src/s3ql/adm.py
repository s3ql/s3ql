'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import CURRENT_FS_REV, REV_VER_MAP
from .backends.common import BetterBucket, get_bucket, NoSuchBucket
from .common import (QuietError, BUFSIZE, setup_logging, get_bucket_cachedir, get_seq_no, 
    stream_write_bz2, stream_read_bz2, CTRL_INODE)
from .database import Connection, NoSuchRowError
from .metadata import restore_metadata, cycle_metadata, dump_metadata, create_tables
from .parse_args import ArgumentParser
from datetime import datetime as Datetime
from getpass import getpass
from llfuse import ROOT_INODE
import cPickle as pickle
import logging
import lzma
import os
import shutil
import stat
import sys
import tempfile
import textwrap
import time

log = logging.getLogger("adm")

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Manage S3QL Buckets.",
        epilog=textwrap.dedent('''\
               Hint: run `%(prog)s <action> --help` to get help on the additional
               arguments that the different actions take.'''))

    pparser = ArgumentParser(add_help=False, epilog=textwrap.dedent('''\
               Hint: run `%(prog)s --help` to get help on other available actions and
               optional arguments that can be used with all actions.'''))
    pparser.add_storage_url()

    subparsers = parser.add_subparsers(metavar='<action>', dest='action',
                                       help='may be either of')
    subparsers.add_parser("passphrase", help="change bucket passphrase",
                          parents=[pparser])
    subparsers.add_parser("upgrade", help="upgrade file system to newest revision",
                          parents=[pparser])
    subparsers.add_parser("clear", help="delete all S3QL data from the bucket",
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
            bucket = get_bucket(options, plain=True)
        except NoSuchBucket as exc:
            raise QuietError(str(exc))
        return clear(bucket,
                     get_bucket_cachedir(options.storage_url, options.cachedir))

    try:
        bucket = get_bucket(options)
    except NoSuchBucket as exc:
        raise QuietError(str(exc))

    if options.action == 'upgrade':
        return upgrade(bucket, get_bucket_cachedir(options.storage_url,
                                                   options.cachedir))

    if options.action == 'passphrase':
        return change_passphrase(bucket)

    if options.action == 'download-metadata':
        return download_metadata(bucket, options.storage_url)


def download_metadata(bucket, storage_url):
    '''Download old metadata backups'''

    backups = sorted(bucket.list('s3ql_metadata_bak_'))

    if not backups:
        raise QuietError('No metadata backups found.')

    log.info('The following backups are available:')
    log.info('%3s  %-23s %-15s', 'No', 'Name', 'Date')
    for (i, name) in enumerate(backups):
        params = bucket.lookup(name)
        if 'last-modified' in params:
            date = Datetime.fromtimestamp(params['last-modified']).strftime('%Y-%m-%d %H:%M:%S')
        else:
            # (metadata might from an older fs revision)
            date = '(unknown)'

        log.info('%3d  %-23s %-15s', i, name, date)

    name = None
    while name is None:
        buf = raw_input('Enter no to download: ')
        try:
            name = backups[int(buf.strip())]
        except:
            log.warn('Invalid input')

    cachepath = get_bucket_cachedir(storage_url, '.')
    for i in ('.db', '.params'):
        if os.path.exists(cachepath + i):
            raise QuietError('%s already exists, aborting.' % cachepath + i)

    param = bucket.lookup(name)
    try:
        log.info('Downloading and decompressing %s...', name)
        def do_read(fh):
            tmpfh = tempfile.TemporaryFile()
            stream_read_bz2(fh, tmpfh)
            return tmpfh
        tmpfh = bucket.perform_read(do_read, name)
        os.close(os.open(cachepath + '.db.tmp', os.O_RDWR | os.O_CREAT | os.O_TRUNC,
                         stat.S_IRUSR | stat.S_IWUSR))
        db = Connection(cachepath + '.db.tmp', fast_mode=True)
        log.info("Reading metadata...")
        tmpfh.seek(0)
        restore_metadata(tmpfh, db)
        db.close()
        os.rename(cachepath + '.db.tmp', cachepath + '.db')

    except:
        # Don't keep file if it doesn't contain anything sensible
        os.unlink(cachepath + '.db.tmp')
        raise

    # Raise sequence number so that fsck.s3ql actually uses the
    # downloaded backup
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    param['seq_no'] = max(seq_nos) + 1
    pickle.dump(param, open(cachepath + '.params', 'wb'), 2)

def change_passphrase(bucket):
    '''Change bucket passphrase'''

    if not isinstance(bucket, BetterBucket) and bucket.passphrase:
        raise QuietError('Bucket is not encrypted.')

    data_pw = bucket.passphrase

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()

    bucket.passphrase = wrap_pw
    bucket['s3ql_passphrase'] = data_pw
    bucket.passphrase = data_pw

def clear(bucket, cachepath):
    print('I am about to delete the S3QL file system in %s.' % bucket,
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

    bucket.clear()

    log.info('File system deleted.')
    log.info('Note: it may take a while for the removals to propagate through the backend.')

def get_old_rev_msg(rev, prog):
    return textwrap.dedent('''\
        The last S3QL version that supported this file system revision
        was %(version)s. You can run this version's %(prog)s by executing:
        
          $ wget http://s3ql.googlecode.com/files/s3ql-%(version)s.tar.bz2
          $ tar xjf s3ql-%(version)s.tar.bz2
          $ s3ql-%(version)s/bin/%(prog)s <options>
        ''' % { 'version': REV_VER_MAP[rev],
                'prog': prog })

def upgrade(bucket, cachepath):
    '''Upgrade file system to newest revision'''

    log.info('Getting file system parameters..')

    seq_nos = list(bucket.list('s3ql_seq_no_'))
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
    seq_no = get_seq_no(bucket)

    # Check for cached metadata
    db = None
    if os.path.exists(cachepath + '.params'):
        param = pickle.load(open(cachepath + '.params', 'rb'))
        if param['seq_no'] < seq_no:
            log.info('Ignoring locally cached metadata (outdated).')
            param = bucket.lookup('s3ql_metadata')
        else:
            log.info('Using cached metadata.')
            db = Connection(cachepath + '.db')
    else:
        param = bucket.lookup('s3ql_metadata')

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
        raise QuietError("File system damaged, run fsck first!")

    # Check revision
    if param['revision'] < CURRENT_FS_REV - 1:
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

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    # Download metadata
    if not db:
        log.info("Downloading & uncompressing metadata...")
        def do_read(fh):
            tmpfh = tempfile.TemporaryFile()
            stream_read_bz2(fh, tmpfh)
            return tmpfh
        tmpfh = bucket.perform_read(do_read, "s3ql_metadata")
        os.close(os.open(cachepath + '.db.tmp', os.O_RDWR | os.O_CREAT | os.O_TRUNC,
                         stat.S_IRUSR | stat.S_IWUSR))
        db = Connection(cachepath + '.db.tmp', fast_mode=True)
        log.info("Reading metadata...")
        tmpfh.seek(0)
        restore_metadata(tmpfh, db)
        db.close()
        os.rename(cachepath + '.db.tmp', cachepath + '.db')
        db = Connection(cachepath + '.db')


    log.info('Upgrading from revision %d to %d...', param['revision'], CURRENT_FS_REV)

    if 'max_obj_size' not in param:
        param['max_obj_size'] = param['blocksize']
    if 'blocksize' in param:
        del param['blocksize']

    db.execute('UPDATE inodes SET mtime=mtime+?, ctime=ctime+?, atime=atime+?',
               (time.timezone, time.timezone, time.timezone))
    
    param['revision'] = CURRENT_FS_REV
    param['last-modified'] = time.time()

    cycle_metadata(bucket)
    log.info('Dumping metadata...')
    fh = tempfile.TemporaryFile()
    dump_metadata(db, fh)
    def do_write(obj_fh):
        fh.seek(0)
        stream_write_bz2(fh, obj_fh)
        return obj_fh

    log.info("Compressing and uploading metadata...")
    bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
    obj_fh = bucket.perform_write(do_write, "s3ql_metadata", metadata=param,
                                  is_compressed=True)
    log.info('Wrote %.2f MB of compressed metadata.', obj_fh.get_obj_size() / 1024 ** 2)
    pickle.dump(param, open(cachepath + '.params', 'wb'), 2)

    db.execute('ANALYZE')
    db.execute('VACUUM')


if __name__ == '__main__':
    main(sys.argv[1:])

