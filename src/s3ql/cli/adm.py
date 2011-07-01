'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

import logging
from s3ql.common import (get_backend, QuietError, unlock_bucket,
                         cycle_metadata, dump_metadata, restore_metadata,
                         setup_logging, get_bucket_home)
from s3ql.backends import s3
from s3ql.parse_args import ArgumentParser
from s3ql import CURRENT_FS_REV
from getpass import getpass
import sys
from s3ql.backends.common import ChecksumError
import os
from s3ql.database import Connection
import tempfile
from datetime import datetime as Datetime
import textwrap
import shutil
import stat
import time
import cPickle as pickle

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
    subparsers.add_parser("delete", help="completely delete a bucket with all contents",
                          parents=[pparser])                                        
    subparsers.add_parser("download-metadata", 
                          help="Interactively download metadata backups. "
                               "Use only if you know what you are doing.",
                          parents=[pparser])    
                
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_homedir()
    parser.add_version()
    parser.add_ssl()
        
    options = parser.parse_args(args)
    
    if not os.path.exists(options.homedir):
        os.mkdir(options.homedir, 0700)
        
    return options

def main(args=None):
    '''Change or show S3QL file system parameters'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options, 'adm.log')

    with get_backend(options.storage_url, 
                     options.homedir, options.ssl) as (conn, bucketname):
        home = get_bucket_home(options.storage_url, options.homedir)
               
        if options.action == 'delete':
            return delete_bucket(conn, bucketname, home)

        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        
        bucket = conn.get_bucket(bucketname)
        
        try:
            unlock_bucket(options.homedir, options.storage_url, bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        if options.action == 'passphrase':
            return change_passphrase(bucket)

        if options.action == 'upgrade':
            return upgrade(bucket)

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
        
    log.info('Downloading %s...', name)
    
    home = get_bucket_home(storage_url, '.')
    for i in ('.db', '.params'):
        if os.path.exists(home + i):
            raise QuietError('%s already exists, aborting.' % home+i)
    
    fh = os.fdopen(os.open(home + '.db', os.O_RDWR | os.O_CREAT,
                           stat.S_IRUSR | stat.S_IWUSR), 'w+b')
    param = bucket.lookup(name)
    try:
        fh.close()
        db = Connection(home + '.db')
        fh = tempfile.TemporaryFile()
        bucket.fetch_fh(name, fh)
        fh.seek(0)
        log.info('Reading metadata...')
        restore_metadata(fh, db)
        fh.close()
    except:
        # Don't keep file if it doesn't contain anything sensible
        os.unlink(home + '.db')
        raise
    pickle.dump(param, open(home + '.params', 'wb'), 2)
    


def change_passphrase(bucket):
    '''Change bucket passphrase'''

    if 's3ql_passphrase' not in bucket:
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

def delete_bucket(conn, bucketname, home):
    print('I am about to delete the bucket %s with ALL contents.' % bucketname,
          'Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Deleting...')
    
    for suffix in ('.db', '.params'):
        name = home + suffix
        if os.path.exists(name):
            os.unlink(name)
            
    name = home + '-cache'
    if os.path.exists(name):
        shutil.rmtree(name)
        
    if bucketname in conn:
        conn.delete_bucket(bucketname, recursive=True)

    print('Bucket deleted.')
    if isinstance(conn, s3.Connection):
        print('Note that it may take a while until the removal becomes visible.')

def upgrade(bucket):
    '''Upgrade file system to newest revision'''

    # Access to protected member
    #pylint: disable=W0212
    log.info('Getting file system parameters..')
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    if not seq_nos:
        raise QuietError(textwrap.dedent(''' 
            File system revision too old to upgrade!
            
            You need to use an older S3QL version to upgrade to a more recent
            revision before you can use this version to upgrade to the newest
            revision.
            '''))                     
    seq_no = max(seq_nos)
    param = bucket.lookup('s3ql_metadata')

    # Check for unclean shutdown
    if param['seq_no'] < seq_no:
        if (bucket.read_after_write_consistent() and
            bucket.read_after_delete_consistent()):
            raise QuietError(textwrap.fill(textwrap.dedent('''\
                It appears that the file system is still mounted somewhere else. If this is not
                the case, the file system may have not been unmounted cleanly and you should try
                to run fsck on the computer where the file system has been mounted most recently.
                ''')))
        else:                
            raise QuietError(textwrap.fill(textwrap.dedent('''\
                It appears that the file system is still mounted somewhere else. If this is not the
                case, the file system may have not been unmounted cleanly or the data from the 
                most-recent mount may have not yet propagated through the backend. In the later case,
                waiting for a while should fix the problem, in the former case you should try to run
                fsck on the computer where the file system has been mounted most recently.
                ''')))    

    # Check that the fs itself is clean
    if param['needs_fsck']:
        raise QuietError("File system damaged, run fsck!")
    
    # Check revision
    if param['revision'] < CURRENT_FS_REV - 1:
        raise QuietError(textwrap.dedent(''' 
            File system revision too old to upgrade!
            
            You need to use an older S3QL version to upgrade to a more recent
            revision before you can use this version to upgrade to the newest
            revision.
            '''))

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
    log.info("Downloading & uncompressing metadata...")
    dbfile = tempfile.NamedTemporaryFile()
    db = Connection(dbfile.name, fast_mode=True)
    fh = tempfile.TemporaryFile()
    bucket.fetch_fh("s3ql_metadata", fh)
    fh.seek(0)
    log.info('Reading metadata...')
    restore_metadata(fh, db)
    fh.close()
    
    log.info('Upgrading from revision %d to %d...', CURRENT_FS_REV - 1,
             CURRENT_FS_REV)
    param['revision'] = CURRENT_FS_REV
    
    for (id_, mode, target) in db.query('SELECT id, mode, target FROM inodes'):
        if stat.S_ISLNK(mode):
            db.execute('UPDATE inodes SET size=? WHERE id=?',
                       (len(target), id_))
    
    # Increase metadata sequence no
    param['seq_no'] += 1
    bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
    for i in seq_nos:
        if i < param['seq_no'] - 5:
            del bucket['s3ql_seq_no_%d' % i ]

    # Upload metadata
    fh = tempfile.TemporaryFile()
    dump_metadata(fh, db)
    fh.seek(0)
    log.info("Uploading database..")
    cycle_metadata(bucket)
    param['last-modified'] = time.time() - time.timezone
    bucket.store_fh("s3ql_metadata", fh, param)
    fh.close()


if __name__ == '__main__':
    main(sys.argv[1:])
