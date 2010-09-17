'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import logging
from s3ql.common import (get_backend, QuietError, unlock_bucket,
                         cycle_metadata, dump_metadata, restore_metadata,
                         setup_logging, get_bucket_home)
from s3ql.argparse import ArgumentParser
from s3ql import CURRENT_FS_REV
#from s3ql.mkfs import create_indices
from getpass import getpass
import sys
from s3ql.backends import s3, local, sftp
from s3ql.backends.common import ChecksumError
import os
from s3ql.database import Connection
import tempfile
import textwrap
import shutil

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
            
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_homedir()
    parser.add_version()

    options = parser.parse_args(args)
    
    if not os.path.exists(options.homedir):
        os.mkdir(options.homedir, 0700)
        
    return options

def main(args=None):
    '''Change or show S3QL file system parameters'''

    if args is None:
        args = sys.argv[1:]

    try:
        import psyco
        psyco.profile()
    except ImportError:
        pass

    options = parse_args(args)
    setup_logging(options, 'adm.log')

    with get_backend(options.storage_url, options.homedir) as (conn, bucketname):
        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)
        home = get_bucket_home(options.storage_url, options.homedir)
        
        if options.action == 'delete':
            return delete_bucket(conn, bucketname, home)

        try:
            unlock_bucket(options.homedir, options.storage_url, bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        if options.action == 'passphrase':
            return change_passphrase(bucket)

        if options.action == 'upgrade':
            return upgrade(bucket)


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
        raise QuietError(1)

    log.info('Deleting...')
    conn.delete_bucket(bucketname, recursive=True)

    for suffix in ('.db', '.params'):
        name = home + suffix
        if os.path.exists(name):
            os.unlink(name)
            
    name = home + '-cache'
    if os.path.exists(name):
        shutil.rmtree(name)
    
    print('Bucket deleted.')
    if isinstance(conn, s3.Connection):
        print('Note that it may take a while until the removal becomes visible.')

def upgrade(bucket):
    '''Upgrade file system to newest revision'''

    # Access to protected member
    #pylint: disable=W0212
            
    print(textwrap.dedent('''
        I am about to update the file system to the newest revision. Note that:
        
         - You should make very sure that this command is not interrupted and
           that no one else tries to mount, fsck or upgrade the file system at
           the same time.
        
         - You will not be able to access the file system with any older version
           of S3QL after this operation. 
           
         - The upgrade can only be done from the *previous* revision. If you
           skipped an intermediate revision, you have to use an intermediate
           version of S3QL to first upgrade the file system to the previous
           revision.
        '''))

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError(1)

    log.info('Getting file system parameters..')
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    if not seq_nos:
        raise QuietError('File system revision too old to upgrade.')
    seq_no = max(seq_nos)
    param = bucket.lookup('s3ql_metadata')

    # Check revision
    if param['revision'] < CURRENT_FS_REV - 1:
        raise QuietError('File system revision too old to upgrade.')

    elif param['revision'] >= CURRENT_FS_REV:
        print('File system already at most-recent revision')
        return

    # Check that the fs itself is clean
    if param['needs_fsck']:
        raise QuietError("File system damaged, run fsck!")

    # Check for unclean shutdown on other computer
    if param['seq_no'] < seq_no and False:
        if isinstance(bucket, s3.Bucket):
            raise QuietError(textwrap.fill(textwrap.dedent('''
                It appears that the file system is still mounted somewhere else. If this is not
                the case, the file system may have not been unmounted cleanly or the data from
                the most-recent mount may have not yet propagated through S3. In the later case,
                waiting for a while should fix the problem, in the former case you should try to
                run fsck on the computer where the file system has been mounted most recently.
                ''')))
        else:
            raise QuietError(textwrap.fill(textwrap.dedent('''
                It appears that the file system is still mounted somewhere else. If this is not
                the case, the file system may have not been unmounted cleanly and you should try
                to run fsck on the computer where the file system has been mounted most recently.
                ''')))
    elif param['seq_no'] > seq_no:
        raise RuntimeError('param[seq_no] > seq_no, this should not happen.')

    # Download metadata
    log.info("Downloading & uncompressing metadata...")
    if param['DB-Format'] == 'dump':
        dbfile = tempfile.NamedTemporaryFile()
        db = Connection(dbfile.name)
        fh = tempfile.TemporaryFile()
        bucket.fetch_fh("s3ql_metadata", fh)
        fh.seek(0)
        log.info('Reading metadata...')
        restore_metadata(fh, db)
        fh.close()
    elif param['DB-Format'] == 'sqlite':
        dbfile = tempfile.NamedTemporaryFile()
        bucket.fetch_fh("s3ql_metadata", dbfile)
        dbfile.flush()
        db = Connection(dbfile.name)
    else:
        raise RuntimeError('Unsupported DB format: %s' % param['DB-Format'])
    
    #log.info("Indexing...")
    #create_indices(dbcm)
    #dbcm.execute('ANALYZE')
    
    log.info('Upgrading from revision %d to %d...', CURRENT_FS_REV - 1,
             CURRENT_FS_REV)
    param['revision'] = CURRENT_FS_REV
    
    # Update backend directory structure
    if isinstance(bucket, local.Bucket): 
        os.rename('%s/s3ql_data' % bucket.name, '%s/s3ql_data_' % bucket.name)
    elif isinstance(bucket, sftp.Bucket):
        bucket.conn.sftp.rename('%s/s3ql_data' % bucket.name, '%s/s3ql_data_' % bucket.name)

    # Add compr_size column
    if param['DB-Format'] == 'sqlite':
        db.execute('ALTER TABLE objects ADD COLUMN compr_size INT')
        db.execute('ALTER TABLE inodes ADD COLUMN locked BOOLEAN NOT NULL DEFAULT 0')
    
    # Add missing values for compr_size
    log.info('Requesting all object sizes, this may take some time...')
    if isinstance(bucket, (local.Bucket, sftp.Bucket)):
        if isinstance(bucket, local.Bucket):
            get_size = os.path.getsize
        elif isinstance(bucket, sftp.Bucket):
            get_size = lambda p: bucket.conn.sftp.lstat(p).st_size
        
        for (no, (obj_id,)) in enumerate(db.query('SELECT id FROM objects')):
            if no != 0 and no % 5000 == 0:
                log.info('Checked %d objects so far..', no)
            
            path = bucket._key_to_path('s3ql_data_%d' % obj_id) + '.dat'
            db.execute('UPDATE objects SET compr_size=? WHERE id=?',
                         (get_size(path), obj_id))
                                  
    elif isinstance(bucket, s3.Bucket):
        with bucket._get_boto() as boto:
            for bkey in boto.list('s3ql_data_'):
                obj_id = int(bkey.name[10:])
                db.execute('UPDATE objects SET compr_size=? WHERE id=?',
                             (bkey.size, obj_id))
                
            if db.has_val('SELECT 1 FROM objects WHERE compr_size IS NULL'):
                log.warn('Could not determine sizes for all S3 objects, '
                         's3qlstat output will be incorrect.')
    
    # Increase metadata sequence no
    param['seq_no'] += 1
    bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')
    for i in seq_nos:
        if i < param['seq_no'] - 5:
            del bucket['s3ql_seq_no_%d' % i ]

    # Upload metadata in dump format, so that we have the newest
    # table definitions on the next mount.
    param['DB-Format'] = 'dump'
    fh = tempfile.TemporaryFile()
    dump_metadata(fh, db)
    fh.seek(0)
    log.info("Uploading database..")
    cycle_metadata(bucket)
    bucket.store_fh("s3ql_metadata", fh, param)
    fh.close()


if __name__ == '__main__':
    main(sys.argv[1:])
