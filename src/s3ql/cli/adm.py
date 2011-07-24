'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import

import logging
from s3ql.common import (get_backend, QuietError, unlock_bucket,
                         restore_metadata,
                         cycle_metadata, dump_metadata, create_tables,
                         setup_logging, get_bucket_cachedir)
from s3ql.backends import s3
from s3ql.backends.local import Bucket as LocalBucket
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
    parser.add_log()
    parser.add_authfile()
    parser.add_cachedir()
    parser.add_version()
    parser.add_ssl()
        
    options = parser.parse_args(args)
        
    return options

def main(args=None):
    '''Change or show S3QL file system parameters'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    with get_backend(options.storage_url, 
                     options.authfile, options.ssl) as (conn, bucketname):
        cachepath = get_bucket_cachedir(options.storage_url, options.cachedir)
               
        if options.action == 'delete':
            return delete_bucket(conn, bucketname, cachepath)

        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        
        bucket = conn.get_bucket(bucketname)
        
        try:
            unlock_bucket(options.authfile, options.storage_url, bucket)
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
    
    cachepath = get_bucket_cachedir(storage_url, '.')
    for i in ('.db', '.params'):
        if os.path.exists(cachepath + i):
            raise QuietError('%s already exists, aborting.' % cachepath+i)
    
    fh = os.fdopen(os.open(cachepath + '.db', os.O_RDWR | os.O_CREAT,
                           stat.S_IRUSR | stat.S_IWUSR), 'w+b')
    param = bucket.lookup(name)
    try:
        fh.close()
        db = Connection(cachepath + '.db')
        fh = tempfile.TemporaryFile()
        bucket.fetch_fh(name, fh)
        fh.seek(0)
        log.info('Reading metadata...')
        restore_metadata(fh, db)
        fh.close()
    except:
        # Don't keep file if it doesn't contain anything sensible
        os.unlink(cachepath + '.db')
        raise
    
    # Raise sequence number so that fsck.s3ql actually uses the
    # downloaded backup
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    param['seq_no'] = max(seq_nos) + 1
    pickle.dump(param, open(cachepath + '.params', 'wb'), 2)
    


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

def delete_bucket(conn, bucketname, cachepath):
    print('I am about to delete the bucket %s with ALL contents.' % bucketname,
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
    seq_nos = list(bucket.list('s3ql_seq_no_')) 
    if not seq_nos:
        raise QuietError(textwrap.dedent(''' 
            File system revision too old to upgrade!
            
            You need to use an older S3QL version to upgrade to a more recent
            revision before you can use this version to upgrade to the newest
            revision.
            '''))                     
    elif (isinstance(bucket, LocalBucket) and
         (seq_nos[0].endswith('.meta') or seq_nos[0].endswith('.dat'))):
        param = bucket.lookup('s3ql_metadata.meta')
        seq_nos = [ int(x[len('s3ql_seq_no_'):-4]) for x in seq_nos if x.endswith('.dat') ]                 
    else:
        param = bucket.lookup('s3ql_metadata')
        seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in seq_nos ]
    seq_no = max(seq_nos)

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

    log.info('Upgrading from revision %d to %d...', CURRENT_FS_REV - 1,
             CURRENT_FS_REV)
    param['revision'] = CURRENT_FS_REV
    
    if isinstance(bucket, LocalBucket):
        for (path, _, filenames) in os.walk(bucket.name, topdown=True):
            for name in filenames:
                if name.endswith('.dat'):
                    continue
                 
                basename = os.path.splitext(name)[0]
                os.rename(os.path.join(path, name),
                          os.path.join(path, basename))
                with open(os.path.join(path, basename), 'r+b') as dst:
                    dst.seek(0, os.SEEK_END)
                    with open(os.path.join(path, basename + '.dat'), 'rb') as src:
                        shutil.copyfileobj(src, dst)
                os.unlink(os.path.join(path, basename + '.dat'))
                     
    # Download metadata
    log.info("Downloading & uncompressing metadata...")
    dbfile = tempfile.NamedTemporaryFile()
    db = Connection(dbfile.name, fast_mode=True)
    fh = tempfile.TemporaryFile()
    bucket.fetch_fh("s3ql_metadata", fh)
    fh.seek(0)
    log.info('Reading metadata...')
    restore_legacy_metadata(fh, db)
    fh.close()
    
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

def restore_legacy_metadata(ifh, conn):
    unpickler = pickle.Unpickler(ifh)
    (data_start, to_dump, sizes, columns) = unpickler.load()
    ifh.seek(data_start)
    create_tables(conn)
    create_legacy_tables(conn)
    for (table, _) in to_dump:
        log.info('Loading %s', table)
        col_str = ', '.join(columns[table])
        val_str = ', '.join('?' for _ in columns[table])
        if table in ('inodes', 'blocks', 'objects', 'contents'):
            sql_str = 'INSERT INTO leg_%s (%s) VALUES(%s)' % (table, col_str, val_str)
        else:
            sql_str = 'INSERT INTO %s (%s) VALUES(%s)' % (table, col_str, val_str)
        for _ in xrange(sizes[table]):
            buf = unpickler.load()
            for row in buf:
                conn.execute(sql_str, row)

    # Create a block for each object
    conn.execute('''
         INSERT INTO blocks (id, hash, refcount, obj_id, size)
            SELECT id, hash, refcount, id, size FROM leg_objects
    ''')
    conn.execute('''
         INSERT INTO objects (id, refcount, compr_size)
            SELECT id, 1, compr_size FROM leg_objects
    ''')
    conn.execute('DROP TABLE leg_objects')
              
    # Create new inode_blocks table for inodes with multiple blocks
    conn.execute('''
         CREATE TEMP TABLE multi_block_inodes AS 
            SELECT inode FROM leg_blocks
            GROUP BY inode HAVING COUNT(inode) > 1
    ''')    
    conn.execute('''
         INSERT INTO inode_blocks (inode, blockno, block_id)
            SELECT inode, blockno, obj_id 
            FROM leg_blocks JOIN multi_block_inodes USING(inode)
    ''')
    
    # Create new inodes table for inodes with multiple blocks
    conn.execute('''
        INSERT INTO inodes (id, uid, gid, mode, mtime, atime, ctime, 
                            refcount, size, rdev, locked, block_id)
               SELECT id, uid, gid, mode, mtime, atime, ctime, 
                      refcount, size, rdev, locked, NULL
               FROM leg_inodes JOIN multi_block_inodes ON inode == id 
            ''')
    
    # Add inodes with just one block or no block
    conn.execute('''
        INSERT INTO inodes (id, uid, gid, mode, mtime, atime, ctime, 
                            refcount, size, rdev, locked, block_id)
               SELECT id, uid, gid, mode, mtime, atime, ctime, 
                      refcount, size, rdev, locked, obj_id
               FROM leg_inodes LEFT JOIN leg_blocks ON leg_inodes.id == leg_blocks.inode 
               GROUP BY leg_inodes.id HAVING COUNT(leg_inodes.id) <= 1  
            ''')
    
    conn.execute('''
        INSERT INTO symlink_targets (inode, target)
        SELECT id, target FROM leg_inodes WHERE target IS NOT NULL
    ''')
    
    conn.execute('DROP TABLE leg_inodes')
    conn.execute('DROP TABLE leg_blocks')
    
    # Sort out names
    conn.execute('''
        INSERT INTO names (name, refcount) 
        SELECT name, COUNT(name) FROM leg_contents GROUP BY name
    ''')
    conn.execute('''
        INSERT INTO contents (name_id, inode, parent_inode) 
        SELECT names.id, inode, parent_inode 
        FROM leg_contents JOIN names ON leg_contents.name == names.name
    ''')
    conn.execute('DROP TABLE leg_contents')
    
    conn.execute('ANALYZE')
    
def create_legacy_tables(conn):
    conn.execute("""
    CREATE TABLE leg_inodes (
        id        INTEGER PRIMARY KEY,
        uid       INT NOT NULL,
        gid       INT NOT NULL,
        mode      INT NOT NULL,
        mtime     REAL NOT NULL,
        atime     REAL NOT NULL,
        ctime     REAL NOT NULL,
        refcount  INT NOT NULL,
        target    BLOB(256) ,
        size      INT NOT NULL DEFAULT 0,
        rdev      INT NOT NULL DEFAULT 0,
        locked    BOOLEAN NOT NULL DEFAULT 0
    )
    """)    
    conn.execute("""
    CREATE TABLE leg_objects (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        refcount  INT NOT NULL,
        hash      BLOB(16) UNIQUE,
        size      INT NOT NULL,
        compr_size INT                  
    )""")
    conn.execute("""
    CREATE TABLE leg_blocks (
        inode     INTEGER NOT NULL REFERENCES leg_inodes(id),
        blockno   INT NOT NULL,
        obj_id    INTEGER NOT NULL REFERENCES leg_objects(id),
        PRIMARY KEY (inode, blockno)
    )""")
    conn.execute("""
    CREATE TABLE leg_contents (
        rowid     INTEGER PRIMARY KEY AUTOINCREMENT,
        name      BLOB(256) NOT NULL,
        inode     INT NOT NULL REFERENCES leg_inodes(id),
        parent_inode INT NOT NULL REFERENCES leg_inodes(id),
        
        UNIQUE (name, parent_inode)
    )""")
        
def create_legacy_indices(conn):
    conn.execute('CREATE INDEX ix_leg_contents_parent_inode ON contents(parent_inode)')
    conn.execute('CREATE INDEX ix_leg_contents_inode ON contents(inode)')
    conn.execute('CREATE INDEX ix_leg_objects_hash ON objects(hash)')
    conn.execute('CREATE INDEX ix_leg_blocks_obj_id ON blocks(obj_id)')
    conn.execute('CREATE INDEX ix_leg_blocks_inode ON blocks(inode)')
        
if __name__ == '__main__':
    main(sys.argv[1:])
