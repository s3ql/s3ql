'''
adm.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from Queue import Queue
from datetime import datetime as Datetime
from getpass import getpass
from s3ql import CURRENT_FS_REV
from s3ql.backends.common import (BetterBucket, get_bucket, NoSuchBucket, 
    ChecksumError, AbstractBucket, NoSuchObject)
from s3ql.backends.local import Bucket as LocalBucket, ObjectR, unescape, escape
from s3ql.common import (QuietError, restore_metadata, cycle_metadata, 
    dump_metadata, create_tables, setup_logging, get_bucket_cachedir)
from s3ql.database import Connection
from s3ql.fsck import Fsck
from s3ql.parse_args import ArgumentParser
from threading import Thread
import ConfigParser
import cPickle as pickle
import errno
import hashlib
import logging
import os
import re
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
        return clear(get_bucket(options, plain=True),
                     get_bucket_cachedir(options.storage_url, options.cachedir))
    
    if options.action == 'upgrade':
        return upgrade(get_possibly_old_bucket(options))
        
    bucket = get_bucket(options)
    
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
        
    log.info('Downloading %s...', name)
    
    cachepath = get_bucket_cachedir(storage_url, '.')
    for i in ('.db', '.params'):
        if os.path.exists(cachepath + i):
            raise QuietError('%s already exists, aborting.' % cachepath+i)
    
    param = bucket.lookup(name)
    try:
        log.info('Reading metadata...')
        def do_read(fh):
            os.close(os.open(cachepath + '.db', os.O_RDWR | os.O_CREAT,
                             stat.S_IRUSR | stat.S_IWUSR), 'w+b')            
            db = Connection(cachepath + '.db')
            restore_metadata(fh, db)
        bucket.perform_read(do_read, name)
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
    
    print('File system deleted.')
    
    if not bucket.is_get_consistent():
        log.info('Note: it may take a while for the removals to propagate through the backend.')
                

def get_possibly_old_bucket(options, plain=False):
    '''Return factory producing bucket objects for given storage-url
    
    If *plain* is true, don't attempt to unlock and don't wrap into
    BetterBucket.    
    '''
                                     
    hit = re.match(r'^([a-zA-Z0-9]+)://(.+)$', options.storage_url)
    if not hit:
        raise QuietError('Unknown storage url: %s' % options.storage_url)
    
    backend_name = 's3ql.backends.%s' % hit.group(1)
    bucket_name = hit.group(2)
    try:
        __import__(backend_name)
    except ImportError:
        raise QuietError('No such backend: %s' % hit.group(1))
    
    bucket_class = getattr(sys.modules[backend_name], 'Bucket')
    
    # Read authfile
    config = ConfigParser.SafeConfigParser()
    if os.path.isfile(options.authfile):
        mode = os.stat(options.authfile).st_mode
        if mode & (stat.S_IRGRP | stat.S_IROTH):
            raise QuietError("%s has insecure permissions, aborting." % options.authfile)    
        config.read(options.authfile)
    
    backend_login = None
    backend_pw = None
    bucket_passphrase = None
    for section in config.sections():
        def getopt(name):
            try:
                return config.get(section, name)
            except ConfigParser.NoOptionError:
                return None

        pattern = getopt('storage-url')
        
        if not pattern or not options.storage_url.startswith(pattern):
            continue
        
        backend_login = backend_login or getopt('backend-login')
        backend_pw = backend_pw or getopt('backend-password')
        bucket_passphrase = bucket_passphrase or getopt('bucket-passphrase')
      
    if not backend_login and bucket_class.needs_login:
        if sys.stdin.isatty():
            backend_login = getpass("Enter backend login: ")
        else:
            backend_login = sys.stdin.readline().rstrip()

    if not backend_pw and bucket_class.needs_login:
        if sys.stdin.isatty():
            backend_pw = getpass("Enter backend password: ") 
        else:
            backend_pw = sys.stdin.readline().rstrip()   

    bucket = bucket_class(bucket_name, backend_login, backend_pw)
    if bucket_class == LocalBucket and 's3ql_metadata.dat' in bucket:
        bucket_class = LegacyLocalBucket
        
    if plain:
        return lambda: bucket_class(bucket_name, backend_login, backend_pw)
    
    bucket = bucket_class(bucket_name, backend_login, backend_pw)
    
    try:
        encrypted = 's3ql_passphrase' in bucket
    except NoSuchBucket:
        raise QuietError('Bucket %d does not exist' % bucket_name)
        
    if encrypted and not bucket_passphrase:
        if sys.stdin.isatty():
            bucket_passphrase = getpass("Enter bucket encryption passphrase: ") 
        else:
            bucket_passphrase = sys.stdin.readline().rstrip()
    elif not encrypted:
        bucket_passphrase = None
        
    if hasattr(options, 'compress'):
        compress = options.compress
    else:
        compress = 'zlib'
            
    if not encrypted:
        return lambda: BetterBucket(None, compress, 
                                    bucket_class(bucket_name, backend_login, backend_pw))
    
    tmp_bucket = BetterBucket(bucket_passphrase, compress, bucket)
    
    try:
        data_pw = tmp_bucket['s3ql_passphrase']
    except ChecksumError:
        raise QuietError('Wrong bucket passphrase')

    return lambda: BetterBucket(data_pw, compress, 
                                bucket_class(bucket_name, backend_login, backend_pw))
    
def upgrade(bucket_factory):
    '''Upgrade file system to newest revision'''

    bucket = bucket_factory()
    
    # Access to protected member
    #pylint: disable=W0212
    log.info('Getting file system parameters..')
    seq_nos = [ int(x[len('s3ql_seq_no_'):]) for x in bucket.list('s3ql_seq_no_') ]
    seq_no = max(seq_nos)
    if not seq_nos:
        raise QuietError(textwrap.dedent(''' 
            File system revision too old to upgrade!
            
            You need to use an older S3QL version to upgrade to a more recent
            revision before you can use this version to upgrade to the newest
            revision.
            '''))                     
    param = bucket.lookup('s3ql_metadata')

    # Check for unclean shutdown
    if param['seq_no'] < seq_no:
        if bucket.is_get_consistent():
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
        
        When using the local backend, metadata and data of each stored
        object will be merged into one file. This requires every object
        to be rewritten and may thus take some time.
        '''))

    print('Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError()

    log.info('Upgrading from revision %d to %d...', CURRENT_FS_REV - 1,
             CURRENT_FS_REV)
    
    if 's3ql_hash_check_status' not in bucket:        
        if (isinstance(bucket, LegacyLocalBucket) or
            (isinstance(bucket, BetterBucket) and
             isinstance(bucket.bucket, LegacyLocalBucket))):
            log.info('Merging metadata into datafiles...')
            if isinstance(bucket, LegacyLocalBucket):
                bucketpath = bucket.name
            else:
                bucketpath = bucket.bucket.name
            i = 0
            for (path, _, filenames) in os.walk(bucketpath, topdown=True):
                for name in filenames:
                    if not name.endswith('.meta'):
                        continue
                    
                    basename = os.path.splitext(name)[0]
                    if '=00' in basename:
                        raise RuntimeError("No, seriously, you tried to break things, didn't you?")
                    
                    with open(os.path.join(path, name), 'r+b') as dst:
                        dst.seek(0, os.SEEK_END)
                        with open(os.path.join(path, basename + '.dat'), 'rb') as src:
                            shutil.copyfileobj(src, dst)
                    
                    basename = basename.replace('#', '=23')
                    os.rename(os.path.join(path, name),
                              os.path.join(path, basename))
                    os.unlink(os.path.join(path, basename + '.dat'))
                    
                    i += 1
                    if i % 100 == 0:
                        log.info('..processed %d objects so far..', i)
                    
            print("Merging complete. Please restart s3qladm upgrade to complete the upgrade.")
            return
                         
        # Download metadata
        log.info("Downloading & uncompressing metadata...")
        dbfile = tempfile.NamedTemporaryFile()
        with tempfile.TemporaryFile() as tmp:    
            def do_read(fh):
                tmp.seek(0)
                tmp.truncate()
                shutil.copyfileobj(fh, tmp)
            bucket.perform_read(do_read, "s3ql_metadata")
            db = Connection(dbfile.name, fast_mode=True)
            tmp.seek(0)
            restore_legacy_metadata(tmp, db)
    
        # Increase metadata sequence no
        param['seq_no'] += 1
        bucket['s3ql_seq_no_%d' % param['seq_no']] = 'Empty'
        for i in seq_nos:
            if i < param['seq_no'] - 5:
                del bucket['s3ql_seq_no_%d' % i ]
    
        log.info("Uploading database..")
        cycle_metadata(bucket)
        param['last-modified'] = time.time() - time.timezone
        bucket.perform_write(lambda fh: dump_metadata(fh, db) , "s3ql_metadata", param) 
            
    else:
        log.info("Downloading & uncompressing metadata...")
        def do_read(fh):
            dbfile = tempfile.NamedTemporaryFile() 
            db = Connection(dbfile.name, fast_mode=True)
            restore_metadata(fh, db)
            return db
        bucket.perform_read(do_read, "s3ql_metadata") 

    print(textwrap.dedent('''
        The following process may take a long time, but can be interrupted
        with Ctrl-C and resumed from this point by calling `s3qladm upgrade`
        again. Please see Changes.txt for why this is necessary.
        '''))

    if 's3ql_hash_check_status' not in bucket:
        log.info("Starting hash verification..")
        start_obj = 0
        bucket['s3ql_hash_check_status'] = '%d' % start_obj
    else:
        start_obj = int(bucket['s3ql_hash_check_status'])
        log.info("Resuming hash verification with object %d..", start_obj)
    
    try:
        total = db.get_val('SELECT COUNT(id) FROM objects WHERE id > ?', (start_obj,))
        i = 0
        queue = Queue(1)
        queue.error = None
        threads = []
        if (isinstance(bucket, LocalBucket) or
            (isinstance(bucket, BetterBucket) and
             isinstance(bucket.bucket,LocalBucket))):
            thread_count = 1
        else:
            thread_count = 25
            
        for _ in range(thread_count):
            t = Thread(target=check_hash, args=(queue, bucket_factory()))
            t.daemon = True
            t.start()
            threads.append(t)
            
        for tmp in db.query('SELECT obj_id, hash FROM blocks JOIN objects '
                            'ON obj_id == objects.id WHERE obj_id > ? '
                            'ORDER BY obj_id ASC', (start_obj,)):
            queue.put(tmp)
            i += 1
            if i % 100 == 0:
                log.info(' ..checked %d/%d objects..', i, total)      
                
            if queue.error:
                raise queue.error[0], queue.error[1], queue.error[2]
                
        for t in threads:
            queue.put(None)
        for t in threads:
            t.join()                  
            
    except KeyboardInterrupt:
        log.info("Storing verification status...")
        for t in threads:
            queue.put(None)
        for t in threads:
            t.join()
        bucket['s3ql_hash_check_status'] = '%d' % tmp[0]
        raise QuietError('Aborting..')

    except:
        log.info("Storing verification status...")
        bucket['s3ql_hash_check_status'] = '%d' % tmp[0]
        raise
        
    log.info('Running fsck...')
    bucket['s3ql_hash_check_status'] = '%d' % tmp[0]
    fsck = Fsck(tempfile.mkdtemp(), bucket, param, db)
    fsck.check()    
    
    if fsck.uncorrectable_errors:
        raise QuietError("Uncorrectable errors found, aborting.")
            
    param['revision'] = CURRENT_FS_REV
    param['seq_no'] += 1
    bucket['s3ql_seq_no_%d' % param['seq_no']] = 'Empty'

    log.info("Uploading database..")
    cycle_metadata(bucket)
    param['last-modified'] = time.time() - time.timezone
    bucket.perform_write(lambda fh: dump_metadata(fh, db) , "s3ql_metadata", param) 
                
def check_hash(queue, bucket):
    
    try:
        while True:
            tmp = queue.get()
            if tmp is None:
                break
                  
            (obj_id, hash_) = tmp
              
            
            def do_read(fh):
                sha = hashlib.sha256()
                while True:
                    buf = fh.read(128*1024)
                    if not buf:
                        break
                    sha.update(buf)
                return sha           
            try:
                sha = bucket.perform_read(do_read, "s3ql_data_%d" % obj_id)
    
            except ChecksumError:
                log.warn('Object %d corrupted! Deleting..', obj_id)
                bucket.delete('s3ql_data_%d' % obj_id)
                
            except NoSuchObject:
                log.warn('Object %d seems to have disappeared', obj_id)
            
            else:
                if sha.digest() != hash_:
                    log.warn('Object %d corrupted! Deleting..', obj_id)
                    bucket.delete('s3ql_data_%d' % obj_id)
    except:
        queue.error = sys.exc_info()
        queue.get()
                            
                            
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


class LegacyLocalBucket(AbstractBucket):
    needs_login = False
    
    def __init__(self, name, backend_login, backend_pw): #IGNORE:W0613
        super(LegacyLocalBucket, self).__init__()
        self.name = name
        if not os.path.exists(name):
            raise NoSuchBucket(name)
            
    def lookup(self, key):
        path = self._key_to_path(key) + '.meta'
        try:
            with open(path, 'rb') as src:
                return pickle.load(src)
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise NoSuchObject(key)
            else:
                raise

    def open_read(self, key):
        path = self._key_to_path(key)
        try:
            fh = ObjectR(path + '.dat') 
        except IOError as exc:
            if exc.errno == errno.ENOENT:
                raise NoSuchObject(key)
            else:
                raise
            
        fh.metadata = pickle.load(open(path + '.meta', 'rb'))
        
        return fh
    
    def open_write(self, key, metadata=None):
        raise RuntimeError('Not implemented')

    def clear(self):
        raise RuntimeError('Not implemented')

    def copy(self, src, dest):
        raise RuntimeError('Not implemented')
        
    def contains(self, key):
        path = self._key_to_path(key)
        try:
            os.lstat(path + '.meta')
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return False
            raise
        return True

    def delete(self, key, force=False):
        raise RuntimeError('Not implemented')

    def list(self, prefix=''):
        if prefix:
            base = os.path.dirname(self._key_to_path(prefix))     
        else:
            base = self.name
            
        for (path, dirnames, filenames) in os.walk(base, topdown=True):
            
            # Do not look in wrong directories
            if prefix:
                rpath = path[len(self.name):] # path relative to base
                prefix_l = ''.join(rpath.split('/'))
                
                dirs_to_walk = list()
                for name in dirnames:
                    prefix_ll = unescape(prefix_l + name)
                    if prefix_ll.startswith(prefix[:len(prefix_ll)]):
                        dirs_to_walk.append(name)
                dirnames[:] = dirs_to_walk
                                            
            for name in filenames:
                key = unescape(name)
                
                if not prefix or key.startswith(prefix):
                    if key.endswith('.meta'):
                        yield key[:-5]

    def _key_to_path(self, key):
        key = escape(key)
        
        if not key.startswith('s3ql_data_'):
            return os.path.join(self.name, key)
        
        no = key[10:]
        path = [ self.name, 's3ql_data_']
        for i in range(0, len(no), 3):
            path.append(no[:i])
        path.append(key)
        
        return os.path.join(*path)

    def is_get_consistent(self):
        return True
                    
    def is_list_create_consistent(self):
        return True
    
                        
if __name__ == '__main__':
    main(sys.argv[1:])

