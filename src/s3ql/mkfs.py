'''
mkfs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from . import CURRENT_FS_REV
from .backends.common import get_bucket, BetterBucket
from .common import (get_bucket_cachedir, setup_logging, QuietError, 
    stream_write_bz2, CTRL_INODE)
from .database import Connection
from .metadata import dump_metadata, create_tables
from .parse_args import ArgumentParser
from getpass import getpass
from llfuse import ROOT_INODE
import cPickle as pickle
import logging
import os
import shutil
import stat
import sys
import tempfile
import time

log = logging.getLogger("mkfs")

def parse_args(args):

    parser = ArgumentParser(
        description="Initializes an S3QL file system")

    parser.add_cachedir()
    parser.add_authfile()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()
    parser.add_storage_url()
    
    parser.add_argument("-L", default='', help="Filesystem label",
                      dest="label", metavar='<name>',)
    parser.add_argument("--blocksize", type=int, default=10240, metavar='<size>',
                      help="Maximum block size in KB (default: %(default)d)")
    parser.add_argument("--plain", action="store_true", default=False,
                      help="Create unencrypted file system.")
    parser.add_argument("--force", action="store_true", default=False,
                        help="Overwrite any existing data.")

    options = parser.parse_args(args)
        
    return options

def init_tables(conn):
    # Insert root directory
    timestamp = time.time() - time.timezone
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                 "VALUES (?,?,?,?,?,?,?,?)",
                   (ROOT_INODE, stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR
                   | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH,
                    os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))

    # Insert control inode, the actual values don't matter that much 
    conn.execute("INSERT INTO inodes (id,mode,uid,gid,mtime,atime,ctime,refcount) "
                 "VALUES (?,?,?,?,?,?,?,?)",
                 (CTRL_INODE, stat.S_IFIFO | stat.S_IRUSR | stat.S_IWUSR,
                  0, 0, timestamp, timestamp, timestamp, 42))

    # Insert lost+found directory
    inode = conn.rowid("INSERT INTO inodes (mode,uid,gid,mtime,atime,ctime,refcount) "
                       "VALUES (?,?,?,?,?,?,?)",
                       (stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR,
                        os.getuid(), os.getgid(), timestamp, timestamp, timestamp, 1))
    name_id = conn.rowid('INSERT INTO names (name, refcount) VALUES(?,?)',
                         (b'lost+found', 1))
    conn.execute("INSERT INTO contents (name_id, inode, parent_inode) VALUES(?,?,?)",
                 (name_id, inode, ROOT_INODE))
    
def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)
    
    plain_bucket = get_bucket(options, plain=True)
    
    if 's3ql_metadata' in plain_bucket:
        if not options.force:
            raise QuietError("Found existing file system! Use --force to overwrite")
            
        log.info('Purging existing file system data..')
        plain_bucket.clear()
        if not plain_bucket.is_get_consistent():
            log.info('Please note that the new file system may appear inconsistent\n'
                     'for a while until the removals have propagated through the backend.')
            
    if not options.plain:
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter encryption password: ")
            if not wrap_pw == getpass("Confirm encryption password: "):
                raise QuietError("Passwords don't match.")
        else:
            wrap_pw = sys.stdin.readline().rstrip()

        # Generate data encryption passphrase
        log.info('Generating random encryption key...')
        fh = open('/dev/urandom', "rb", 0) # No buffering
        data_pw = fh.read(32)
        fh.close()
        
        bucket = BetterBucket(wrap_pw, 'bzip2', plain_bucket)
        bucket['s3ql_passphrase'] = data_pw
    else:    
        data_pw = None
        
    bucket = BetterBucket(data_pw, 'bzip2', plain_bucket)

    # Setup database
    cachepath = get_bucket_cachedir(options.storage_url, options.cachedir)

    # There can't be a corresponding bucket, so we can safely delete
    # these files.
    if os.path.exists(cachepath + '.db'):
        os.unlink(cachepath + '.db')
    if os.path.exists(cachepath + '-cache'):
        shutil.rmtree(cachepath + '-cache')

    log.info('Creating metadata tables...')
    db = Connection(cachepath + '.db')
    create_tables(db)
    init_tables(db)

    param = dict()
    param['revision'] = CURRENT_FS_REV
    param['seq_no'] = 1
    param['label'] = options.label
    param['blocksize'] = options.blocksize * 1024
    param['needs_fsck'] = False
    param['inode_gen'] = 0
    param['max_inode'] = db.get_val('SELECT MAX(id) FROM inodes')   
    param['last_fsck'] = time.time() - time.timezone
    param['last-modified'] = time.time() - time.timezone
    
    # This indicates that the convert_legacy_metadata() stuff
    # in BetterBucket is not required for this file system.
    param['bucket_revision'] = 1
    
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
    log.info('Wrote %.2f MB of compressed metadata.', obj_fh.get_obj_size() / 1024**2)
    pickle.dump(param, open(cachepath + '.params', 'wb'), 2)


if __name__ == '__main__':
    main(sys.argv[1:])
