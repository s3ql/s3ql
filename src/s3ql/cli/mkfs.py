'''
mkfs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import sys
import os
from getpass import getpass
import shutil
import logging
import cPickle as pickle
from s3ql import CURRENT_FS_REV
from s3ql.common import (get_backend, get_bucket_home, setup_logging,
                         QuietError, dump_metadata, create_tables,
                         init_tables)
from s3ql.parse_args import ArgumentParser
from s3ql.database import Connection
from s3ql.backends.boto.s3.connection import Location
from s3ql.backends import s3
import time
import tempfile

log = logging.getLogger("mkfs")

def parse_args(args):

    parser = ArgumentParser(
        description="Initializes an S3QL file system")

    parser.add_homedir()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()
    parser.add_storage_url()
    parser.add_ssl()
    
    parser.add_argument("--s3-location", default='EU', metavar='<name>',
                      choices=('EU', 'us-west-1', 'us-standard', 'ap-southeast-1'),
                      help="Storage location for new S3 buckets. Allowed values: `EU`, "
                           '`us-west-1`, `ap-southeast-1`, or `us-standard`. '
                           '(default: %(default)s)')
    parser.add_argument("-L", default='', help="Filesystem label",
                      dest="label", metavar='<name>',)
    parser.add_argument("--blocksize", type=int, default=10240, metavar='<size>',
                      help="Maximum block size in KB (default: %(default)d)")
    parser.add_argument("--plain", action="store_true", default=False,
                      help="Create unencrypted file system.")
    parser.add_argument("--force", action="store_true", default=False,
                        help="Overwrite any existing data.")

    options = parser.parse_args(args)

    if options.s3_location == 'us-standard':
        options.s3_location = Location.DEFAULT

    if not os.path.exists(options.homedir):
        os.mkdir(options.homedir, 0700)
        
    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    with get_backend(options.storage_url, options.homedir,
                     options.ssl) as (conn, bucketname):
        if conn.bucket_exists(bucketname):
            if not options.force:
                raise QuietError("Bucket already exists! Use --force to overwrite")
            
            bucket = conn.get_bucket(bucketname)
            log.info('Bucket already exists. Purging old file system data..')
            if not bucket.read_after_delete_consistent():
                log.info('Please note that the new file system may appear inconsistent\n'
                         'for a while until the removals have propagated through the backend.')
            bucket.clear()
            
        elif isinstance(conn, s3.Connection):
            bucket = conn.create_bucket(bucketname, location=options.s3_location)
        else:
            bucket = conn.create_bucket(bucketname)

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

            bucket.passphrase = wrap_pw
            bucket['s3ql_passphrase'] = data_pw
            bucket.passphrase = data_pw

        # Setup database
        home = get_bucket_home(options.storage_url, options.homedir)

        # There can't be a corresponding bucket, so we can safely delete
        # these files.
        if os.path.exists(home + '.db'):
            os.unlink(home + '.db')
        if os.path.exists(home + '-cache'):
            shutil.rmtree(home + '-cache')

        log.info('Creating metadata tables...')
        db = Connection(home + '.db')
        create_tables(db)
        init_tables(db)

        param = dict()
        param['revision'] = CURRENT_FS_REV
        param['seq_no'] = 0
        param['label'] = options.label
        param['blocksize'] = options.blocksize * 1024
        param['needs_fsck'] = False
        param['last_fsck'] = time.time() - time.timezone
        param['last-modified'] = time.time() - time.timezone
        bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')

        log.info('Saving metadata...')
        fh = tempfile.TemporaryFile()
        dump_metadata(fh, db)  
        
        log.info("Compressing & uploading metadata..")         
        fh.seek(0)
        bucket.store_fh("s3ql_metadata", fh, param)
        fh.close()
        pickle.dump(param, open(home + '.params', 'wb'), 2)


if __name__ == '__main__':
    main(sys.argv[1:])
