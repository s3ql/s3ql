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
from s3ql import mkfs, CURRENT_FS_REV
from s3ql.common import (get_backend, get_bucket_home, setup_logging,
                         QuietError)
from s3ql.argparse import ArgumentParser
from s3ql.database import Connection
from s3ql.backends.boto.s3.connection import Location
from s3ql.backends import s3
import time

log = logging.getLogger("mkfs")

def parse_args(args):

    parser = ArgumentParser(
        description="Initializes an S3QL file system")

    parser.add_homedir()
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()
    parser.add_storage_url()
    
    parser.add_argument("--s3-location", default='EU', metavar='<name>',
                      choices=('EU', 'us-west-1', 'us-standard', 'ap-southeast-1'),
                      help="Storage location for new S3 buckets. Allowed values: `EU`, "
                           '`us-west-1`, `ap-southeast-1`, or `us-standard`.')
    parser.add_argument("-L", default='', help="Filesystem label",
                      dest="label", metavar='<name>',)
    parser.add_argument("--blocksize", type=int, default=10240, metavar='<size>',
                      help="Maximum block size in KB (default: %(default)d)")
    parser.add_argument("--plain", action="store_true", default=False,
                      help="Create unencrypted file system.")


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

    with get_backend(options.storage_url, options.homedir) as (conn, bucketname):
        if conn.bucket_exists(bucketname):
            raise QuietError("Bucket already exists!\n"
                             "(you can delete an existing bucket with s3qladm --delete)\n")

        if isinstance(conn, s3.Connection):
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

        try:
            log.info('Creating metadata tables...')
            db = Connection(home + '.db')
            mkfs.setup_tables(db)
            mkfs.init_tables(db)

            param = dict()
            param['revision'] = CURRENT_FS_REV
            param['seq_no'] = 0
            param['label'] = options.label
            param['blocksize'] = options.blocksize * 1024
            param['needs_fsck'] = False
            param['DB-Format'] = 'sqlite'
            param['last_fsck'] = time.time() - time.timezone
            bucket.store('s3ql_seq_no_%d' % param['seq_no'], 'Empty')

            log.info("Compressing & uploading metadata..")         
            db.execute('VACUUM')
            db.close()
            fh = open(home + '.db', 'rb')        
            bucket.store_fh("s3ql_metadata", fh, param)
            fh.close()

        finally:
            if os.path.exists(db.file):
                os.unlink(db.file)


if __name__ == '__main__':
    main(sys.argv[1:])
