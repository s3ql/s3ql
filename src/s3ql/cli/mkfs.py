'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import sys
import os
from getpass import getpass
import shutil
from optparse import OptionParser
import logging
from s3ql import mkfs
from s3ql.common import (init_logging_from_options, get_backend, get_cachedir, get_dbfile,
                         QuietError)
from s3ql.database import WrappedConnection
from s3ql.backends.boto.s3.connection import Location
from s3ql.backends import s3
import apsw
import cPickle as pickle

log = logging.getLogger("mkfs")

def parse_args(args):

    parser = OptionParser(
        usage="%prog  [options] <storage-url>\n" \
            "       %prog --help",
        description="Initializes an S3QL file system")

    parser.add_option("--s3-location", type="string", default='EU',
                      help="Specify storage location for new bucket. Allowed values: EU,"
                           'us-west-1, or us-standard. The later is not recommended, please '
                           'refer to the FAQ for more information.')
    parser.add_option("--homedir", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql"),
                      help='Directory for log files, cache and authentication info.'
                      'Default: ~/.s3ql')
    parser.add_option("-L", type="string", default='', help="Filesystem label",
                      dest="label")
    parser.add_option("--blocksize", type="int", default=10240,
                      help="Maximum size of s3 objects in KB (default: %default)")
    parser.add_option("--encrypt", action="store_true", default=None,
                      help="Create an encrypted and compressed file system")
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")


    (options, pps) = parser.parse_args(args)

    if options.s3_location not in ('EU', 'us-west-1', 'us-standard'):
        parser.error("Invalid S3 storage location. Allowed values: EU, us-west-1, us-standard")

    if options.s3_location == 'us-standard':
        options.s3_location = Location.DEFAULT

    if len(pps) != 1:
        parser.error("Incorrect number of arguments.")
    options.storage_url = pps[0]

    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    init_logging_from_options(options, 'mkfs.log')

    with get_backend(options) as (conn, bucketname):
        if conn.bucket_exists(bucketname):
            raise QuietError("Bucket already exists!\n"
                             "(you can delete an existing bucket with tune.s3ql --delete)\n")

        if isinstance(conn, s3.Connection):
            bucket = conn.create_bucket(bucketname, location=options.s3_location)
        else:
            bucket = conn.create_bucket(bucketname)

        if options.encrypt:
            if sys.stdin.isatty():
                wrap_pw = getpass("Enter encryption password: ")
                if not wrap_pw == getpass("Confirm encryption password: "):
                    raise QuietError("Passwords don't match.")
            else:
                wrap_pw = sys.stdin.readline().rstrip()

            # Generate data encryption passphrase
            log.info('Generating random encryption key...')
            fh = open('/dev/random', "rb", 0) # No buffering
            data_pw = fh.read(32)
            fh.close()

            bucket.passphrase = wrap_pw
            bucket['s3ql_passphrase'] = data_pw
            bucket.passphrase = data_pw

        # Setup database
        dbfile = get_dbfile(options.storage_url, options.homedir)
        cachedir = get_cachedir(options.storage_url, options.homedir)


        # There can't be a corresponding bucket, so we can safely delete
        # these files.
        if os.path.exists(dbfile):
            os.unlink(dbfile)
        if os.path.exists(cachedir):
            shutil.rmtree(cachedir)

        try:
            log.info('Creating metadata tables...')
            mkfs.setup_db(WrappedConnection(apsw.Connection(dbfile), retrytime=0),
                          options.blocksize * 1024, options.label)

            log.info('Uploading database...')
            param = dict()
            param['revision'] = 2
            param['mountcnt'] = 0
            bucket.store('s3ql_parameters_%d' % param['mountcnt'],
                         pickle.dumps(param, 2))
            bucket.store_fh('s3ql_metadata', open(dbfile, 'r'))

        finally:
            os.unlink(dbfile)


if __name__ == '__main__':
    main(sys.argv[1:])
