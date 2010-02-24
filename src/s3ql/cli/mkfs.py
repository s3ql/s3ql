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
from .. import mkfs
import re
from ..backends import s3, local
from ..common import (init_logging_from_options, get_credentials, get_cachedir, get_dbfile,
                         QuietError)
from ..database import WrappedConnection
from boto.s3.connection import Location
import apsw
import cPickle as pickle

log = logging.getLogger("mkfs")

def parse_args(args):

    parser = OptionParser(
        usage="%prog  [options] <bucketname>\n" \
            "       %prog --help",
        description="Initializes on Amazon S3 bucket for use as a filesystem")

    parser.add_option("--awskey", type="string",
                      help="Amazon Webservices access key to use. If not "
                           "specified, tries to read ~/.awssecret or the file given by --credfile.")
    parser.add_option("--logfile", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql", 'mkfs.log'),
                      help="Write log messages in this file. Default: ~/.s3ql/mkfs.log")
    parser.add_option("--s3-location", type="string", default='EU',
                      help="Specify storage location for new bucket. Allowed values: EU,"
                           'us-west-1, or us-standard. The later is not recommended, see'
                           'http://code.google.com/p/s3ql/wiki/FAQ#'
                           'What%27s_wrong_with_having_S3_buckets_in_the_%22US_Standar')
    parser.add_option("--credfile", type="string",
                      default=os.path.join(os.environ["HOME"], ".awssecret"),
                      help='Try to read AWS access key and key id from this file. '
                      'The file must be readable only be the owner and should contain '
                      'the key id and the secret key separated by a newline. '
                      'Default: ~/.awssecret')
    parser.add_option("--cachedir", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql"),
                      help="Specifies the directory for cache files. Different S3QL file systems "
                      '(i.e. located in different S3 buckets) can share a cache location, even if '
                      'they are mounted at the same time. '
                      'You should try to always use the same location here, so that S3QL can detect '
                      'and, as far as possible, recover from unclean unmounts. Default is ~/.s3ql.')
    parser.add_option("-L", type="string", default='', help="Filesystem label",
                      dest="label")
    parser.add_option("--blocksize", type="int", default=10240,
                      help="Maximum size of s3 objects in KB (default: %default)")
    parser.add_option("--encrypt", action="store_true", default=None,
                      help="Create an AES encrypted filesystem")
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
    options.bucketname = pps[0]

    return options

def main(args):

    options = parse_args(args)
    init_logging_from_options(options)

    if options.bucketname.startswith('local:'):
        # Canonicalize path, otherwise we don't have a unique dbfile/cachdir for this bucket
        options.bucketname = os.path.abspath(options.bucketname[len('local:'):])
        conn = local.Connection()
    else:
        if (not re.match(r'^[a-z0-9][a-z0-9.-]{1,60}[a-z0-9]$', options.bucketname)
            or re.match(r'^[0-9.]+$', options.bucketname)
            or re.search(r'\.-|-\.', options.bucketname)):
            raise QuietError('Invalid bucket name. Valid names consist only of lowercase letters,\n'
                             'digits, dots and dashes (but some more restrictions apply, refer to '
                             'S3 documentation).')

        raise QuietError(0)
        (awskey, awspass) = get_credentials(options.credfile, options.awskey)
        conn = s3.Connection(awskey, awspass)

    if conn.bucket_exists(options.bucketname):
        log.error(
            "Bucket already exists!\n"
            "(you can delete an existing bucket with tune.s3ql --delete)\n")
        raise QuietError(1)

    if isinstance(conn, s3.Connection):
        bucket = conn.create_bucket(options.bucketname, location=options.s3_location)
    else:
        bucket = conn.create_bucket(options.bucketname)

    if options.encrypt:
        if sys.stdin.isatty():
            wrap_pw = getpass("Enter encryption password: ")
            if not wrap_pw == getpass("Confirm encryption password: "):
                sys.stderr.write("Passwords don't match\n.")
                sys.exit(1)
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


    #
    # Setup database
    #
    dbfile = get_dbfile(options.bucketname, options.cachedir)
    cachedir = get_cachedir(options.bucketname, options.cachedir)


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
