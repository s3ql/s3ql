'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from optparse import OptionParser
import logging
import cPickle as pickle
from ..common import (init_logging_from_options, get_credentials, QuietError, unlock_bucket,
                      ExceptionStoringThread)
from getpass import getpass
import sys
from ..backends import s3, local
from boto.s3.connection import Location
from ..backends.common import ChecksumError
import os
from ..database import ConnectionManager
import tempfile

log = logging.getLogger("tune")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <bucketname>\n"
              "       %prog --copy <src_bucket> <dest_bucket>\n"
              "       %prog --help",
        description="Change or show S3QL file system parameters.")

    parser.add_option("--logfile", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql", 'tune.log'),
                      help="Write log messages in this file. Default: ~/.s3ql/tune.log")
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--change-passphrase", action="store_true", default=False,
                      help="Change bucket passphrase")
    parser.add_option("--upgrade", action="store_true", default=False,
                      help="Upgrade file system to newest revision.")
    parser.add_option("--delete", action="store_true", default=False,
                      help="Completely delete a bucket with all contents.")
    parser.add_option("--copy", action="store_true", default=False,
                      help="Copy a bucket. The destination bucket must not exist.")
    parser.add_option("--awskey", type="string",
                      help="Amazon Webservices access key to use. If not "
                      "specified, tries to read ~/.awssecret or the file given by --credfile.")
    parser.add_option("--credfile", type="string", default=os.environ["HOME"].rstrip("/")
                       + "/.awssecret",
                      help='Try to read AWS access key and key id from this file. '
                      'The file must be readable only be the owner and should contain '
                      'the key id and the secret key separated by a newline. '
                      'Default: ~/.awssecret')
    parser.add_option("--s3-location", type="string", default='EU',
                      help="Specify storage location for new bucket. Allowed values: EU,"
                           'us-west-1, or us-standard. The later is not recommended, see'
                           'http://code.google.com/p/s3ql/wiki/FAQ#'
                           'What%27s_wrong_with_having_S3_buckets_in_the_%22US_Standar')

    (options, pps) = parser.parse_args(args)

    actions = [options.change_passphrase, options.upgrade, options.delete, options.copy]
    selected = len([ act for act in actions if act ])
    if selected != 1:
        parser.error("Need to specify exactly one action.")

    if options.s3_location not in ('EU', 'us-west-1', 'us-standard'):
        parser.error("Invalid S3 storage location. Allowed values: EU, us-west-1, us-standard")

    if options.s3_location == 'us-standard':
        options.s3_location = Location.DEFAULT

    if options.copy:
        if len(pps) != 2:
            parser.error("Incorrect number of arguments.")
        options.bucketname = pps[0]
        options.dest_name = pps[1]
    else:
        if len(pps) != 1:
            parser.error("Incorrect number of arguments.")
        options.bucketname = pps[0]

    return options

def main(args):
    '''Change or show S3QL file system parameters'''

    options = parse_args(args)
    init_logging_from_options(options)

    if options.bucketname.startswith('local:'):
        # Canonicalize path, otherwise we don't have a unique dbfile/cachdir for this bucket
        options.bucketname = os.path.abspath(options.bucketname[len('local:'):])
        conn = local.Connection()
    else:
        (awskey, awspass) = get_credentials(options.credfile, options.awskey)
        conn = s3.Connection(awskey, awspass)

    if not conn.bucket_exists(options.bucketname):
        raise QuietError("Bucket does not exist.")
    bucket = conn.get_bucket(options.bucketname)

    if options.delete:
        return delete_bucket(conn, options.bucketname)

    if options.copy:
        return copy_bucket(conn, options)

    try:
        unlock_bucket(bucket)
    except ChecksumError:
        raise QuietError('Checksum error - incorrect password?')

    if options.upgrade:
        return upgrade(conn, bucket)

    if options.change_passphrase:
        return change_passphrase(bucket)


def change_passphrase(bucket):
    '''Change bucket passphrase'''

    if 's3ql_passphrase' not in bucket:
        raise QuietError('Bucket is not encrypted.')

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()

    data_pw = bucket['s3ql_passphrase']
    bucket.passphrase = wrap_pw
    bucket['s3ql_passphrase'] = data_pw

def delete_bucket(conn, bucketname):
    print('I am about to delete the bucket %s with ALL contents.' % bucketname,
          'Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError(1)

    log.info('Deleting...')
    conn.delete_bucket(bucketname, recursive=True)

    print('Bucket deleted. Please note that it may take a while until',
          'the removal is visible everywhere.', sep='\n')

def upgrade(conn, bucket):
    '''Upgrade file system to newest revision'''

    print('I am about to update the file system to the newest revision.',
          'Please note that you will not be able to access the file system',
          'with any older version of S3QL after this operation.',
          'Please enter "yes" to continue.', '> ', sep='\n', end='')

    if sys.stdin.readline().strip().lower() != 'yes':
        raise QuietError(1)

    # Upgrade from revision 1 to 2
    if not list(bucket.list('s3ql_parameters_')):
        upgrade_rev1(bucket)

    # Read parameters
    #seq_no = max([ int(x[len('s3ql_parameters_'):]) for x in bucket.list('s3ql_parameters_') ])
    #param = pickle.loads(bucket['s3ql_parameters_%d' % seq_no])

    # Upgrade from rev. 2 to rev. 3
    #if param['revision'] == 2:
    #    upgrade_rev2(bucket)

def upgrade_rev1(bucket):
    '''Upgrade file system from revision 1 to 2'''

    log.info('Updating file system from revision 1 to 2.')

    log.info('Downloading metadata...')
    dbfile = tempfile.NamedTemporaryFile()
    bucket.fetch_fh('s3ql_metadata', dbfile)
    dbfile.flush()

    log.info('Updating metadata...')
    dbcm = ConnectionManager(dbfile.name, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')

    # Remove . and .. from database
    dbcm.execute('DELETE FROM contents WHERE name=? OR name=?', ('.', '..'))

    # Prepare new eventual consistency handling
    dbcm.execute('UPDATE parameters SET mountcnt=mountcnt+1')

    param = dict()
    param['revision'] = 2
    param['mountcnt'] = dbcm.get_val('SELECT mountcnt FROM parameters')

    log.info('Uploading metadata')
    bucket.store('s3ql_parameters_%d' % param['mountcnt'],
                 pickle.dumps(param, 2))
    bucket.store_fh('s3ql_metadata', dbfile)

def copy_bucket(conn, options, src_bucket):
    '''Copy bucket to different storage location'''


    if conn.bucket_exists(options.dest_name):
        print('Destination bucket already exists.')
        raise QuietError(1)

    # This creates the bucket in the new location
    if not isinstance(conn, s3.Connection):
        raise QuietError('Can only copy S3 buckets')

    dest_bucket = conn.create_bucket(options.bucketname, location=options.s3_location)

    with dest_bucket._get_boto() as boto_new:

        log.info('Copying objects into new bucket..')
        threads = list()
        for (no, key) in enumerate(src_bucket):
            if no != 0 and no % 100 == 0:
                log.info('Copied %d objects so far..', no)

            t = ExceptionStoringThread(boto_new.copy_key, args=(key, src_bucket.name, key))
            t.start()
            threads.append(t)

            if len(threads) > 50:
                log.debug('50 threads reached, waiting..')
                threads.pop(0).join_and_raise()

        log.debug('Waiting for copying threads')
        for t in threads:
            t.join_and_raise()


if __name__ == '__main__':
    main(sys.argv[1:])
