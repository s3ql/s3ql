'''
$Id: tune.py 965 2010-03-14 00:36:36Z nikratio $

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from optparse import OptionParser
import logging
import cPickle as pickle
from ..common import (init_logging_from_options, get_backend, QuietError, unlock_bucket,
                      ExceptionStoringThread, cycle_metadata)
from getpass import getpass
import sys
from ..backends import s3
from ..backends.boto.s3.connection import Location
from ..backends.common import ChecksumError
import os
from ..database import ConnectionManager
import tempfile
import re

log = logging.getLogger("tune")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <storage-url>\n"
              "       %prog --copy <source-url> <dest-url>\n"
              "       %prog --help",
        description="Change or show S3QL file system parameters.")

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
    parser.add_option("--homedir", type="string",
                      default=os.path.join(os.environ["HOME"], ".s3ql"),
                      help='Directory for log files, cache and authentication info.'
                      'Default: ~/.s3ql')
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
        options.storage_url = pps[0]
        options.dest_url = pps[1]
    else:
        if len(pps) != 1:
            parser.error("Incorrect number of arguments.")
        options.storage_url = pps[0]

    return options

def main(args=None):
    '''Change or show S3QL file system parameters'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    init_logging_from_options(options, 'tune.log')

    with get_backend(options) as (conn, bucketname):
        if not bucketname in conn:
            raise QuietError("Bucket does not exist.")
        bucket = conn.get_bucket(bucketname)

        try:
            unlock_bucket(bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')

        if options.delete:
            return delete_bucket(conn, bucketname)

        if options.copy:
            return copy_bucket(conn, options, bucket)

        if options.change_passphrase:
            return change_passphrase(bucket)

        if options.upgrade:
            return upgrade(conn, bucket)


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

    log.info('Downloading metadata...')
    dbfile = tempfile.NamedTemporaryFile()
    bucket.fetch_fh('s3ql_metadata', dbfile)
    dbfile.flush()
    dbcm = ConnectionManager(dbfile.name, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')

    # Upgrade from revision 1 to 2
    if not list(bucket.list('s3ql_parameters_')):
        param = upgrade_rev1(dbcm, bucket)
    else:
        # Read parameters
        seq_no = max([ int(x[len('s3ql_parameters_'):]) for x in bucket.list('s3ql_parameters_') ])
        param = pickle.loads(bucket['s3ql_parameters_%d' % seq_no])

    # Check consistency
    mountcnt_db = dbcm.get_val('SELECT mountcnt FROM parameters')
    if mountcnt_db < param['mountcnt']:
        raise QuietError('It appears that the file system is still mounted somewhere else, or has\n'
                         'not been unmounted cleanly. In the later case you should run fsck.s3ql\n'
                         'on the computer where the file system has been mounted most recently.\n'
                         'If you are using the S3 backend, it is also possible that updates are\n'
                         'still propagating through S3 and you just have to wait a while.')
    elif mountcnt_db > param['mountcnt']:
        raise RuntimeError('mountcnt_db > mountcnt_s3, this should not happen.')

    # Lock the bucket   
    param['mountcnt'] += 1
    dbcm.execute('UPDATE parameters SET mountcnt=mountcnt+1')
    bucket.store('s3ql_parameters_%d' % param['mountcnt'],
                 pickle.dumps(param, 2))

    # Upgrade from rev. 2 to rev. 3
    if param['revision'] == 2:
        upgrade_rev2(dbcm, param)

    # Upload parameters
    param['mountcnt'] += 1
    dbcm.execute('UPDATE parameters SET mountcnt=mountcnt+1')
    bucket.store('s3ql_parameters_%d' % param['mountcnt'],
                 pickle.dumps(param, 2))

    # Upload and metadata
    log.info("Uploading database..")
    dbcm.execute("VACUUM")
    cycle_metadata(bucket)
    bucket.store_fh("s3ql_metadata", open(dbfile.name, 'rb'))

def upgrade_rev1(dbcm, bucket):
    '''Upgrade file system from revision 1 to 2'''

    log.info('Updating file system from revision 1 to 2.')

    # Remove . and .. from database
    dbcm.execute('DELETE FROM contents WHERE name=? OR name=?', ('.', '..'))

    # Prepare new eventual consistency handling
    dbcm.execute('UPDATE parameters SET mountcnt=mountcnt+1')

    param = dict()
    param['revision'] = 2
    param['mountcnt'] = dbcm.get_val('SELECT mountcnt FROM parameters')

    bucket.store('s3ql_parameters_%d' % param['mountcnt'],
                 pickle.dumps(param, 2))

    return param


def upgrade_rev2(dbcm, param):
    '''Upgrade file system from revision 2 to 3'''

    log.info('Updating file system from revision 2 to 3.')
    dbcm.execute('CREATE INDEX ix_blocks_s3key ON blocks(s3key)')
    param['revision'] = 3


def copy_bucket(conn, options, src_bucket):
    '''Copy bucket to different storage location'''

    if not re.match('^[a-z][a-z0-9-]*$', options.dest_name):
        raise QuietError('Invalid dest. bucket name. Name must consist only of lowercase letters,\n'
                         'digits and dashes, and the first character has to be a letter.')

    if conn.bucket_exists(options.dest_name):
        print('Destination bucket already exists.')
        raise QuietError(1)

    # This creates the bucket in the new location
    if not isinstance(conn, s3.Connection):
        raise QuietError('Can only copy S3 buckets')

    dest_bucket = conn.create_bucket(options.dest_name, location=options.s3_location)

    log.info('Copying objects into new bucket..')
    threads = list()
    for (no, key) in enumerate(src_bucket):
        if no != 0 and no % 100 == 0:
            log.info('Copied %d objects so far..', no)

        def cp(key=key):
            with dest_bucket._get_boto() as boto:
                s3.retry_boto(boto.copy_key, key, src_bucket.name, key)

        t = ExceptionStoringThread(cp, log)
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
