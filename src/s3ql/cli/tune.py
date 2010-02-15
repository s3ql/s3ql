'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from optparse import OptionParser
import logging
import cPickle as pickle
from boto.s3.connection import Location
from ..common import (init_logging_from_options, get_credentials, QuietError, unlock_bucket,
                      ExceptionStoringThread, get_parameters)
from getpass import getpass
import sys
from .. import s3
import os
from ..database import ConnectionManager
import tempfile

log = logging.getLogger("tune")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <bucketname>\n"
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
    parser.add_option("--awskey", type="string",
                      help="Amazon Webservices access key to use. If not "
                      "specified, tries to read ~/.awssecret or the file given by --credfile.")
    parser.add_option("--credfile", type="string", default=os.environ["HOME"].rstrip("/")
                       + "/.awssecret",
                      help='Try to read AWS access key and key id from this file. '
                      'The file must be readable only be the owner and should contain '
                      'the key id and the secret key separated by a newline. '
                      'Default: ~/.awssecret')

    (options, pps) = parser.parse_args(args)

    # Verify parameters
    if len(pps) != 1:
        parser.error("Incorrect number of arguments.")
    options.bucketname = pps[0]

    actions = [options.change_passphrase, options.upgrade, options.delete]
    selected = len([ act for act in actions if act ])
    if selected != 1:
        parser.error("Need to specify exactly one action.")

    return options

def main(args):
    '''Change or show S3QL file system parameters'''

    options = parse_args(args)
    init_logging_from_options(options)

    if options.bucketname.startswith('local:'):
        options.bucketname = os.path.abspath(options.bucketname[len('local:'):])
        conn = s3.LocalConnection()
    else:
        (awskey, awspass) = get_credentials(options.credfile, options.awskey)
        conn = s3.Connection(awskey, awspass)

    if not conn.bucket_exists(options.bucketname):
        raise QuietError("Bucket does not exist.")
    bucket = conn.get_bucket(options.bucketname)

    if options.delete:
        return delete_bucket(conn, options.bucketname)

    try:
        unlock_bucket(bucket)
    except s3.ChecksumError:
        raise QuietError('Checksum error - incorrect password?')

    if options.upgrade:
        return upgrade(conn, bucket)

    if options.change_passphrase:
        return change_passphrase(bucket)


def change_passphrase(bucket):
    '''Change bucket passphrase'''

    if not bucket.has_key('s3ql_passphrase'):
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
    if not list(bucket.keys('s3ql_parameters_')):
        upgrade_rev1(bucket)

    # Check that the bucket is in the correct location
    if not isinstance(bucket, s3.LocalBucket):
        with bucket._get_boto() as boto:
            need_move = boto.get_location() != Location.EU
        if need_move:
            relocate_bucket(conn, bucket)
            print('Bucket successfully relocated. Please re-run tune.s3ql --upgrade',
                  'with the new bucket name.', sep='\n')
            raise QuietError(0)

    # Read parameters
    seq_no = max([ int(x[len('s3ql_parameters_'):]) for x in bucket.keys('s3ql_parameters_') ])
    param = pickle.loads(bucket['s3ql_parameters_%d' % seq_no])

    # Upgrade from rev. 2 to rev. 3
    if param['revision'] == 2:
        upgrade_rev2(bucket)

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

def relocate_bucket(conn, bucket_old):
    '''Move bucket to different storage location
    
    Ask the user for a new bucket name, copy all data into the new
    bucket, delete the old bucket.
    '''

    print('Your bucket needs to be moved to a different S3 storage location',
          'that provides more reliability guarantees. This can only be done by creating',
          'a new bucket in the new storage location, copying all objects into the',
          'new bucket and then deleting the old bucket. Note that the copying is',
          'done internally on the Amazon network, so the objects do not need to be',
          'downloaded and uploaded again.',
          'Please enter the name for the new bucket (or press Ctrl-C to abort).', sep='\n')
    print('Name: ', end='')
    bucketname = sys.stdin.readline().strip()

    if conn.bucket_exists(bucketname):
        print('Bucket already exists.')
        raise QuietError(1)

    # This creates the bucket in the new location
    bucket_new = conn.create_bucket(bucketname)

    with bucket_new._get_boto() as boto_new:
        assert boto_new.get_location() == Location.EU

        log.info('Copying objects into new bucket..')
        threads = list()
        for (no, key) in enumerate(bucket_old):
            if no != 0 and no % 100 == 0:
                log.info('Copied %d objects so far..', no)

            t = ExceptionStoringThread(boto_new.copy_key, args=(key, bucket_old.name, key))
            t.start()
            threads.append(t)

            if len(threads) > 50:
                log.debug('50 threads reached, waiting..')
                threads.pop(0).join_and_raise()

        log.debug('Waiting for copying threads')
        for t in threads:
            t.join_and_raise()

        log.info('Removing old bucket...')
        #conn.delete_bucket(bucket_old.name, recursive=True)

def upgrade_rev2(bucket):
    '''Upgrade file system from revision 2 to 3'''

    pass


if __name__ == '__main__':
    main(sys.argv[1:])
