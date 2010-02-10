'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from optparse import OptionParser
import logging
import cPickle as pickle
from ..common import (init_logging_from_options, get_credentials, QuietError)
from getpass import getpass
import sys
from .. import s3
import os
import time
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
        parser.error("Wrong number of parameters")
    options.bucketname = pps[0]

    if not any([options.change_passphrase, options.upgrade]):
        parser.error("Need to specify at least one action.")

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

    if options.upgrade:
        upgrade(bucket)

    if 's3ql_parameters' not in bucket:
        raise QuietError('Old file system revision, please run tune.s3ql --upgrade first.')

    param = pickle.loads(bucket['s3ql_parameters'])

    if param['revision'] < 2:
        raise QuietError('File system revision too old, please run tune.s3ql --upgrade first.')
    elif param['revision'] > 2:
        raise QuietError('File system revision too new, please update your '
                         'S3QL installation.')

    if options.change_passphrase:
        change_passphrase(bucket)



def upgrade(bucket):
    '''Upgrade file system to newest revision'''

    log.info('Updating file system to newest revision.')
    log.info('Please note that you will not be able to access the file system\n'
             'with any older version of S3QL after this operation.')
    log.info('Continuing in 10 seconds, press Ctrl-C to abort.')
    time.sleep(10)


    # Revision 1
    if 's3ql_parameters' not in bucket:
        upgrade_rev1(bucket)

def upgrade_rev1(bucket):
    '''Upgrade file system from revision 1 to 2'''

    log.info('Updating file system from revision 1 to 2.')

    log.info('Downloading metadata...')
    dbfile = tempfile.NamedTemporaryFile()
    bucket.fetch_fh('s3ql_metadata', dbfile)

    log.info('Updating metadata...')
    dbcm = ConnectionManager(dbfile.name, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
    dbcm.execute('UPDATE parameters SET mountcnt=mountcnt+1')

    param = dict()
    param['revision'] = 2
    param['mountcnt'] = dbcm.get_val('SELECT mountcnt FROM parameters')

    log.info('Uploading metadata')
    bucket.store_wait('s3ql_parameters', pickle.dumps(param, 2))
    bucket.store_fh('s3ql_metadata', open(dbfile, 'r'))

def change_passphrase(bucket):
    '''Change bucket passphrase'''


    if not bucket.has_key('s3ql_passphrase'):
        raise QuietError('Bucket is not encrypted.')

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter old encryption password: ")
    else:
        wrap_pw = sys.stdin.readline().rstrip()
    bucket.passphrase = wrap_pw
    try:
        data_pw = bucket['s3ql_passphrase']
    except s3.ChecksumError:
        raise QuietError('Incorrect password')

    if sys.stdin.isatty():
        wrap_pw = getpass("Enter new encryption password: ")
        if not wrap_pw == getpass("Confirm new encryption password: "):
            raise QuietError("Passwords don't match")
    else:
        wrap_pw = sys.stdin.readline().rstrip()

    bucket.passphrase = wrap_pw
    bucket['s3ql_passphrase'] = data_pw

if __name__ == '__main__':
    main(sys.argv[1:])
