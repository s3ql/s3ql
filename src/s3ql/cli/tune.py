'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from optparse import OptionParser 
import logging
from ..common import (init_logging_from_options, get_credentials, QuietError)
from getpass import getpass
import sys
from .. import s3
import os

log = logging.getLogger("tune.s3ql")

def parse_args(args):
    '''Parse command line'''
        
    parser = OptionParser(
        usage="%prog [options] <bucketname>\n"
              "       %prog --help",
        description="Change or show S3QL file system parameters.")
    
    parser.add_option("--debuglog", type="string",
                      help="Write debugging information in specified file.")
    parser.add_option("--debug", action="append", 
                      help="Activate debugging output from specified facility. "
                           "This option can be specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--change-passphrase", action="store_true", default=False,
                      help="Change bucket passphrase")    
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
    
    return options
    
def main(args):
    '''Change or show S3QL file system parameters'''

    options = parse_args(args)
    init_logging_from_options(options)

    (awskey, awspass) = get_credentials(options.credfile, options.awskey)
    conn = s3.Connection(awskey, awspass)
    if not conn.bucket_exists(options.bucketname):
        raise QuietError("Bucket does not exist.")
    bucket = conn.get_bucket(options.bucketname)
    
    if options.change_passphrase:
        change_passphrase(bucket)
        
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
