#!/usr/bin/env python
'''
s3_copy.py - this file is part of S3QL (http://s3ql.googlecode.com)

Migrate an S3 bucket to a different storage region or storage class.

---
Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import sys
import time
import os
import logging
import re

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.common import (add_stdout_logging, setup_excepthook, QuietError,
                         get_backend_credentials, ExceptionStoringThread,
                         LoggerFilter)
from s3ql.backends import s3
from s3ql.backends.boto.s3.connection import Location
from s3ql.optparse import OptionParser

log = logging.getLogger('s3_copy')

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <source-bucket> <dest-bucket>\n"
              "%prog --help",
        description="Remote-copy source-bucket to dest-bucket, optionally changing "
                    'storage region and storage class.')

    parser.add_option("--homedir", type="string",
                      default=os.path.expanduser("~/.s3ql"),
                      help='Directory for log files, cache and authentication info. '
                      'Default: ~/.s3ql')
    parser.add_option("--debug", action="append",
                      help="Activate debugging output from specified module. Use 'all' "
                           "to get debug messages from all modules. This option can be "
                           "specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--location", type="string", default='EU',
                      help="New storage location. Allowed values: EU,"
                           'us-west-1, ap-southeast-1, or us-standard.')
    parser.add_option("--rrs", action="store_true", default=False,
                      help="Use reduced redundancy storage (RSS) for new "
                           'bucket. If not specified, standard storage is used.')        
    
    (options, pps) = parser.parse_args(args)

    #
    # Verify parameters
    #
    if len(pps) != 2:
        parser.error("Incorrect number of arguments.")
    options.src = pps[0]
    options.dest = pps[1]

    if options.location not in ('EU', 'us-west-1', 'us-standard', 'ap-southeast-1'):
        parser.error("Invalid S3 storage location. "
                     "Allowed values: EU, us-west-1, ap-southeast-1, us-standard")
        
    if options.location == 'us-standard':
        options.location = Location.DEFAULT
        
    if options.rrs:
        options.storage_class = 'REDUCED_REDUNDANCY'
    else:
        options.storage_class = 'STANDARD'
        
    return options


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)

    # Initialize logging if not yet initialized
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = add_stdout_logging(options.quiet)
        setup_excepthook()  
        if options.debug:
            root_logger.setLevel(logging.DEBUG)
            handler.setLevel(logging.DEBUG)
            if 'all' not in options.debug:
                root_logger.addFilter(LoggerFilter(options.debug, logging.INFO))
        else:
            root_logger.setLevel(logging.INFO) 
    else:
        log.info("Logging already initialized.")

    (login, password) = get_backend_credentials(options.homedir, 's3', None)
    conn = s3.Connection(login, password)
    
    if not re.match('^[a-z][a-z0-9-]*$', options.dest):
        raise QuietError('Invalid destination name. Name must consist only of lowercase letters,\n'
                         'digits and dashes, and the first character has to be a letter.')

    if conn.bucket_exists(options.dest):
        raise QuietError('Destination bucket already exists.')

    if not conn.bucket_exists(options.src):
        raise QuietError('Source bucket does not exist.')
    
    src_bucket = conn.get_bucket(options.src)
    dest_bucket = conn.create_bucket(options.dest, location=options.location)

    log.info('Copying objects into new bucket..')
    threads = list()
    for (no, key) in enumerate(src_bucket):
        if no != 0 and no % 500 == 0:
            log.info('Copied %d objects so far..', no)

        def cp(key=key):
            with dest_bucket._get_boto() as boto:
                s3.retry_boto(boto.copy_key, key, src_bucket.name, key,
                              storage_class=options.storage_class)

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
