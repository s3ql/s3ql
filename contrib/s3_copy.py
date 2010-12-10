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
import os
import logging

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.common import (setup_logging, QuietError,
                         get_backend_credentials, AsyncFn )
from s3ql.backends import s3
from s3ql.backends.boto.s3.connection import Location
from s3ql.parse_args import ArgumentParser

log = logging.getLogger('s3_copy')

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Remote-copy source-bucket to dest-bucket, optionally changing "
                    'storage region and storage class.')

    parser.add_homedir()
    parser.add_quiet()
    parser.add_debug_modules()
    parser.add_version()

    parser.add_argument("src", metavar='<source bucket>',
                        help='Source bucket name')
    parser.add_argument("dest", metavar='<dest bucket>',
                        help='Destination bucket name')
    parser.add_argument("--location", default='EU', metavar='<name>',
                        choices=('EU', 'us-west-1', 'us-standard', 'ap-southeast-1'),
                        help="Storage location for new bucket. Allowed values: `EU`, "
                        '`us-west-1`, `ap-southeast-1`, or `us-standard`.')
    
    parser.add_argument("--rrs", action="store_true", default=False,
                      help="Use reduced redundancy storage (RSS) for new "
                           'bucket. If not specified, standard storage is used.')        
    
    options = parser.parse_args(args)
        
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
    setup_logging(options)
    
    (login, password) = get_backend_credentials(options.homedir, 's3', None)
    conn = s3.Connection(login, password)

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
            # Access to protected member
            #pylint: disable=W0212
            with dest_bucket._get_boto() as boto:
                s3.retry_boto(boto.copy_key, key, src_bucket.name, key,
                              storage_class=options.storage_class)

        t = AsyncFn(cp)
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
