#!/usr/bin/env python
'''
make_dummy.py - this file is part of S3QL (http://s3ql.googlecode.com)

Creates a dummy copy of an entire bucket. The dummy will appear to contain
all the data of the original bucket. However, in fact only the metadata
will be copied and all files contain just \0 bytes. 

---
Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''


from __future__ import division, print_function, absolute_import

import sys
import os
import logging
import tempfile

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.common import (add_stdout_logging, setup_excepthook, QuietError,
                         unlock_bucket,
                         get_backend, LoggerFilter, canonicalize_storage_url)
from s3ql.backends.common import ChecksumError
from s3ql.optparse import OptionParser

log = logging.getLogger('make_dummy')

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <source-storage-url> <dest-storage-url>\n"
              "%prog --help",
        description="Create a dummy-copy of the source bucket. The target will "
                    'contain a file system with the same structure, but all files'
                    'will just contain \\0 bytes.')

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
    
    (options, pps) = parser.parse_args(args)

    #
    # Verify parameters
    #
    if len(pps) != 2:
        parser.error("Incorrect number of arguments.")
    options.src = canonicalize_storage_url(pps[0])
    options.dest = canonicalize_storage_url(pps[1])
        
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


    with get_backend(options.src, options.homedir) as (src_conn, src_name):
        
        if not src_name in src_conn:
            raise QuietError("Source bucket does not exist.")
        src_bucket = src_conn.get_bucket(src_name)
        
        try:
            unlock_bucket(options.homedir, options.src, src_bucket)
        except ChecksumError:
            raise QuietError('Checksum error - incorrect password?')
        
        with get_backend(options.dest, options.homedir) as (dest_conn, dest_name):
        
            if dest_name in dest_conn:
                raise QuietError("Bucket already exists!\n"
                                 "(you can delete an existing bucket with s3qladm --delete)\n")

            dest_bucket = dest_conn.create_bucket(dest_name, compression=None)            

            copy_objects(src_bucket, dest_bucket)

        
def copy_objects(src_bucket, dest_bucket):        
        
    log.info('Copying...')
    
    fh = tempfile.TemporaryFile()
    for (no, key) in enumerate(src_bucket):
        if no != 0 and no % 5000 == 0:
            log.info('Copied %d objects so far..', no)

        if key.startswith('s3ql_data_'):
            dest_bucket[key] = key
        elif key == 's3ql_passphrase':
            pass
        else:
            log.info('Copying %s..', key)
        
            fh.seek(0)
            meta = src_bucket.fetch_fh(key, fh, plain=True)
            fh.seek(0)
            dest_bucket.store_fh(key, fh, meta)
            
    log.info('Done.')


if __name__ == '__main__':
    main(sys.argv[1:])
