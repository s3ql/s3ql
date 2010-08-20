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

from s3ql.common import (setup_logging, QuietError,
                         unlock_bucket, get_backend)
from s3ql.backends.common import ChecksumError
from s3ql.argparse import ArgumentParser, storage_url_type

log = logging.getLogger('make_dummy')

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Create a dummy-copy of the source bucket. The target will "
                    'contain a file system with the same structure, but all files'
                    'will just contain \\0 bytes.')

    parser.add_homedir()
    parser.add_quiet()
    parser.add_debug_modules()
    parser.add_version()

    parser.add_argument("src", metavar='<source storage-url>',
                        type=storage_url_type, 
                        help='Source storage URL')

    parser.add_argument("dest", metavar='<dest storage-url>',
                        type=storage_url_type, 
                        help='Destination storage URL')

        
    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

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
    
    
    for (no, key) in enumerate(src_bucket):
        if no != 0 and no % 5000 == 0:
            log.info('Copied %d objects so far..', no)

        if key.startswith('s3ql_data_'):
            dest_bucket[key] = key
        elif key == 's3ql_passphrase' or key.startswith('s3ql_metadata_bak'):
            pass
        else:
            log.info('Copying %s..', key)
        
            fh = tempfile.TemporaryFile()
            meta = src_bucket.fetch_fh(key, fh, plain=True)
            fh.seek(0)
            dest_bucket.store_fh(key, fh, meta)
            fh.close()
            
    log.info('Done.')


if __name__ == '__main__':
    main(sys.argv[1:])
