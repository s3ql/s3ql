#!/usr/bin/env python3
'''
remove_objects.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import os
import sys
import argparse
import atexit

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging
from s3ql.common import get_backend
from s3ql.parse_args import ArgumentParser, storage_url_type

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(description='Batch remove objects from an S3QL backend')

    parser.add_authfile()
    parser.add_quiet()
    parser.add_debug()
    parser.add_backend_options()
    parser.add_version()

    parser.add_argument("storage_url", type=storage_url_type,
                        help='Storage URL of the backend to delete from')

    parser.add_argument("file", type=argparse.FileType(mode='r', encoding='utf-8'),
                        help='File with newline separated object keys to delete')

    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    backend = get_backend(options, raw=True)
    atexit.register(backend.close)

    for line in options.file:
        key = line.rstrip()
        log.info('Deleting %s', key)
        backend.delete(key)

if __name__ == '__main__':
    main(sys.argv[1:])
