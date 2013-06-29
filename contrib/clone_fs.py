#!/usr/bin/env python3
'''
clone_fs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Clone an S3QL file system from one backend into another, without
recompressing or reencrypting.

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

import os
import sys
import tempfile

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging, QuietError
from s3ql.backends.common import get_backend, DanglingStorageURLError
from s3ql.common import BUFSIZE
from s3ql.parse_args import ArgumentParser, storage_url_type

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
                description='Clone an S3QL file system.')

    parser.add_authfile()
    parser.add_quiet()
    parser.add_debug()
    parser.add_ssl()
    parser.add_version()
    
    parser.add_argument("src_storage_url", metavar='<source-storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the source backend that contains the file system')

    parser.add_argument("dst_storage_url", metavar='<destination-storage-url>',
                        type=storage_url_type,
                        help='Storage URL of the destination backend')

    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    try:
        options.storage_url = options.src_storage_url
        src_backend = get_backend(options, plain=True)

        options.storage_url = options.dst_storage_url
        dst_backend = get_backend(options, plain=True)
    except DanglingStorageURLError as exc:
        raise QuietError(str(exc)) from None

    tmpfh = tempfile.TemporaryFile()
    for (i, key) in enumerate(src_backend):
        if sys.stdout.isatty():
            sys.stdout.write('\rCopied %d objects so far...' % i)
            sys.stdout.flush()
        metadata = src_backend.lookup(key)

        log.debug('reading object %s', key)
        def do_read(fh):
            tmpfh.seek(0)
            tmpfh.truncate()
            while True:
                buf = fh.read(BUFSIZE)
                if not buf:
                    break
                tmpfh.write(buf)
        src_backend.perform_read(do_read, key)
        
        log.debug('writing object %s', key)
        def do_write(fh):
            tmpfh.seek(0)
            while True:
                buf = tmpfh.read(BUFSIZE)
                if not buf:
                    break
                fh.write(buf)
        dst_backend.perform_write(do_write, key, metadata)

    if sys.stdout.isatty():
        sys.stdout.write('\n')
    
if __name__ == '__main__':
    main(sys.argv[1:])
