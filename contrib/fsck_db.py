#!/usr/bin/env python3
'''
fsck_db.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C)  Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from argparse import ArgumentTypeError
import os
import sys

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.logging import logging, setup_logging
from s3ql.fsck import ROFsck
from s3ql.parse_args import ArgumentParser

log = logging.getLogger(__name__)

def parse_args(args):

    parser = ArgumentParser(
        description="Checks S3QL file system metadata")

    parser.add_log('~/.s3ql/fsck_db.log')
    parser.add_debug_modules()
    parser.add_quiet()
    parser.add_version()

    def db_path(s):
        s = os.path.splitext(s)[0]
        if not os.path.exists(s + '.db'):
            raise ArgumentTypeError('Unable to read %s.db' % s)
        if not os.path.exists(s + '.params'):
            raise ArgumentTypeError('Unable to read %s.params' % s)
        return s

    parser.add_argument("path", metavar='<path>', type=db_path,
                        help='Database to be checked')

    options = parser.parse_args(args)

    return options

def main(args=None):

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    fsck = ROFsck(options.path)
    fsck.check()

if __name__ == '__main__':
    main(sys.argv[1:])

