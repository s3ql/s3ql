'''
lock.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .common import setup_logging, CTRL_NAME, QuietError
from .parse_args import ArgumentParser
import cPickle as pickle
import llfuse
import logging
import os
import sys
import textwrap

log = logging.getLogger("lock")

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Makes the given directory tree(s) immutable. No changes of any sort can
        be performed on the tree after that. Immutable entries can only be
        deleted with s3qlrm. 
        '''))

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    parser.add_argument('path', metavar='<path>', nargs='+',
                        help='Directories to make immutable.',
                         type=(lambda x: x.rstrip('/')))

    return parser.parse_args(args)


def main(args=None):
    '''Make directory tree immutable'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    for name in options.path:
        if not os.path.exists(name):
            raise QuietError('%r does not exist' % name)

        parent = os.path.dirname(os.path.abspath(name))
        fstat_p = os.stat(parent)
        fstat = os.stat(name)

        if fstat_p.st_dev != fstat.st_dev:
            raise QuietError('%s is a mount point itself.' % name)

        ctrlfile = os.path.join(parent, CTRL_NAME)
        if not (CTRL_NAME not in llfuse.listdir(parent) and os.path.exists(ctrlfile)):
            raise QuietError('%s is not on an S3QL file system' % name)

        if os.stat(ctrlfile).st_uid != os.geteuid():
            raise QuietError('Only root and the mounting user may run s3qllock.')

        llfuse.setxattr(ctrlfile, 'lock', pickle.dumps((fstat.st_ino,),
                                                       pickle.HIGHEST_PROTOCOL))

if __name__ == '__main__':
    main(sys.argv[1:])
