'''
remove.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .common import assert_fs_owner, PICKLE_PROTOCOL, path2bytes
from .parse_args import ArgumentParser
import llfuse
import os
import pickle
import sys
import textwrap

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Recursively delete files and directories in an S3QL file system,
        including immutable entries.
        '''))

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_fatal_warnings()

    parser.add_argument('path', metavar='<path>', nargs='+',
                        help='Directories to remove',
                         type=(lambda x: x.rstrip('/')))

    return parser.parse_args(args)


def main(args=None):
    '''Recursively delete files and directories in an S3QL file system'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    for name in options.path:
        if os.path.ismount(name):
            raise QuietError('%s is a mount point.' % name)

        ctrlfile = assert_fs_owner(name)
        fstat_p = os.stat(os.path.dirname(os.path.abspath(name)))
        llfuse.setxattr(ctrlfile, 'rmtree', pickle.dumps((fstat_p.st_ino,
                                                          path2bytes(os.path.basename(name))),
                                                          PICKLE_PROTOCOL))


if __name__ == '__main__':
    main(sys.argv[1:])
