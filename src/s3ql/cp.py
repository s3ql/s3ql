'''
cp.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging, QuietError
from .common import assert_fs_owner
from . import PICKLE_PROTOCOL
from .parse_args import ArgumentParser
import llfuse
import os
import pickle
import stat
import sys
import textwrap

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Replicates the contents of the directory <source> in the
        directory <target>. <source> has to be an existing directory and
        <target>  must not exist. Both directories have to be within
        the same S3QL file system.

        The replication will not take any additional space. Only if one
        of directories is modified later on, the modified data will take
        additional storage space.
        '''))

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_fatal_warnings()

    parser.add_argument('source', help='source directory',
                        type=(lambda x: x.rstrip('/')))
    parser.add_argument('target', help='target directory',
                        type=(lambda x: x.rstrip('/')))

    options = parser.parse_args(args)

    return options

def main(args=None):
    '''Efficiently copy a directory tree'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    if not os.path.exists(options.source):
        raise QuietError('Source directory %r does not exist' % options.source)

    if os.path.exists(options.target):
        raise QuietError('Target directory %r must not yet exist.' % options.target)

    parent = os.path.dirname(os.path.abspath(options.target))
    if not os.path.exists(parent):
        raise QuietError('Target parent %r does not exist' % parent)

    fstat_s = os.stat(options.source)
    fstat_p = os.stat(parent)
    if not stat.S_ISDIR(fstat_s.st_mode):
        raise QuietError('Source %r is not a directory' % options.source)

    if not stat.S_ISDIR(fstat_p.st_mode):
        raise QuietError('Target parent %r is not a directory' % parent)

    if fstat_p.st_dev != fstat_s.st_dev:
        raise QuietError('Source and target are not on the same file system.')

    if os.path.ismount(options.source):
        raise QuietError('%s is a mount point.' % options.source)

    ctrlfile = assert_fs_owner(options.source)
    try:
        os.mkdir(options.target)
    except PermissionError:
        raise QuietError('No permission to create target directory')

    fstat_t = os.stat(options.target)
    llfuse.setxattr(ctrlfile, 'copy', pickle.dumps((fstat_s.st_ino, fstat_t.st_ino),
                                                    PICKLE_PROTOCOL))


if __name__ == '__main__':
    main(sys.argv[1:])
