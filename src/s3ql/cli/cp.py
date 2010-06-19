'''
cp.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from s3ql import libc
from optparse import OptionParser
import os
import logging
from s3ql.common import init_logging_from_options, CTRL_NAME, QuietError
import struct
import stat
import sys

log = logging.getLogger("cp")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog  [options] <source> <target>\n"
              "       %prog --help",
        description="Efficiently copy directory trees.")

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

    # Verify parameters
    if len(pps) != 2:
        parser.error("Incorrect number of arguments.")
    options.source = pps[0].rstrip('/')
    options.target = pps[1].rstrip('/')

    return options

def main(args=None):
    '''Efficiently copy a directory tree'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    init_logging_from_options(options, 'cp.log')

    if not os.path.exists(options.source):
        log.error('Source directory %r does not exist', options.source)
        raise QuietError(1)

    if os.path.exists(options.target):
        log.error('Target directory %r must not yet exist.', options.target)
        raise QuietError(1)

    parent = os.path.dirname(os.path.abspath(options.target))
    if not os.path.exists(parent):
        log.error('Target parent %r does not exist', parent)
        raise QuietError(1)

    fstat_s = os.stat(options.source)
    fstat_p = os.stat(parent)
    if not stat.S_ISDIR(fstat_s.st_mode):
        log.error('Source %r is not a directory', options.source)
        raise QuietError(1)

    if not stat.S_ISDIR(fstat_p.st_mode):
        log.error('Target parent %r is not a directory', parent)
        raise QuietError(1)

    if fstat_p.st_dev != fstat_s.st_dev:
        log.error('Source and target are not on the same file system.')
        raise QuietError(1)

    ctrlfile = os.path.join(parent, CTRL_NAME)
    if not (CTRL_NAME not in libc.listdir(parent) and os.path.exists(ctrlfile)):
        log.error('Source and target are not on an S3QL file system')
        raise QuietError(1)

    os.mkdir(options.target)
    fstat_t = os.stat(options.target)
    libc.setxattr(ctrlfile, 'copy', struct.pack('II', fstat_s.st_ino, fstat_t.st_ino))


if __name__ == '__main__':
    main(sys.argv[1:])
