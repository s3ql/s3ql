'''
statfs.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging
from .common import assert_fs_owner, pretty_print_size
from .parse_args import ArgumentParser
import llfuse
import struct
import sys

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Print file system statistics.")

    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    parser.add_fatal_warnings()

    parser.add_argument("mountpoint", metavar='<mountpoint>',
                        type=(lambda x: x.rstrip('/')),
                        help='Mount point of the file system to examine')

    return parser.parse_args(args)

def main(args=None):
    '''Print file system statistics to sys.stdout'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    ctrlfile = assert_fs_owner(options.mountpoint, mountpoint=True)

    # Use a decent sized buffer, otherwise the statistics have to be
    # calculated thee(!) times because we need to invoce getxattr
    # three times.
    buf = llfuse.getxattr(ctrlfile, 's3qlstat', size_guess=256)

    (entries, blocks, inodes, fs_size, dedup_size,
     compr_size, db_size) = struct.unpack('QQQQQQQ', buf)
    p_dedup = dedup_size * 100 / fs_size if fs_size else 0
    p_compr_1 = compr_size * 100 / fs_size if fs_size else 0
    p_compr_2 = compr_size * 100 / dedup_size if dedup_size else 0
    print ('Directory entries:    %d' % entries,
           'Inodes:               %d' % inodes,
           'Data blocks:          %d' % blocks,
           'Total data size:      %s' % pretty_print_size(fs_size),
           'After de-duplication: %s (%.2f%% of total)'
             % (pretty_print_size(dedup_size), p_dedup),
           'After compression:    %s (%.2f%% of total, %.2f%% of de-duplicated)'
             % (pretty_print_size(compr_size), p_compr_1, p_compr_2),
           'Database size:        %s (uncompressed)' % pretty_print_size(db_size),
           '(some values do not take into account not-yet-uploaded dirty blocks in cache)',
           sep='\n')


if __name__ == '__main__':
    main(sys.argv[1:])
