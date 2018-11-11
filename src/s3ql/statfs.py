'''
statfs.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging
from .common import assert_fs_owner, pretty_print_size
from .parse_args import ArgumentParser
import pyfuse3
import struct
import sys

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(
        description="Print file system statistics.")

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    parser.add_argument("mountpoint", metavar='<mountpoint>',
                        type=(lambda x: x.rstrip('/')),
                        help='Mount point of the file system to examine')

    parser.add_argument("--raw", action="store_true", default=False,
                          help="Do not pretty-print numbers")

    return parser.parse_args(args)

def main(args=None):
    '''Print file system statistics to sys.stdout'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    if options.raw:
        pprint = lambda x: '%d bytes' % x
    else:
        pprint = pretty_print_size

    ctrlfile = assert_fs_owner(options.mountpoint, mountpoint=True)

    # Use a decent sized buffer, otherwise the statistics have to be
    # calculated three(!) times because we need to invoke getxattr
    # three times.
    buf = pyfuse3.getxattr(ctrlfile, 's3qlstat', size_guess=256)

    (entries, blocks, inodes, fs_size, dedup_size,
     compr_size, db_size, cache_cnt, cache_size, dirty_cnt,
     dirty_size, removal_cnt) = struct.unpack('QQQQQQQQQQQQ', buf)
    p_dedup = dedup_size * 100 / fs_size if fs_size else 0
    p_compr_1 = compr_size * 100 / fs_size if fs_size else 0
    p_compr_2 = compr_size * 100 / dedup_size if dedup_size else 0
    print ('Directory entries:    %d' % entries,
           'Inodes:               %d' % inodes,
           'Data blocks:          %d' % blocks,
           'Total data size:      %s' % pprint(fs_size),
           'After de-duplication: %s (%.2f%% of total)'
             % (pprint(dedup_size), p_dedup),
           'After compression:    %s (%.2f%% of total, %.2f%% of de-duplicated)'
             % (pprint(compr_size), p_compr_1, p_compr_2),
           'Database size:        %s (uncompressed)' % pprint(db_size),
           'Cache size:           %s, %d entries' % (pprint(cache_size), cache_cnt),
           'Cache size (dirty):   %s, %d entries' % (pprint(dirty_size), dirty_cnt),
           'Queued object removals: %d' % (removal_cnt,),
           sep='\n')


if __name__ == '__main__':
    main(sys.argv[1:])
