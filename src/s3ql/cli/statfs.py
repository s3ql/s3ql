'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from s3ql import libc
from optparse import OptionParser 
import os
import logging
from s3ql.common import init_logging_from_options, CTRL_NAME, QuietError
import posixpath
import struct

log = logging.getLogger("stat.s3ql")

def parse_args(args):
    '''Parse command line'''
        
    parser = OptionParser(
        usage="%prog  [options] <mountpoint>\n"
              "       %prog --help",
        description="Print file system statistics.")
    
    parser.add_option("--debuglog", type="string",
                      help='Write debugging information in specified file.')
    parser.add_option("--debug", action="append", 
                      help="Activate debugging output from specified facility. "
                           "This option can be specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
                          
    (options, pps) = parser.parse_args(args)
    
    # Verify parameters
    if not len(pps) == 1:
        parser.error("Wrong number of parameters")
    options.mountpoint = pps[0].rstrip('/')
    
    return options
    
def main(args):
    '''Print file system statistics to sys.stdout'''

    options = parse_args(args)
    mountpoint = options.mountpoint
    init_logging_from_options(options)

    # Check if it's a mount point
    if not posixpath.ismount(mountpoint):
        raise QuietError('%s is not a mount point' % mountpoint)

    # Check if it's an S3QL mountpoint
    ctrlfile = os.path.join(mountpoint, CTRL_NAME) 
    if not (CTRL_NAME not in libc.listdir(mountpoint)
            and os.path.exists(ctrlfile)):
        raise QuietError('%s is not a mount point' % mountpoint)
    
    log.info('Gathering statistics (this may take a while)...')
    # Use a decent sized buffer, otherwise the statistics have to be
    # calculated thee(!) times because we need to invoce getxattr
    # three times.
    buf = libc.getxattr(ctrlfile, b'stat.s3ql', size_guess=256)
     
    (entries, blocks, inodes, size_1, size_2, 
     size_3, dbsize) = struct.unpack('LLLLLLL', buf)
    print ('Directory entries:    %d\n'        
           'Inodes:               %d\n' 
           'Data blocks:          %d\n' 
           'Total data size:      %.2f MB\n'
           'After de-duplication: %.2f MB (%.2f%% of total)\n' 
           'After compression:    %.2f MB (%.2f%% of total, %.2f%% of de-duplicated)\n'
           'Database size:        %.2f MB'
            % (entries, inodes, blocks, size_1/1024**2, size_2/1024**2, size_2/size_1 * 100,
               size_3/1024**2, size_3/size_1 * 100, size_3/size_2 * 100, dbsize / 1024**2))
            


if __name__ == '__main__':
    main(sys.argv[1:])    
