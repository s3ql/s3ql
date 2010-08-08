#!/usr/bin/env python
'''
pcp.py - this file is part of S3QL (http://s3ql.googlecode.com)

Parallel, recursive copy of directory trees.

---
Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

import sys
import os
import logging
import subprocess

# We are running from the S3QL source directory, make sure
# that we use modules from this directory
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 's3ql', '__init__.py'))):
    sys.path = [os.path.join(basedir, 'src')] + sys.path

from s3ql.common import (add_stdout_logging, setup_excepthook)
from s3ql.optparse import OptionParser

log = logging.getLogger('pcp')

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <source> ... <destination>\n"
              "%prog --help",
        description='Recursively copy source(s) to destination using multiple '
                    'parallel rsync processes.')

    parser.add_option("--debug", action="store_true", default=False,
                      help="Activate debugging output")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet.")
    parser.add_option("-a", action="store_true",
                      help='Pass -a option to rsync.')
    parser.add_option("-H", action="store_true",
                      help='Pass -H option to rsync.')
    parser.add_option("--processes", action="store", type="int", metavar='<no>',
                      default=10,
                      help='Number of rsync processes to use (default: %default).')
    
    (options, pps) = parser.parse_args(args)

    if len(pps) < 2:
        parser.error("Incorrect number of arguments.")
    options.pps = pps

    return options


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)

    # Initialize logging if not yet initialized
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = add_stdout_logging(options.quiet)
        setup_excepthook()  
        if options.debug:
            root_logger.setLevel(logging.DEBUG)
            handler.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO) 
    else:
        log.info("Logging already initialized.")

    pool = ( 'abcdefghijklmnopqrstuvwxyz',
             'ABCDEFGHIJKLMNOPQRSTUVWXYZ',
             '0123456789' )
    steps = [ len(x) / (options.processes-1) for x in pool ]
    prefixes = list()
    for i in range(options.processes - 1):
        parts = [ x[int(i*y):int((i+1)*y)] for (x, y) in zip(pool, steps) ]
        prefixes.append(''.join(parts))

    filters = [ '-! [%s]*' % x for x in prefixes ]
    
    # Catch all
    filters.append( '- [%s]*' % ''.join(prefixes))
        
    rsync_args = [ 'rsync', '-f', '+ */' ]
    if not options.quiet:
        rsync_args.append('--out-format')
        rsync_args.append('%n%L')
    if options.H:
        rsync_args.append('-H')
    if options.a:
        rsync_args.append('-a')
    
    processes = list()
    for filter in filters:
        cmd = rsync_args + [ '-f', filter ] + options.pps
        log.debug('Calling %s', cmd)
        processes.append(subprocess.Popen(cmd))
        
    if all([ c.wait() == 0 for c in processes]):
        sys.exit(0)
    else:
        sys.exit(1)
    

if __name__ == '__main__':
    main(sys.argv[1:])