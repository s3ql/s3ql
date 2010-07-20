'''
ctrl.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from s3ql import libc
from optparse import OptionParser
import os
import logging
from s3ql.common import CTRL_NAME, QuietError, add_stdout_logging, setup_excepthook
import s3ql
import sys

log = logging.getLogger("ctrl")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog  [options] <mountpoint>\n"
              "       %prog --help",
        version='S3QL %s' % s3ql.VERSION,
        description="Control a mounted S3QL File System.")

    parser.add_option("--debug", action="store_true",
                      help="Activate debugging output")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option("--stacktrace", action="store_true", default=False,
                      help="Dump stack trace for all active threads into logfile. "
                           "Useful for debugging deadlocks.")
    parser.add_option("--flush-cache", action="store_true", default=False,
                      help="Flush file system cache. The command blocks until "
                           "the cache has been flushed.")

    (options, pps) = parser.parse_args(args)

    # Verify parameters
    if len(pps) != 1:
        parser.error("Incorrect number of arguments.")
    options.mountpoint = pps[0].rstrip('/')

    actions = [options.stacktrace, options.flush_cache]
    selected = len([ act for act in actions if act ])
    if selected != 1:
        parser.error("Need to specify exactly one action.")
        
    return options

def main(args=None):
    '''Control a mounted S3QL File System.'''

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

    if not os.path.exists(options.mountpoint):
        raise QuietError('Mountpoint %r does not exist' % options.mountpoint)

    ctrlfile = os.path.join(options.mountpoint, CTRL_NAME)
    if not (CTRL_NAME not in libc.listdir(options.mountpoint) 
            and os.path.exists(ctrlfile)):
        raise QuietError('Mountpoint is not an S3QL file system')

    if options.stacktrace:
        libc.setxattr(ctrlfile, 'stacktrace', 'dummy')
        
    elif options.flush_cache:
        libc.setxattr(ctrlfile, 's3ql_flushcache!', 'dummy')
        

if __name__ == '__main__':
    main(sys.argv[1:])
