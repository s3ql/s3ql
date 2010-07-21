'''
ctrl.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from s3ql import libc
import os
import logging
from s3ql.common import (CTRL_NAME, QuietError, add_stdout_logging, setup_excepthook)
from s3ql.optparse import (OptionParser, ArgumentGroup)
import sys

log = logging.getLogger("ctrl")

def parse_args(args):
    '''Parse command line'''

    parser = OptionParser(
        usage="%prog [options] <action> <mountpoint>\n"
              "%prog --help",
        description='''Control a mounted S3QL File System''')

    group = ArgumentGroup(parser, "<action> may be either of")
    group.add_argument("stacktrace", "Dump stack trace for all active threads into logfile.")
    group.add_argument("flushcache", "Flush file system cache. The command blocks until "
                                      "the cache has been flushed.")
    parser.add_option_group(group)

    parser.add_option("--debug", action="store_true",
                      help="Activate debugging output")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")

    (options, pps) = parser.parse_args(args)
    
    # Verify parameters
    if len(pps) != 2:
        parser.error("Incorrect number of arguments.")
    options.action = pps[0]
    options.mountpoint = pps[1].rstrip('/')

    if options.action not in group.arguments:
        parser.error("Invalid <action>: %s" % options.action)
        
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

    if options.action == 'stacktrace':
        libc.setxattr(ctrlfile, 'stacktrace', 'dummy')
        
    elif options.action == 'flushcache':
        libc.setxattr(ctrlfile, 's3ql_flushcache!', 'dummy')
        

if __name__ == '__main__':
    main(sys.argv[1:])
