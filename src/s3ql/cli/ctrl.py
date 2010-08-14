'''
ctrl.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from s3ql import libc
import os
import logging
from s3ql.common import (CTRL_NAME, QuietError, setup_logging)
from s3ql.argparse import ArgumentParser
import textwrap
import sys

log = logging.getLogger("ctrl")

def parse_args(args):
    '''Parse command line'''
    
    parser = ArgumentParser(
        description='''Control a mounted S3QL File System''',
        epilog=textwrap.dedent('''\
               Hint: run `%(prog)s <action> --help` to get help on the additional
               arguments that the different actions take.'''))

    pparser = ArgumentParser(add_help=False, epilog=textwrap.dedent('''\
               Hint: run `%(prog)s --help` to get help on other available actions and
               optional arguments that can be used with all actions.'''))
    pparser.add_argument("mountpoint", metavar='<mountpoint>',
                         type=(lambda x: x.rstrip('/')),
                         help='Mountpoint of the file system')    
        
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()
    
    subparsers = parser.add_subparsers(metavar='<action>', dest='action',
                                       help='may be either of')                               
    subparsers.add_parser('flushcache', help='flush file system cache',
                          parents=[pparser])

    subparsers.add_parser('stacktrace', help='Print stack trace',
                          parents=[pparser])
            
    options = parser.parse_args(args)
    
    return options

    
def main(args=None):
    '''Control a mounted S3QL File System.'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    path = options.mountpoint
    
    if not os.path.exists(path):
        raise QuietError('Mountpoint %r does not exist' % path)
    
    ctrlfile = os.path.join(path, CTRL_NAME)
    if not (CTRL_NAME not in libc.listdir(path) 
            and os.path.exists(ctrlfile)):
        raise QuietError('Mountpoint is not an S3QL file system')

    if os.stat(ctrlfile).st_uid != os.geteuid() and os.geteuid() != 0:
        raise QuietError('Only root and the mounting user may run s3qlctrl.')
    
    if options.action == 'stacktrace':
        libc.setxattr(ctrlfile, 'stacktrace', 'dummy')
    elif options.action == 'flushcache':
        libc.setxattr(ctrlfile, 's3ql_flushcache!', 'dummy')
    
        

if __name__ == '__main__':
    main(sys.argv[1:])
