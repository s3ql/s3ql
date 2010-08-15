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
import cPickle as pickle

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

    sparser = subparsers.add_parser('log', help='Change log level',
                                    parents=[pparser])
    sparser.add_argument('level', choices=('debug', 'info', 'warn'),
                         metavar='<level>',
                         help='Desired new log level for mount.s3ql process. '
                              'Allowed values: %(choices)s')
    sparser.add_argument('modules', nargs='*', metavar='<module>', 
                         help='Modules to enable debugging output for. Specify '
                              '`all` to enable debugging for all modules.')
                
    options = parser.parse_args(args)
    
    if options.level != 'debug' and options.modules:
        parser.error('Modules can only be specified with `debug` logging level.')
    
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
    elif options.action == 'log':
        libc.setxattr(ctrlfile, 'logging', 
                      pickle.dumps((options.level, options.modules),
                                   pickle.HIGHEST_PROTOCOL))
        

if __name__ == '__main__':
    main(sys.argv[1:])
