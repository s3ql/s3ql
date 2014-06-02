'''
ctrl.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2010 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging
from .common import assert_fs_owner, PICKLE_PROTOCOL
from .parse_args import ArgumentParser
import llfuse
import pickle
import sys
import textwrap

log = logging.getLogger(__name__)

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
    parser.add_fatal_warnings()

    subparsers = parser.add_subparsers(metavar='<action>', dest='action',
                                       help='may be either of')
    subparsers.add_parser('flushcache', help='flush file system cache',
                          parents=[pparser])
    subparsers.add_parser('upload-meta', help='Upload metadata',
                          parents=[pparser])

    sparser = subparsers.add_parser('cachesize', help='Change cache size',
                                    parents=[pparser])
    sparser.add_argument('cachesize', metavar='<size>', type=int,
                         help='New cache size in KiB')

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

    if options.action == 'log':
        if options.level != 'debug' and options.modules:
            parser.error('Modules can only be specified with `debug` logging level.')
        if not options.modules:
            options.modules = [ 'all' ]

        if options.level:
            # Protected member ok, hopefully this won't break
            #pylint: disable=W0212
            options.level = logging._levelNames[options.level.upper()]

    return options


def main(args=None):
    '''Control a mounted S3QL File System.'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    path = options.mountpoint

    ctrlfile = assert_fs_owner(path, mountpoint=True)

    if options.action == 'flushcache':
        llfuse.setxattr(ctrlfile, 's3ql_flushcache!', b'dummy')

    if options.action == 'upload-meta':
        llfuse.setxattr(ctrlfile, 'upload-meta', b'dummy')

    elif options.action == 'log':
        llfuse.setxattr(ctrlfile, 'logging',
                      pickle.dumps((options.level, options.modules),
                                   PICKLE_PROTOCOL))

    elif options.action == 'cachesize':
        llfuse.setxattr(ctrlfile, 'cachesize',
                        pickle.dumps(options.cachesize * 1024, PICKLE_PROTOCOL))



if __name__ == '__main__':
    main(sys.argv[1:])
