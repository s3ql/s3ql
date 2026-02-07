#!/usr/bin/env python3
'''
remove_objects.py - this file is part of S3QL.

Copyright © 2014 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

import argparse
import logging
import sys

import trio

from s3ql.common import async_get_backend
from s3ql.logging import setup_logging, setup_warnings
from s3ql.parse_args import ArgumentParser

log = logging.getLogger(__name__)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser(description='Batch remove objects from an S3QL backend')

    parser.add_storage_url()
    parser.add_quiet()
    parser.add_log()
    parser.add_debug()
    parser.add_backend_options()
    parser.add_version()

    parser.add_argument(
        "file",
        type=argparse.FileType(mode='r', encoding='utf-8'),
        help='File with newline separated object keys to delete',
    )

    return parser.parse_args(args)


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    setup_warnings()
    options = parse_args(args)
    setup_logging(options)

    trio.run(main_async, options)


async def main_async(options: argparse.Namespace) -> None:
    async with await async_get_backend(options, raw=True) as backend:
        for line in options.file:
            key = line.rstrip()
            log.info('Deleting %s', key)
            await backend.delete(key)


if __name__ == '__main__':
    main(sys.argv[1:])
