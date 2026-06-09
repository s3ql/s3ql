'''
ctrl.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from enum import Enum
from typing import Annotated

import pyfuse3
import typer

from .common import assert_fs_owner
from .logging import setup_logging
from .parse_args import (
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    make_app,
    run_app,
    trio_command,
)

log: logging.Logger = logging.getLogger(__name__)

app = make_app()

MountPoint = Annotated[
    str, typer.Argument(metavar='<mountpoint>', help='Mountpoint of the file system')
]


class LogLevel(str, Enum):
    debug = 'debug'
    info = 'info'
    warn = 'warn'


@app.callback()
def _setup(
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Control a mounted S3QL File System.'''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)


def _open_ctrlfile(mountpoint: str) -> str:
    return assert_fs_owner(mountpoint.rstrip('/'), mountpoint=True)


@app.command()
@trio_command
async def flushcache(mountpoint: MountPoint) -> None:
    '''Flush file system cache.'''
    pyfuse3.setxattr(_open_ctrlfile(mountpoint), 's3ql_flushcache!', b'dummy')


@app.command()
@trio_command
async def dropcache(mountpoint: MountPoint) -> None:
    '''Drop file system cache.'''
    pyfuse3.setxattr(_open_ctrlfile(mountpoint), 's3ql_dropcache!', b'dummy')


@app.command(name='backup-metadata')
@trio_command
async def backup_metadata(mountpoint: MountPoint) -> None:
    '''Trigger immediate metadata backup.'''
    pyfuse3.setxattr(_open_ctrlfile(mountpoint), 'upload-meta', b'dummy')


@app.command()
@trio_command
async def cachesize(
    mountpoint: MountPoint,
    size: Annotated[int, typer.Argument(metavar='<size>', help='New cache size in KiB')],
) -> None:
    '''Change cache size.'''
    pyfuse3.setxattr(_open_ctrlfile(mountpoint), 'cachesize', ('%d' % (size * 1024,)).encode())


@app.command(name='log')
@trio_command
async def set_log_level(
    mountpoint: MountPoint,
    level: Annotated[
        LogLevel,
        typer.Argument(metavar='<level>', help='Desired new log level for mount.s3ql process'),
    ],
    modules: Annotated[
        list[str] | None,
        typer.Argument(
            metavar='<module>',
            help="Modules to enable debugging output for. Specify 'all' to enable debugging "
            'for all modules.',
        ),
    ] = None,
) -> None:
    '''Change log level.'''
    if level is not LogLevel.debug and modules:
        raise typer.BadParameter('Modules can only be specified with `debug` logging level.')
    if not modules:
        modules = ['all']

    level_no = getattr(logging, level.value.upper())
    cmd = ('(%r, %r)' % (level_no, ','.join(modules))).encode()
    pyfuse3.setxattr(_open_ctrlfile(mountpoint), 'logging', cmd)


def main(args: Sequence[str] | None = None) -> None:
    '''Control a mounted S3QL File System.'''

    run_app(app, args, prog_name='s3qlctrl')


if __name__ == '__main__':
    main(sys.argv[1:])
