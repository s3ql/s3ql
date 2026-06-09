'''
lock.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import os
import sys
from collections.abc import Sequence
from typing import Annotated

import pyfuse3
import typer

from .common import assert_fs_owner
from .logging import QuietError, setup_logging
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


@app.command()
@trio_command
async def lock(
    path: Annotated[
        list[str], typer.Argument(metavar='<path>', help='Directories to make immutable.')
    ],
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Make the given directory tree(s) immutable.

    No changes of any sort can be performed on the tree after that. Immutable entries can
    only be deleted with s3qlrm.
    '''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    for name in path:
        name = name.rstrip('/')
        if os.path.ismount(name):
            raise QuietError('%s is a mount point.' % name)
        ctrlfile = assert_fs_owner(name)
        fstat = os.stat(name)
        pyfuse3.setxattr(ctrlfile, 'lock', ('%d' % fstat.st_ino).encode())


def main(args: Sequence[str] | None = None) -> None:
    '''Make directory tree immutable'''

    run_app(app, args, prog_name='s3qllock')


if __name__ == '__main__':
    main(sys.argv[1:])
