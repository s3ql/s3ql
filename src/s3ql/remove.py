'''
remove.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import errno
import logging
import os
import sys
from collections.abc import Sequence
from typing import Annotated

import pyfuse3
import typer

from .common import assert_fs_owner, path2bytes
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
async def remove(
    path: Annotated[list[str], typer.Argument(metavar='<path>', help='Directories to remove')],
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Recursively delete files and directories in an S3QL file system.

    This includes immutable entries.
    '''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    for name in path:
        name = name.rstrip('/')
        if os.path.ismount(name):
            raise QuietError('%s is a mount point.' % name)

        parent = os.path.join(name, '..')
        ctrlfile = assert_fs_owner(parent)
        fstat_p = os.stat(parent)

        # Make sure that write cache is flushed
        pyfuse3.syncfs(name)

        cmd = ('(%d, %r)' % (fstat_p.st_ino, path2bytes(os.path.basename(name)))).encode()
        try:
            pyfuse3.setxattr(ctrlfile, 'rmtree', cmd)
        except OSError as exc:
            if exc.errno == errno.ENOTEMPTY:
                print(
                    f'Unable to remove {name}: not empty even after removing all contents!\n',
                    file=sys.stderr,
                )
                sys.exit(1)
            raise


def main(args: Sequence[str] | None = None) -> None:
    '''Recursively delete files and directories in an S3QL file system'''

    run_app(app, args, prog_name='s3qlrm')


if __name__ == '__main__':
    main(sys.argv[1:])
