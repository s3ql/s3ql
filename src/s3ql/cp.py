'''
cp.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import os
import stat
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
async def cp(
    source: Annotated[str, typer.Argument(metavar='<source>', help='source directory')],
    target: Annotated[str, typer.Argument(metavar='<target>', help='target directory')],
    log_target: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Replicate the contents of the directory <source> in the directory <target>.

    <source> has to be an existing directory and <target> must not exist. Both directories
    have to be within the same S3QL file system. The replication will not take any additional
    space. Only if one of directories is modified later on, the modified data will take
    additional storage space.
    '''
    setup_logging(quiet=quiet, log=log_target, debug=debug, debug_modules=debug_modules)

    source = source.rstrip('/')
    target = target.rstrip('/')

    if not os.path.exists(source):
        raise QuietError('Source directory %r does not exist' % source)

    if os.path.exists(target):
        raise QuietError('Target directory %r must not yet exist.' % target)

    parent = os.path.dirname(os.path.abspath(target))
    if not os.path.exists(parent):
        raise QuietError('Target parent %r does not exist' % parent)

    fstat_s = os.stat(source)
    fstat_p = os.stat(parent)
    if not stat.S_ISDIR(fstat_s.st_mode):
        raise QuietError('Source %r is not a directory' % source)

    if not stat.S_ISDIR(fstat_p.st_mode):
        raise QuietError('Target parent %r is not a directory' % parent)

    if fstat_p.st_dev != fstat_s.st_dev:
        raise QuietError('Source and target are not on the same file system.')

    if os.path.ismount(source):
        raise QuietError('%s is a mount point.' % source)

    ctrlfile = assert_fs_owner(source)
    try:
        os.mkdir(target)
    except PermissionError:
        raise QuietError('No permission to create target directory')

    # Make sure that write cache is flushed
    pyfuse3.syncfs(target)

    # Ensure the inode of the target folder stays in the kernel dentry cache
    # (We invalidate it during the copy)
    with os.scandir(target):
        fstat_t = os.stat(target)
        pyfuse3.setxattr(ctrlfile, 'copy', ('(%d, %d)' % (fstat_s.st_ino, fstat_t.st_ino)).encode())


def main(args: Sequence[str] | None = None) -> None:
    '''Efficiently copy a directory tree'''

    run_app(app, args, prog_name='s3qlcp')


if __name__ == '__main__':
    main(sys.argv[1:])
