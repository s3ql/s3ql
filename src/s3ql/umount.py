'''
umount.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import annotations

import logging
import os
import platform
import shutil
import subprocess
import sys
import time
from collections.abc import Sequence
from typing import Annotated

import pyfuse3
import typer

from . import CTRL_NAME
from .common import assert_s3ql_mountpoint, parse_literal
from .logging import setup_logging
from .parse_args import (
    DebugFlag,
    DebugModules,
    LogDest,
    QuietFlag,
    run_app,
    trio_command,
)

log: logging.Logger = logging.getLogger(__name__)

app = typer.Typer(add_completion=False, rich_markup_mode=None, pretty_exceptions_enable=False)


def _fusermount() -> str:
    '''Return path to fusermount3 if available, otherwise 'fusermount'.'''
    return shutil.which('fusermount3') or 'fusermount'


class UmountError(Exception):
    """
    Base class for unmount errors.
    """

    message = 'internal error'
    exitcode = 3

    def __init__(self, mountpoint: str) -> None:
        super().__init__()
        self.mountpoint = mountpoint

    def __str__(self) -> str:
        return self.message


class UmountSubError(UmountError):
    message = 'Unmount subprocess failed.'
    exitcode = 2


class MountInUseError(UmountError):
    message = 'In use.'
    exitcode = 1


def lazy_umount(mountpoint: str) -> None:
    '''Invoke fusermount -u -z for mountpoint'''

    umount_cmd: tuple[str, ...]
    if os.getuid() == 0 or platform.system() == 'Darwin':
        # MacOS X always uses umount rather than fusermount
        umount_cmd = ('umount', '-l', mountpoint)
    else:
        umount_cmd = (_fusermount(), '-u', '-z', mountpoint)

    if subprocess.call(umount_cmd) != 0:
        raise UmountSubError(mountpoint)


def get_cmdline(pid: int) -> str | None:
    '''Return command line for *pid*

    If *pid* doesn't exists, return None. If command line
    cannot be determined for other reasons, log warning
    and return None.
    '''

    if os.path.isdir('/proc'):
        try:
            with open('/proc/%d/cmdline' % pid, 'r') as cmd_file:
                return cmd_file.read()

        except FileNotFoundError:
            return None

    else:
        try:
            output = subprocess.check_output(
                ['ps', '-p', str(pid), '-o', 'args='], universal_newlines=True
            ).strip()
            if output:
                return output

        except subprocess.CalledProcessError:
            log.warning('Error when executing ps, assuming process %d has terminated.' % pid)

    return None


def blocking_umount(mountpoint: str) -> None:
    '''Invoke fusermount and wait for daemon to terminate.'''

    with open('/dev/null', 'wb') as devnull:
        if subprocess.call(['fuser', '-m', mountpoint], stdout=devnull, stderr=devnull) == 0:
            raise MountInUseError(mountpoint)

    ctrlfile = os.path.join(mountpoint, CTRL_NAME.decode('utf-8'))

    log.debug('Flushing cache...')
    pyfuse3.setxattr(ctrlfile, 's3ql_flushcache!', b'dummy')

    # Get pid
    log.debug('Trying to get pid')
    pid = parse_literal(pyfuse3.getxattr(ctrlfile, 's3ql_pid?'), int)
    log.debug('PID is %d', pid)

    # Get command line to make race conditions less-likely
    cmdline = get_cmdline(pid)

    # Unmount
    log.debug('Unmounting...')

    if os.getuid() == 0 or platform.system() == 'Darwin':
        # MacOS X always uses umount rather than fusermount
        umount_cmd = ['umount', mountpoint]
    else:
        umount_cmd = [_fusermount(), '-u', mountpoint]

    if subprocess.call(umount_cmd) != 0:
        raise UmountSubError(mountpoint)

    # Wait for daemon
    log.debug('Uploading metadata...')
    step = 0.1
    while True:
        try:
            os.kill(pid, 0)
        except OSError:
            log.debug('Kill failed, assuming daemon has quit.')
            break

        # Check that the process did not terminate and the PID
        # was reused by a different process
        cmdline2 = get_cmdline(pid)
        if cmdline2 is None:
            log.debug('Reading cmdline failed, assuming daemon has quit.')
            break
        elif cmdline2 == cmdline:
            log.debug('PID still alive and commandline unchanged.')
        else:
            log.debug('PID still alive, but cmdline changed')
            break

        # Process still exists, we wait
        log.debug('Daemon seems to be alive, waiting...')
        time.sleep(step)
        if step < 1:
            step += 0.1


@app.command()
@trio_command
async def umount(
    mountpoint: Annotated[
        str, typer.Argument(metavar='<mountpoint>', help='Mount point to un-mount')
    ],
    lazy: Annotated[
        bool,
        typer.Option(
            '--lazy',
            '-z',
            help='Lazy umount. Detaches the file system immediately, even if there are still '
            'open files. The data will be uploaded in the background once all open files have '
            'been closed.',
        ),
    ] = False,
    log: LogDest = None,
    debug: DebugFlag = False,
    debug_modules: DebugModules = None,
    quiet: QuietFlag = False,
) -> None:
    '''Unmount an S3QL file system.

    The command returns only after all data has been uploaded to the backend.
    '''
    setup_logging(quiet=quiet, log=log, debug=debug, debug_modules=debug_modules)

    mountpoint = mountpoint.rstrip('/')
    assert_s3ql_mountpoint(mountpoint)

    try:
        if lazy:
            lazy_umount(mountpoint)
        else:
            blocking_umount(mountpoint)

    except MountInUseError as err:
        print(
            'Cannot unmount, the following processes still access the mountpoint:', file=sys.stderr
        )
        subprocess.call(['fuser', '-v', '-m', mountpoint], stdout=sys.stderr, stderr=sys.stderr)
        sys.exit(err.exitcode)

    except UmountError as err:
        print('%s: %s' % (mountpoint, err), file=sys.stderr)
        sys.exit(err.exitcode)


def main(args: Sequence[str] | None = None) -> None:
    '''Umount S3QL file system'''

    run_app(app, args, prog_name='umount.s3ql')


if __name__ == '__main__':
    main(sys.argv[1:])
