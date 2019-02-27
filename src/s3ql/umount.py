'''
umount.py - this file is part of S3QL.

Copyright © 2008 Nikolaus Rath <Nikolaus@rath.org>

This work can be distributed under the terms of the GNU GPLv3.
'''

from .logging import logging, setup_logging
from . import CTRL_NAME
from .common import assert_s3ql_mountpoint, parse_literal
from .parse_args import ArgumentParser
import llfuse
import os
import subprocess
import platform
import sys
import textwrap
import time

log = logging.getLogger(__name__)

def parse_args(args):
    '''Parse command line

    This function writes to stdout/stderr and may call `system.exit()` instead
    of throwing an exception if it encounters errors.
    '''

    parser = ArgumentParser(
        description=textwrap.dedent('''\
        Unmounts an S3QL file system. The command returns only after all data
        has been uploaded to the backend.'''))

    parser.add_log()
    parser.add_debug()
    parser.add_quiet()
    parser.add_version()

    parser.add_argument("mountpoint", metavar='<mountpoint>',
                        type=(lambda x: x.rstrip('/')),
                        help='Mount point to un-mount')

    parser.add_argument('--lazy', "-z", action="store_true", default=False,
                      help="Lazy umount. Detaches the file system immediately, even if there "
                      'are still open files. The data will be uploaded in the background '
                      'once all open files have been closed.')

    return parser.parse_args(args)

class UmountError(Exception):
    """
    Base class for unmount errors.
    """

    message = 'internal error'
    exitcode = 3

    def __init__(self, mountpoint):
        super().__init__()
        self.mountpoint = mountpoint

    def __str__(self):
        return self.message

class UmountSubError(UmountError):
    message = 'Unmount subprocess failed.'
    exitcode = 2

class MountInUseError(UmountError):
    message = 'In use.'
    exitcode = 1

def lazy_umount(mountpoint):
    '''Invoke fusermount -u -z for mountpoint'''

    if os.getuid() == 0 or platform.system() == 'Darwin':
        # MacOS X always uses umount rather than fusermount
        umount_cmd = ('umount', '-l', mountpoint)
    else:
        umount_cmd = ('fusermount', '-u', '-z', mountpoint)

    if subprocess.call(umount_cmd) != 0:
        raise UmountSubError(mountpoint)

def get_cmdline(pid):
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
            output = subprocess.check_output(['ps', '-p', str(pid), '-o', 'args='],
                                             universal_newlines=True).strip()
            if output:
                return output

        except subprocess.CalledProcessError:
            log.warning('Error when executing ps, assuming process %d has terminated.'
                        % pid)

    return None

def blocking_umount(mountpoint):
    '''Invoke fusermount and wait for daemon to terminate.'''

    with open('/dev/null', 'wb') as devnull:
        if subprocess.call(['fuser', '-m', mountpoint], stdout=devnull,
                           stderr=devnull) == 0:
            raise MountInUseError(mountpoint)

    ctrlfile = os.path.join(mountpoint, CTRL_NAME)

    log.debug('Flushing cache...')
    llfuse.setxattr(ctrlfile, 's3ql_flushcache!', b'dummy')

    # Get pid
    log.debug('Trying to get pid')
    pid = parse_literal(llfuse.getxattr(ctrlfile, 's3ql_pid?'), int)
    log.debug('PID is %d', pid)

    # Get command line to make race conditions less-likely
    cmdline = get_cmdline(pid)

    # Unmount
    log.debug('Unmounting...')

    if os.getuid() == 0 or platform.system() == 'Darwin':
        # MacOS X always uses umount rather than fusermount
        umount_cmd = ['umount', mountpoint]
    else:
        umount_cmd = ['fusermount', '-u', mountpoint]

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

def main(args=None):
    '''Umount S3QL file system'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    assert_s3ql_mountpoint(options.mountpoint)

    try:
        if options.lazy:
            lazy_umount(options.mountpoint)
        else:
            blocking_umount(options.mountpoint)

    except MountInUseError as err:
        print('Cannot unmount, the following processes still access the mountpoint:',
              file=sys.stderr)
        subprocess.call(['fuser', '-v', '-m', options.mountpoint],
                        stdout=sys.stderr, stderr=sys.stderr)
        sys.exit(err.exitcode)

    except UmountError as err:
        print('%s: %s' % (options.mountpoint, err), file=sys.stderr)
        sys.exit(err.exitcode)

    sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
