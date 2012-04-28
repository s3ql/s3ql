'''
umount.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU GPLv3.
'''

from __future__ import division, print_function, absolute_import
from .common import CTRL_NAME, setup_logging
from .parse_args import ArgumentParser
import llfuse
import logging
import os
import posixpath
import subprocess
import sys
import textwrap
import time

log = logging.getLogger("umount")

def parse_args(args):
    '''Parse command line
     
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''

    parser = ArgumentParser(
        description=textwrap.dedent('''\ 
        Unmounts an S3QL file system. The command returns only after all data
        has been uploaded to the backend.'''))

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

class MountError(Exception):
    """
    Base class for mountpoint errors.
    """

    message = ''
    
    def __init__(self, mountpoint):
        super(MountError, self).__init__()
        self.mountpoint = mountpoint

    def __str__(self):
        return self.message.format(self.mountpoint)


class NotMountPointError(MountError):

    message = '"{}" is not a mountpoint.'


class NotS3qlFsError(MountError):

    message = '"{}" is not an S3QL file system.'


class UmountError(MountError):

    message = 'Error while unmounting "{}".'


class MountInUseError(MountError):

    message = '"{}" is being used.'


def check_mount(mountpoint):
    '''Check that "mountpoint" is a mountpoint and a valid s3ql fs'''
    
    if not posixpath.ismount(mountpoint):
        raise NotMountPointError(mountpoint)

    ctrlfile = os.path.join(mountpoint, CTRL_NAME)
    if not (
        CTRL_NAME not in llfuse.listdir(mountpoint) and
        os.path.exists(ctrlfile)
    ):
        raise NotS3qlFsError(mountpoint)

def lazy_umount(mountpoint):
    '''Invoke fusermount -u -z for mountpoint'''

    if os.getuid() == 0:
        umount_cmd = ('umount', '-l', mountpoint)
    else:
        umount_cmd = ('fusermount', '-u', '-z', mountpoint)

    if subprocess.call(umount_cmd)!=0:
        raise UmountError(mountpoint)

def blocking_umount(mountpoint):
    '''Invoke fusermount and wait for daemon to terminate.'''

    devnull = open('/dev/null', 'wb')
    if subprocess.call(
        ['fuser', '-m', mountpoint], stdout=devnull, stderr=devnull
    ) == 0:
        raise MountInUseError(mountpoint)

    ctrlfile = os.path.join(mountpoint, CTRL_NAME)

    log.debug('Flushing cache...')
    llfuse.setxattr(ctrlfile, b's3ql_flushcache!', b'dummy')

    # Get pid
    log.debug('Trying to get pid')
    pid = int(llfuse.getxattr(ctrlfile, b's3ql_pid?'))
    log.debug('PID is %d', pid)

    # Get command line to make race conditions less-likely
    with open('/proc/%d/cmdline' % pid, 'r') as fh:
        cmdline = fh.readline()
    log.debug('cmdline is %r', cmdline)

    # Unmount
    log.debug('Unmounting...')
    # This seems to be necessary to prevent weird busy errors
    time.sleep(3)

    if os.getuid() == 0:
        umount_cmd = ['umount', mountpoint]
    else:
        umount_cmd = ['fusermount', '-u', mountpoint]

    if subprocess.call(umount_cmd) != 0:
        raise UmountError(mountpoint)

    # Wait for daemon
    log.debug('Uploading metadata...')
    step = 0.5
    while True:
        try:
            os.kill(pid, 0)
        except OSError:
            log.debug('Kill failed, assuming daemon has quit.')
            break

        # Check that the process did not terminate and the PID
        # was reused by a different process
        try:
            with open('/proc/%d/cmdline' % pid, 'r') as fh:
                if fh.readline() != cmdline:
                    log.debug('PID still alive, but cmdline changed')
                    # PID must have been reused, original process terminated
                    break
                else:
                    log.debug('PID still alive and commandline unchanged.')
        except OSError:
            # Process must have exited by now
            log.debug('Reading cmdline failed, assuming daemon has quit.')
            break

        # Process still exists, we wait
        log.debug('Daemon seems to be alive, waiting...')
        time.sleep(step)
        if step < 10:
            step *= 2

def umount(mountpoint, lazy=False):
    '''Umount "mountpoint", blocks if not "lazy".'''

    check_mount(mountpoint)
    if lazy:
        lazy_umount(mountpoint)
    else:
        blocking_umount(mountpoint)

def main(args=None):
    '''Umount S3QL file system'''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    setup_logging(options)

    try:
        umount(options.mountpoint, options.lazy)
    except NotMountPointError as err:
        print(err, file=sys.stderr)
        sys.exit(1)
    except NotS3qlFsError as err:
        print(err, file=sys.stderr)
        sys.exit(2)
    except UmountError as err:
        print(err, file=sys.stderr)
        sys.exit(3)
    except MountInUseError:
        print('Cannot unmount, the following processes still access the mountpoint:',
              file=sys.stderr)
        subprocess.call(['fuser', '-v', '-m', options.mountpoint],
                        stdout=sys.stderr, stderr=sys.stderr)
        sys.exit(4)
    else:
        sys.exit(0)

if __name__ == '__main__':
    main(sys.argv[1:])
