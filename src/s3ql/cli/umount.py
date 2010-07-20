'''
umount.py - this file is part of S3QL (http://s3ql.googlecode.com)

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function, absolute_import

from s3ql import libc
import sys
import os
import logging
from s3ql.common import (CTRL_NAME, QuietError, add_stdout_logging, 
                         setup_excepthook, OptionParser)
import posixpath
import subprocess
import time

log = logging.getLogger("umount")
DONTWAIT = False

def parse_args(args):
    '''Parse command line
     
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''

    parser = OptionParser(
        usage="%prog [options] <mountpoint>\n"
              "%prog --help",
        description="Unmounts an S3QL file system. The command returns only after "
        'all data has been uploaded to the backend. If any file system errors occurred while '
        'the file system was mounted, a warning message is printed. Note that errors '
        'occuring during the unmount (e.g. a failure to upload the metadata) can not '
        'be detected and appear only in the logging messages of the mount program.')

    parser.add_option("--debug", action="store_true",
                      help="Activate debugging output")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option('--lazy', "-z", action="store_true", default=False,
                      help="Lazy umount. Detaches the file system immediately, even if there "
                      'are still open files. The data will be uploaded in the background '
                      'once all open files have been closed.')

    (options, pps) = parser.parse_args(args)

    # Verify parameters
    if len(pps) != 1:
        parser.error("Incorrect number of arguments.")
    options.mountpoint = pps[0].rstrip('/')

    return options

def main(args=None):
    '''Umount S3QL file system
    
    This function writes to stdout/stderr and calls `system.exit()` instead
    of returning.
    '''

    if args is None:
        args = sys.argv[1:]

    options = parse_args(args)
    mountpoint = options.mountpoint
    
    # Initialize logging if not yet initialized
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        handler = add_stdout_logging(options.quiet)
        setup_excepthook()  
        if options.debug:
            root_logger.setLevel(logging.DEBUG)
            handler.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO) 
    else:
        log.info("Logging already initialized.")
        
    # Check if it's a mount point
    if not posixpath.ismount(mountpoint):
        print('Not a mount point.', file=sys.stderr)
        sys.exit(1)

    # Check if it's an S3QL mountpoint
    ctrlfile = os.path.join(mountpoint, CTRL_NAME)
    if not (CTRL_NAME not in libc.listdir(mountpoint)
            and os.path.exists(ctrlfile)):
        print('Not an S3QL file system.', file=sys.stderr)
        sys.exit(1)

    if options.lazy:
        lazy_umount(mountpoint)
    else:
        blocking_umount(mountpoint)


def lazy_umount(mountpoint):
    '''Invoke fusermount -u -z for mountpoint
    
    This function writes to stdout/stderr and calls `system.exit()`.
    '''

    found_errors = False
    if not warn_if_error(mountpoint):
        found_errors = True
    umount_cmd = ('fusermount', '-u', '-z', mountpoint)
    if not subprocess.call(umount_cmd) == 0:
        found_errors = True

    if found_errors:
        sys.exit(1)


def blocking_umount(mountpoint):
    '''Invoke fusermount and wait for daemon to terminate.
    
    This function writes to stdout/stderr and calls `system.exit()`.
    '''

    found_errors = False

    devnull = open('/dev/null', 'wb')
    if subprocess.call(['fuser', '-m', mountpoint], stdout=devnull,
                       stderr=devnull) == 0:
        print('Cannot unmount, the following processes still access the mountpoint:')
        subprocess.call(['fuser', '-v', '-m', mountpoint], stdout=sys.stdout,
                        stderr=sys.stdout)
        raise QuietError(1)

    ctrlfile = os.path.join(mountpoint, CTRL_NAME)
    
    log.info('Flushing cache...')
    libc.setxattr(ctrlfile, b's3ql_flushcache!', b'dummy')

    if not warn_if_error(mountpoint):
        found_errors = True

    # Get pid
    log.debug('Trying to get pid')
    pid = int(libc.getxattr(ctrlfile, b's3ql_pid?'))
    log.debug('PID is %d', pid)

    # Get command line to make race conditions less-likely
    with open('/proc/%d/cmdline' % pid, 'r') as fh:
        cmdline = fh.readline()
    log.debug('cmdline is %r', cmdline)

    # Unmount
    log.info('Unmounting...')
    # This seems to be necessary to prevent weird busy errors
    time.sleep(3)
    if subprocess.call(['fusermount', '-u', mountpoint]) != 0:
        sys.exit(1)

    # Wait for daemon
    log.info('Uploading metadata...')
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

        if DONTWAIT: # for testing 
            break

        # Process still exists, we wait
        log.debug('Daemon seems to be alive, waiting...')
        time.sleep(step)
        if step < 10:
            step *= 2

    if found_errors:
        sys.exit(1)


def warn_if_error(mountpoint):
    '''Check if file system encountered any errors
    
    If there were errors, a warning is printed to stdout and the
    function returns False.
    '''

    log.debug('Trying to get error status')
    ctrlfile = os.path.join(mountpoint, CTRL_NAME)
    status = libc.getxattr(ctrlfile, 's3ql_errors?')

    if status != 'no errors':
        print('Some errors occurred while the file system was mounted.\n'
              'You should examine the log files and run fsck before mounting the\n'
              'file system again.', file=sys.stderr)
        return False
    else:
        return True

if __name__ == '__main__':
    main(sys.argv[1:])
