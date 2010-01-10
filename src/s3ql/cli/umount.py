'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import ctypes
import s3ql.libc_api as libc
import sys
from optparse import OptionParser 
import os
import logging
from s3ql.common import init_logging, CTRL_NAME 
import posixpath
import subprocess
import time

log = logging.getLogger("frontend")

def parse_args():
    '''Parse command line
     
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''
        
    parser = OptionParser(
        usage="%prog  [options] <mountpoint>\n"
              "       %prog --help",
        description="Unmounts an S3QL file system. The command returns only after "
        'all data has been uploaded to S3. If any file system errors occurred while '
        'the file system was mounted, a warning message is printed. Note that errors '
        'occuring during the unmount (e.g. a failure to upload the metadata) can not '
        'be detected and appear only in the logging messages of the mount program.')
    
    parser.add_option("--debuglog", type="string",
                      help="Write debugging information in specified file. You will need to "
                            'use --debug as well in order to get any output.')
    parser.add_option("--debug", action="append", 
                      help="Activate debugging output from specified facility. Valid facility names "
                            "are: fs, fuse, s3, frontend. "
                            "This option can be specified multiple times.")
    parser.add_option("--quiet", action="store_true", default=False,
                      help="Be really quiet")
    parser.add_option('--lazy', "-z", action="store_true", default=False,
                      help="Lazy umount. Detaches the filesystem immediately, even if there "
                      'are still open files. The file system is uploaded in the background '
                      'once all open files have been closed.')
                          
    (options, pps) = parser.parse_args()
    
    # Verify parameters
    if not len(pps) == 1:
        parser.error("Wrong number of parameters")
    options.mountpoint = pps[0].rstrip('/')
    
    return options

def main():
    '''Umount S3QL file system
    
    This function writes to stdout/stderr and calls `system.exit()` instead
    of returning.
    '''
    
    options = parse_args()
    mountpoint = options.mountpoint
     
    # Activate logging
    init_logging(True, options.quiet, options.debug, options.debuglog)
    
    # Check if it's a mount point
    if not posixpath.ismount(mountpoint):
        print('Not a mount point.', file=sys.stderr)
        sys.exit(1)
        
    # Check if it's an S3QL mountpoint
    ctrlfile = os.path.join(mountpoint, CTRL_NAME) 
    if not (CTRL_NAME not in os.listdir(mountpoint)
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
    else:
        sys.exit(0)
        
        
def blocking_umount(mountpoint):
    '''Invoke fusermount and wait for daemon to terminate.
    
    This function writes to stdout/stderr and calls `system.exit()`.
    '''
    
    found_errors = False
    
    ctrlfile = os.path.join(mountpoint, CTRL_NAME) 
    
    log.info('Flushing cache...')
    cmd = b'doit!' 
    if libc.setxattr(ctrlfile, b's3ql_flushcache!', cmd, len(cmd), 0) != 0:
        print('Failed to issue cache flush command: %s. Continuing anyway..' %
               os.strerror(ctypes.get_errno()), file=sys.stderr)    
    
    if not warn_if_error(mountpoint):
        found_errors = True
      
    # Get pid
    log.debug('Trying to get pid')
    bufsize = 42
    buf = ctypes.create_string_buffer(bufsize)
    ret = libc.getxattr(ctrlfile, b's3ql_pid?', buf, bufsize)
    if ret < 0:
        log.error('Failed to read S3QL daemon pid: %s. '
                  'Unable to determine upload status, upload might still be in progress '
                  'after umount command return.', 
                  os.strerror(ctypes.get_errno()))
        if subprocess.call(['fusermount', '-u', mountpoint]) != 0:
            found_errors = True
        if found_errors:
            sys.exit(1)
        else:
            sys.exit(0)
        
    pid = int(ctypes.string_at(buf, ret))
    
    # Get command line to make race conditions less-likely
    with open('/proc/%d/cmdline' % pid, 'r') as fh:
        cmdline = fh.readline()
    
    # Unmount
    log.info('Unmounting...')
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
                    # PID must have been reused, original process terminated
                    break
        except OSError:
            # Process must have exited by now
            log.debug('Reading cmdline failed, assuming daemon has quit.')
            break
    
        # Process still exists, we wait
        log.debug('Daemon seems to be alive, waiting...')
        time.sleep(step)
        if step < 10:
            step *= 2    
        
    if found_errors:
        sys.exit(1)
    else:
        sys.exit(0)
    
def warn_if_error(mountpoint):
    '''Check if file system encountered any errors
    
    If there were errors, a warning is printed to stdout and the
    function returns False.
    '''
    
    log.debug('Trying to get error status')
    bufsize = 42
    ctrlfile = os.path.join(mountpoint, CTRL_NAME) 
    buf = ctypes.create_string_buffer(bufsize)
    ret = libc.getxattr(ctrlfile, 's3ql_errors?', buf, bufsize)
    if ret < 0:
        print('Failed to read current file system status: %s. Continuing anyway..' %
                  os.strerror(ctypes.get_errno()), file=sys.stderr)
        
    status = ctypes.string_at(ctypes.addressof(buf), ret)
    if status != 'no errors':
        print('Some errors occurred while the file system was mounted.\n'
              'You should examine the log files and run fsck before mounting the\n'
              'file system again.', file=sys.stderr) 
        return False
    else:
        return True

if __name__ == '__main__':
    main()    
