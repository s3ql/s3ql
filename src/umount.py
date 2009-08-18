#!/usr/bin/env python
#
#    Copyright (C) 2008  Nikolaus Rath <Nikolaus@rath.org>
#
#    This program can be distributed under the terms of the GNU LGPL.
#

from __future__ import unicode_literals
from __future__ import division
import ctypes
from ctypes import c_char_p, c_size_t, c_int, c_void_p
import sys
from optparse import OptionParser
import os
import logging
from s3ql.common import init_logging, CTRL_NAME 
import posixpath
import subprocess
import time


#
# Parse command line
#
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
                        "are: fs, fs.fuse, s3, frontend. "
                        "This option can be specified multiple times.")
parser.add_option("--quiet", action="store_true", default=False,
                  help="Be really quiet")
parser.add_option('--lazy', "-l", action="store_true", default=False,
                  help="Lazy umount. Detaches the filesystem immediately, even if there "
                  'are still open files. The file system is uploaded in the background '
                  'once all open files have been closed.')
                      

(options, pps) = parser.parse_args()

#
# Verify parameters
#
if not len(pps) == 1:
    parser.error("Wrong number of parameters")
mountpoint = pps[0].rstrip('/')
      
# Activate logging
init_logging(True, options.quiet, options.debug, options.debuglog)
log = logging.getLogger("frontend")

# Check if it's a mount point
if not posixpath.ismount(mountpoint):
    log.error('Not a mount point.')
    sys.exit(1)
    
    
# Check if it's an S3QL mountpoint
ctrlfile = '%s/%s' % (mountpoint, CTRL_NAME) 
if not (CTRL_NAME not in os.listdir(mountpoint)
        and os.path.exists(ctrlfile)):
    log.error('Not an S3QL file system.')
    sys.exit(1)
    

# Import extended attributes
libc = ctypes.CDLL('libc.so.6', use_errno=True)
libc.setxattr.argtypes = [ c_char_p, c_char_p, c_char_p, c_size_t, c_int ]
libc.setxattr.restype = c_int
libc.getxattr.argtypes = [ c_char_p, c_char_p, c_void_p, c_size_t ]
libc.getxattr.restype = c_int # FIXME: This is actually ssize_t, but ctypes doesn't know about it

# Write cache flush command
if not options.lazy:
    cmd = 'doit!' 
    log.info('Flushing cache...')
    if libc.setxattr(ctrlfile, 's3ql_flushcache!', cmd, len(cmd), 0) != 0:
        log.error('Failed to issue cache flush command: %s. Continuing anyway..', 
                  os.strerror(ctypes.get_errno()))

    
# Check for errors
log.debug('Trying to get error status')
bufsize = 42
buf = ctypes.create_string_buffer(bufsize)
ret = libc.getxattr(ctrlfile, 's3ql_errors?', buf, bufsize)
if ret < 0:
    log.error('Failed to read current file system status: %s. Continuing anyway..', 
              os.strerror(ctypes.get_errno()))
status = ctypes.string_at(ctypes.addressof(buf), ret)
if status != 'no errors':
    log.error('Some errors occurred while the file system was mounted ("%s"). '
              'You should examine the log files and run fsck before mounting the '
              'file system again.', status)

# Construct umount command
if options.lazy:
    umount_cmd = ('fusermount', '-u', '-z', mountpoint)
else:
    umount_cmd = ('fusermount', '-u', mountpoint)


# Get pid
log.debug('Trying to get pid')
bufsize = 42
buf = ctypes.create_string_buffer(bufsize)
ret = libc.getxattr(ctrlfile, 's3ql_pid?', buf, bufsize)
if ret < 0:
    log.error('Failed to read S3QL daemon pid: %s. '
              'Unable to determine upload status, upload might still be in progress '
              'after umount command return.', 
              os.strerror(ctypes.get_errno()))
    sys.exit(subprocess.call(umount_cmd))
pid = int(ctypes.string_at(ctypes.addressof(buf), ret))


# Get command line to make race conditions less-likely
with open('/proc/%d/cmdline' % pid, 'r') as fh:
    cmdline = fh.readline()

# Unmount
log.debug('Calling fusermount')
if subprocess.call(umount_cmd) != 0:
    sys.exit(1)


# Wait for daemon
log.debug('Waiting for daemon to disappear')
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
    

sys.exit(0)