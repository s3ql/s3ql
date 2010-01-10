'''
$Id$

Copyright (C) 2008-2009 Nikolaus Rath <Nikolaus@rath.org>

This program can be distributed under the terms of the GNU LGPL.
'''

from __future__ import division, print_function

import sys
from optparse import OptionParser
from time import sleep
from s3ql.common import init_logging
from s3ql.database import ConnectionManager
from s3ql import s3, mkfs, fsck
from s3ql.cli.mount import run_server, add_common_mount_opts 
import os
import tempfile
import logging 

log = logging.getLogger("frontend")

def main():
    '''Run main program.
    
    This function writes to stdout/stderr and calls `system.exit()` instead
    of returning.
    '''
    
    # Parse options
    options = parse_args()  
    
    # Activate logging
    if options.debug is not None and options.debuglog is None and not options.fg:
        sys.stderr.write('Warning! Debugging output will be lost. '
                         'You should use either --fg or --debuglog.\n')
        
    # Foreground logging until we daemonize
    init_logging(True, options.quiet, options.debug, options.debuglog)
    
    # Check mountpoint
    if not os.path.exists(options.mountpoint):
        log.error('Mountpoint does not exist.\n')
        return 1
    
    # Initialize local bucket and database
    bucket =  s3.LocalConnection().create_bucket('foobar', 'brazl')
    dbfile = tempfile.NamedTemporaryFile()
   
    dbcm = ConnectionManager(dbfile.name, initsql='PRAGMA temp_store = 2; PRAGMA synchronous = off')
    with dbcm() as conn:
        mkfs.setup_db(conn, options.blocksize * 1024)
    log.debug("Temporary database in " + dbfile.name)
    
    # Create cache directory
    cachedir = tempfile.mkdtemp() + b'/'
    try:
        
        # Run server
        options.s3timeout = s3.LOCAL_PROP_DELAY*1.1
        operations = run_server(bucket, cachedir, dbcm, options)
        if operations.encountered_errors:
            log.warn('Some errors occured while handling requests. '
                     'Please examine the logs for more information.')
            
        # We have to make sure that all changes have been committed by the
        # background threads
        sleep(s3.LOCAL_PROP_DELAY*1.1)
            
        # Do fsck
        if options.fsck:
            with dbcm.transaction() as conn:
                fsck.fsck(conn, cachedir, bucket)
            if fsck.found_errors:
                log.warn("fsck found errors")
            
    finally:
        os.rmdir(cachedir)
 
    if operations.encountered_errors or fsck.found_errors:
        sys.exit(1)
    else:
        sys.exit(0)
        

def parse_args():
    '''Parse command line
    
    This function writes to stdout/stderr and may call `system.exit()` instead 
    of throwing an exception if it encounters errors.
    '''
 
    parser = OptionParser(
        usage="%prog  [options] <mountpoint>\n"
              "       %prog --help",
        description="Emulates S3QL filesystem using in-memory storage"
        "instead of actually connecting to S3. Only for testing purposes.")
    
    add_common_mount_opts(parser)
    
    parser.add_option("--blocksize", type="int", default=1,
                      help="Maximum size of s3 objects in KB (default: %default)")
    parser.add_option("--fsck", action="store_true", default=False,
                      help="Runs fsck after the filesystem is unmounted.")
    parser.add_option("--cachesize", type="int", default=10,
                      help="Cache size in kb (default: %default). Should be at least 10 times "
                      "the blocksize of the filesystem, otherwise an object may be retrieved and "
                      "written several times during a single write() or read() operation." )    
    
    (options, pps) = parser.parse_args()
    
    #
    # Verify parameters
    #
    if not len(pps) == 1:
        parser.error("Wrong number of parameters")
    options.mountpoint = pps[0]
    
    return options

if __name__ == '__main__':
    main()    
